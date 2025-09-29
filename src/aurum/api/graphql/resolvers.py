from __future__ import annotations

"""GraphQL resolver utilities for the Aurum API layer.

This module centralises common resolver helpers so that the schema module can
focus on type definitions. The helpers here cover:

* ISO energy market batching to avoid N+1 query patterns
* Request complexity analysis and lightweight in-memory rate limiting
* Risk compliance service bridging for schedules and report manifests
* GraphQL federation gateway helpers for stitched microservice schemas
* Client SDK manifest building to expose generated operation metadata
"""

import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from threading import Lock
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple

try:  # Strawberry ships graphql-core so this should normally exist
    from graphql import GraphQLError  # type: ignore
except Exception:  # pragma: no cover - fallback in environments without graphql-core
    class GraphQLError(RuntimeError):
        """Fallback error when GraphQL core is unavailable."""


from strawberry.types import Info

from ..services.iso_service import IsoService
from ..services.risk_compliance_service import (
    ComplianceReportConfig,
    get_risk_compliance_service,
)
from ..services.governance_service import (
    GovernanceConfig,
    get_governance_service,
    initialise_governance_service,
)
from ...governance.catalog import CatalogService
from ...governance.classification import ColumnClassifier
from ...governance.impact_analysis import ImpactAnalyzer
from ...governance.lineage_tracker import LineageTracker
from ...governance.monitors import FreshnessCompletenessMonitor
from ...governance.privacy import PrivacyPolicyManager, PrivacyRule
from ...governance.quality_engine import DataQualityEngine
from ...governance.schema_tracker import SchemaEvolutionTracker
from ...telemetry.context import log_structured

try:  # httpx is optional in some minimal environments
    import httpx
except Exception:  # pragma: no cover - keep type checking happy
    httpx = None  # type: ignore

import pandas as pd


LOGGER = logging.getLogger(__name__)
_GOVERNANCE_INIT_LOCK = Lock()

# ---------------------------------------------------------------------------
# Environment tuned defaults for complexity and rate limiting
# ---------------------------------------------------------------------------
_COMPLEXITY_LIMIT = int(os.getenv("AURUM_GRAPHQL_COMPLEXITY_LIMIT", "800"))
_DEPTH_LIMIT = int(os.getenv("AURUM_GRAPHQL_DEPTH_LIMIT", "8"))
_RATE_BUDGET = int(os.getenv("AURUM_GRAPHQL_RATE_BUDGET", "5000"))
_RATE_WINDOW_SECONDS = float(os.getenv("AURUM_GRAPHQL_RATE_WINDOW", "60"))
_GATEWAY_TIMEOUT_SECONDS = float(os.getenv("AURUM_GRAPHQL_GATEWAY_TIMEOUT", "8"))
_GATEWAY_ENDPOINTS = os.getenv("AURUM_GRAPHQL_FEDERATION_ENDPOINTS", "{}")
_GATEWAY_HEADERS = os.getenv("AURUM_GRAPHQL_FEDERATION_HEADERS", "{}")


# ---------------------------------------------------------------------------
# Data loader keys and caching helpers
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EnergyMarketKey:
    """Unique cache key for ISO market lookups."""

    iso_code: Optional[str]
    market: Optional[str]
    location_id: Optional[str]
    granularity: str
    limit: int
    start: Optional[str] = None
    end: Optional[str] = None


class IsoLmpDataLoader:
    """Batching loader for ISO LMP queries to prevent N+1 fetches."""

    def __init__(self) -> None:
        self._service = IsoService()
        self._cache: Dict[EnergyMarketKey, Tuple[List[Dict[str, Any]], float]] = {}
        self._locks: Dict[EnergyMarketKey, asyncio.Lock] = {}
        self._locks_guard = asyncio.Lock()

    async def load(self, key: EnergyMarketKey) -> Tuple[List[Dict[str, Any]], float]:
        if key in self._cache:
            return self._cache[key]

        lock = await self._ensure_lock(key)
        async with lock:
            if key in self._cache:
                return self._cache[key]

            payload: Tuple[List[Dict[str, Any]], float]
            if key.granularity == "LAST_24H":
                payload = await self._service.lmp_last_24h(
                    iso_code=key.iso_code,
                    market=key.market,
                    location_id=key.location_id,
                    limit=key.limit,
                )
            elif key.granularity == "HOURLY":
                payload = await self._service.lmp_hourly(
                    iso_code=key.iso_code,
                    market=key.market,
                    location_id=key.location_id,
                    date=key.start,
                    limit=key.limit,
                )
            elif key.granularity == "DAILY":
                payload = await self._service.lmp_daily(
                    iso_code=key.iso_code,
                    market=key.market,
                    location_id=key.location_id,
                    start_date=key.start,
                    end_date=key.end,
                    limit=key.limit,
                )
            elif key.granularity == "NEGATIVE":
                payload = await self._service.lmp_negative(
                    iso_code=key.iso_code,
                    market=key.market,
                    location_id=key.location_id,
                    start_date=key.start,
                    end_date=key.end,
                    limit=key.limit,
                )
            else:
                raise GraphQLError(f"Unsupported granularity '{key.granularity}'")

            self._cache[key] = payload
            return payload

    async def _ensure_lock(self, key: EnergyMarketKey) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock


# ---------------------------------------------------------------------------
# Complexity accounting & rate limiting
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ComplexityMetrics:
    depth: int
    nodes: int


def _collect_complexity(selected_fields: Iterable[Any], depth: int = 1) -> ComplexityMetrics:
    max_depth = depth
    total_nodes = 0
    for field in selected_fields or []:
        total_nodes += 1
        children = getattr(field, "selections", None) or []
        if children:
            child_metrics = _collect_complexity(children, depth + 1)
            total_nodes += child_metrics.nodes
            if child_metrics.depth > max_depth:
                max_depth = child_metrics.depth
    return ComplexityMetrics(depth=max_depth, nodes=total_nodes)


class GraphQLRateLimiter:
    """Simple in-memory token bucket keyed by tenant identifier."""

    def __init__(self, budget: int, window_seconds: float) -> None:
        self._budget = max(1, budget)
        self._window = max(1.0, window_seconds)
        self._records: Dict[str, Deque[Tuple[float, int]]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def consume(self, key: str, cost: int) -> None:
        now = time.time()
        async with self._lock:
            queue = self._records[key]
            cutoff = now - self._window
            while queue and queue[0][0] <= cutoff:
                queue.popleft()
            used = sum(value for _ts, value in queue)
            if used + cost > self._budget:
                raise GraphQLError("GraphQL rate limit exceeded for tenant")
            queue.append((now, cost))


_RATE_LIMITER = GraphQLRateLimiter(_RATE_BUDGET, _RATE_WINDOW_SECONDS)


def _default_governance_loader(dataset_fqn: str) -> pd.DataFrame:
    LOGGER.warning("Governance data loader not configured; returning empty frame for %%s", dataset_fqn)
    return pd.DataFrame()


def _initialise_governance_service() -> "GovernanceService":
    with _GOVERNANCE_INIT_LOCK:
        try:
            return get_governance_service()
        except RuntimeError:
            openlineage_url = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
            namespace = os.getenv("OPENLINEAGE_NAMESPACE", "aurum")
            marquez_url = os.getenv("MARQUEZ_URL") or os.getenv("OPENLINEAGE_MARQUEZ_URL") or openlineage_url
            openmetadata_server = os.getenv("OPENMETADATA_SERVER", "http://openmetadata-server:8585/api")
            openmetadata_token = os.getenv("OPENMETADATA_TOKEN")

            lineage_tracker = LineageTracker(
                openlineage_url=openlineage_url,
                namespace=namespace,
                marquez_url=marquez_url,
            )
            catalog_service = CatalogService(server_url=openmetadata_server, auth_token=openmetadata_token)
            quality_engine = DataQualityEngine()
            monitor = FreshnessCompletenessMonitor(quality_engine, _default_governance_loader)
            schema_tracker = SchemaEvolutionTracker(catalog_service)
            impact_analyzer = ImpactAnalyzer(catalog_service, lineage_tracker)
            classifier = ColumnClassifier(catalog=catalog_service)
            privacy_manager = PrivacyPolicyManager(
                catalog_service,
                [
                    PrivacyRule(tag="pii.ssn", action="restrict"),
                    PrivacyRule(tag="pii.email", action="mask"),
                    PrivacyRule(tag="pii.phone", action="mask"),
                ],
            )

            return initialise_governance_service(
                lineage_tracker=lineage_tracker,
                catalog_service=catalog_service,
                quality_engine=quality_engine,
                schema_tracker=schema_tracker,
                impact_analyzer=impact_analyzer,
                monitor=monitor,
                classifier=classifier,
                privacy_manager=privacy_manager,
                dataframe_loader=_default_governance_loader,
                config=GovernanceConfig(),
            )


def get_governance() -> "GovernanceService":
    try:
        return get_governance_service()
    except RuntimeError:
        return _initialise_governance_service()


async def enforce_complexity_limits(info: Info, *, base_cost: int = 1) -> ComplexityMetrics:
    """Ensure queries stay within permitted complexity and rate limits."""

    selected_fields = getattr(info, "selected_fields", None) or []
    metrics = _collect_complexity(selected_fields, depth=1)
    effective_nodes = metrics.nodes + max(0, base_cost)

    depth_limit = int(info.context.get("graphql_depth_limit", _DEPTH_LIMIT))
    node_limit = int(info.context.get("graphql_node_limit", _COMPLEXITY_LIMIT))

    if metrics.depth > depth_limit:
        raise GraphQLError(
            f"Query depth {metrics.depth} exceeds allowed limit of {depth_limit}"
        )
    if effective_nodes > node_limit:
        raise GraphQLError(
            f"Query complexity {effective_nodes} exceeds allowed limit of {node_limit}"
        )

    tenant = info.context.get("tenant_id") or "anonymous"
    await _RATE_LIMITER.consume(str(tenant), effective_nodes)
    return metrics


# ---------------------------------------------------------------------------
# Federation gateway helpers
# ---------------------------------------------------------------------------
def _load_json(value: str) -> Dict[str, Any]:
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_FEDERATED_ENDPOINTS = _load_json(_GATEWAY_ENDPOINTS)
_FEDERATED_HEADERS = _load_json(_GATEWAY_HEADERS)


class GraphQLGateway:
    """Minimal federation gateway capable of querying remote GraphQL services."""

    def __init__(self) -> None:
        self._endpoints: Dict[str, str] = {
            name.lower(): str(url)
            for name, url in _FEDERATED_ENDPOINTS.items()
        }
        self._headers: Dict[str, Dict[str, str]] = {
            name.lower(): {
                str(h_key): str(h_value)
                for h_key, h_value in headers.items()
            }
            for name, headers in _FEDERATED_HEADERS.items()
            if isinstance(headers, dict)
        }
        self._timeout = _GATEWAY_TIMEOUT_SECONDS

    def available_services(self) -> List[str]:
        return sorted(self._endpoints.keys())

    async def execute(self, service: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if httpx is None:
            raise GraphQLError("httpx dependency is required for federation gateway")

        if not service:
            raise GraphQLError("Federated service name is required")

        endpoint = self._endpoints.get(service.lower())
        if not endpoint:
            raise GraphQLError(f"Federated service '{service}' is not configured")

        headers = {
            "content-type": "application/json",
            **self._headers.get(service.lower(), {}),
        }

        payload = {"query": query, "variables": variables or {}}
        timeout = httpx.Timeout(self._timeout)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.post(endpoint, json=payload, headers=headers)
                response.raise_for_status()
            except Exception as exc:
                raise GraphQLError(f"Federated query failed: {exc}") from exc
            try:
                return response.json()
            except ValueError as exc:
                raise GraphQLError("Federated service returned invalid JSON") from exc


def get_gateway(info: Info) -> GraphQLGateway:
    gateway = info.context.get("graphql_gateway")
    if isinstance(gateway, GraphQLGateway):
        return gateway
    gateway = GraphQLGateway()
    info.context["graphql_gateway"] = gateway
    return gateway


# ---------------------------------------------------------------------------
# Energy market resolver helpers
# ---------------------------------------------------------------------------

def get_iso_loader(info: Info) -> IsoLmpDataLoader:
    loader = info.context.get("iso_loader")
    if isinstance(loader, IsoLmpDataLoader):
        return loader
    loader = IsoLmpDataLoader()
    info.context["iso_loader"] = loader
    return loader


async def resolve_energy_market_series(
    info: Info,
    keys: Iterable[EnergyMarketKey],
) -> List[Dict[str, Any]]:
    key_list = list(keys)
    await enforce_complexity_limits(info, base_cost=len(key_list) * 3)
    loader = get_iso_loader(info)

    series: List[Dict[str, Any]] = []
    for key in key_list:
        rows, duration = await loader.load(key)
        payload = {
            "filter": {
                "iso_code": key.iso_code,
                "market": key.market,
                "location_id": key.location_id,
                "granularity": key.granularity,
                "limit": key.limit,
                "start": key.start,
                "end": key.end,
            },
            "query_time_ms": int(duration * 1000),
            "points": [_normalise_energy_row(row) for row in rows],
        }
        series.append(payload)
    return series


def _normalise_energy_row(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "iso_code": row.get("iso_code"),
        "market": row.get("market"),
        "interval_start": row.get("interval_start"),
        "interval_end": row.get("interval_end"),
        "interval_minutes": row.get("interval_minutes"),
        "location_id": row.get("location_id"),
        "location_name": row.get("location_name"),
        "location_type": row.get("location_type"),
        "price_total": _to_float(row.get("price_total")),
        "price_energy": _to_float(row.get("price_energy")),
        "price_congestion": _to_float(row.get("price_congestion")),
        "price_loss": _to_float(row.get("price_loss")),
        "currency": row.get("currency"),
        "uom": row.get("uom"),
        "settlement_point": row.get("settlement_point"),
        "metadata": row.get("metadata") or {},
    }


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(str(value))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Risk compliance helpers
# ---------------------------------------------------------------------------
async def resolve_compliance_schedules(info: Info) -> List[Dict[str, Any]]:
    await enforce_complexity_limits(info, base_cost=4)
    service = get_risk_compliance_service()
    schedules = await service.list_schedules()
    return [_schedule_to_dict(item) for item in schedules]


async def resolve_reports_for_portfolio(info: Info, portfolio_id: str, limit: int) -> List[Dict[str, Any]]:
    await enforce_complexity_limits(info, base_cost=2)
    service = get_risk_compliance_service()
    reports = await service.list_reports(portfolio_id, limit=limit)
    for report in reports:
        report.setdefault("portfolio_id", portfolio_id)
    return reports


async def create_compliance_schedule(
    info: Info,
    *,
    portfolio_id: Optional[str],
    schedule_time_utc: str,
    retention_days: int,
    max_reports: Optional[int],
    report_config: Dict[str, Any],
) -> Dict[str, Any]:
    await enforce_complexity_limits(info, base_cost=6)
    service = get_risk_compliance_service()

    config = _build_report_config(report_config)
    schedule_id = service.create_schedule(
        portfolio=None,
        portfolio_id=portfolio_id,
        report_config=config,
        schedule_time_utc=schedule_time_utc,
        retention_days=retention_days,
        max_reports=max_reports,
    )
    log_structured(
        "graphql_create_compliance_schedule",
        schedule_id=schedule_id,
        portfolio_id=portfolio_id,
    )
    schedules = await service.list_schedules()
    schedule = next((item for item in schedules if item.schedule_id == schedule_id), None)
    return _schedule_to_dict(schedule) if schedule else {"schedule_id": schedule_id}


async def delete_compliance_schedule(info: Info, schedule_id: str) -> bool:
    await enforce_complexity_limits(info, base_cost=2)
    service = get_risk_compliance_service()
    result = service.delete_schedule(schedule_id)
    if result:
        log_structured("graphql_delete_compliance_schedule", schedule_id=schedule_id)
    return result


async def run_compliance_report(info: Info, schedule_id: str) -> Optional[Dict[str, Any]]:
    await enforce_complexity_limits(info, base_cost=5)
    service = get_risk_compliance_service()
    path = await service.run_report_now(schedule_id)
    if not path:
        return None
    return {
        "schedule_id": schedule_id,
        "artifact_path": str(path),
    }


def _build_report_config(payload: Dict[str, Any]) -> ComplianceReportConfig:
    if isinstance(payload, ComplianceReportConfig):
        return payload
    if not isinstance(payload, dict):
        raise GraphQLError("Compliance report configuration must be an object")
    try:
        return ComplianceReportConfig(**payload)
    except Exception as exc:
        raise GraphQLError(f"Invalid compliance report configuration: {exc}") from exc


def _schedule_to_dict(schedule: Any) -> Dict[str, Any]:
    if schedule is None:
        return {}
    return {
        "schedule_id": getattr(schedule, "schedule_id", None),
        "portfolio_id": getattr(schedule, "portfolio_id", None)
        or getattr(getattr(schedule, "portfolio", None), "portfolio_id", None),
        "schedule_time_utc": getattr(schedule, "schedule_time_utc", "00:00"),
        "enabled": getattr(schedule, "enabled", True),
        "retention_days": getattr(schedule, "retention_days", 30),
        "max_reports": getattr(schedule, "max_reports", None),
        "last_run": getattr(schedule, "last_run", None),
        "next_run": getattr(schedule, "next_run", None),
        "report_config": _maybe_model_dump(getattr(schedule, "report_config", None)),
    }


def _maybe_model_dump(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        return value.model_dump()  # type: ignore[attr-defined]
    if hasattr(value, "dict"):
        return value.dict()  # type: ignore[attr-defined]
    if isinstance(value, dict):
        return value
    return value


# ---------------------------------------------------------------------------
# Documentation and SDK manifest helpers
# ---------------------------------------------------------------------------
_SAMPLE_OPERATIONS = [
    {
        "name": "EnergyMarketLast24h",
        "type": "query",
        "signature": "query EnergyMarketLast24h($iso: String!, $location: String) {\n  energyMarkets(filters: [{isoCode: $iso, locationId: $location, granularity: LAST_24H}]) {\n    points {\n      intervalStart\n      priceTotal\n    }\n  }\n}",
    },
    {
        "name": "CreateComplianceSchedule",
        "type": "mutation",
        "signature": "mutation CreateComplianceSchedule($portfolio: String!, $config: JSON!) {\n  createComplianceSchedule(input: {portfolioId: $portfolio, reportConfig: $config, scheduleTimeUtc: \"00:00\"}) {\n    scheduleId\n    nextRun\n  }\n}",
    },
]


def build_graphql_documentation(info: Info) -> Dict[str, Any]:
    request = info.context.get("request")
    if request is not None:
        root_path = getattr(request, "base_url", None)
        playground_url = str(root_path) + "graphql" if root_path else "/graphql"
    else:
        playground_url = "/graphql"

    schema = getattr(info, "schema", None)
    if schema is None:
        sdl_preview = None
    else:  # pragma: no cover - strawberry provides print_schema at runtime
        try:
            from strawberry.printer import print_schema

            sdl_preview = print_schema(schema)
        except Exception:
            sdl_preview = None

    return {
        "playground_url": playground_url,
        "operations": _SAMPLE_OPERATIONS,
        "federated_services": _FEDERATED_ENDPOINTS,
        "schema_sdl": sdl_preview,
    }


def build_client_manifest(info: Info) -> Dict[str, Any]:
    endpoint = "/graphql"
    request = info.context.get("request")
    if request is not None:
        try:
            endpoint = str(request.url_for("graphql_router"))
        except Exception:
            endpoint = str(request.url) if hasattr(request, "url") else "/graphql"

    manifest = {
        "generated_at": int(time.time()),
        "endpoint": endpoint,
        "headers": {
            "X-Aurum-Tenant": info.context.get("tenant_id") or "tenant-placeholder",
        },
        "operations": _SAMPLE_OPERATIONS,
    }
    return manifest


__all__ = [
    "EnergyMarketKey",
    "IsoLmpDataLoader",
    "enforce_complexity_limits",
    "get_iso_loader",
    "resolve_energy_market_series",
    "resolve_compliance_schedules",
    "resolve_reports_for_portfolio",
    "create_compliance_schedule",
    "delete_compliance_schedule",
    "run_compliance_report",
    "GraphQLGateway",
    "get_gateway",
    "build_graphql_documentation",
    "build_client_manifest",
]
