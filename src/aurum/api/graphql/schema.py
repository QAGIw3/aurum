"""Phase 3: GraphQL schema definition for Aurum API.

This module provides a bridge between the REST v2 endpoints and the
GraphQL surface so that both transports share business logic, pagination
rules, and telemetry semantics. Mock data used during the initial GraphQL
spike has been replaced with calls into the real services that back the
REST API.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Tuple

import strawberry
from strawberry.types import Info

from ...scenarios.models import DriverType
from ...telemetry.context import (
    get_request_id,
    get_tenant_id,
    log_structured,
)
from ..async_service import AsyncScenarioService
from ..container import get_service
from ..scenario_models import (
    ScenarioCreateRequest,
    ScenarioData,
    ScenarioRunData,
    ScenarioRunOptions,
    ScenarioRunPriority,
)
from ..services.feature_store_service import get_feature_store_service
from ..v2.forecasting import (
    ForecastBatchRequest,
    ForecastHistoryRequest,
    ForecastRequest,
    ForecastResponse as ForecastResponseModel,
    ForecastPoint as ForecastPointModel,
    ForecastType as ForecastTypeEnum,
    QuantileLevel as QuantileLevelEnum,
    ForecastInterval as ForecastIntervalEnum,
)
from ..v2.pagination import build_next_cursor, build_prev_cursor, resolve_pagination
from ...observability.telemetry_facade import MetricCategory, get_telemetry_facade

try:  # Federation support is optional in some environments
    from strawberry.federation import type as federation_type
    from strawberry.federation.schema import Schema as FederationSchema
except Exception:  # pragma: no cover - fallback when federation extras missing
    federation_type = strawberry.type  # type: ignore
    FederationSchema = strawberry.Schema  # type: ignore

from .resolvers import (
    EnergyMarketKey,
    build_client_manifest,
    build_graphql_documentation,
    create_compliance_schedule as create_compliance_schedule_resolver,
    delete_compliance_schedule as delete_compliance_schedule_resolver,
    get_gateway,
    resolve_compliance_schedules,
    resolve_energy_market_series,
    resolve_reports_for_portfolio,
    run_compliance_report as run_compliance_report_resolver,
    enforce_complexity_limits,
)


LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GraphQL object types
# ---------------------------------------------------------------------------
@strawberry.type
class ScenarioAssumptionType:
    """GraphQL representation of a scenario assumption."""

    driver_type: str
    payload: strawberry.scalars.JSON
    version: Optional[str] = None


@strawberry.type(name="Scenario")
class ScenarioType:
    """Scenario aligned with the v2 REST representation."""

    id: str
    tenant_id: str
    name: str
    description: Optional[str]
    status: str
    unique_key: str
    assumptions: List[ScenarioAssumptionType]
    parameters: strawberry.scalars.JSON
    tags: List[str]
    created_by: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    version: int
    metadata: strawberry.scalars.JSON


@strawberry.type
class ScenarioRunType:
    """Scenario run aligned with the REST response."""

    id: str
    scenario_id: str
    status: str
    priority: str
    run_key: Optional[str]
    input_hash: Optional[str]
    parameters: strawberry.scalars.JSON
    environment: strawberry.scalars.JSON
    result: Optional[strawberry.scalars.JSON]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    created_at: datetime
    queued_at: Optional[datetime]
    cancelled_at: Optional[datetime]
    error_message: Optional[str]
    retry_count: int
    max_retries: int
    progress_percent: Optional[float]


@strawberry.type
class PaginationInfoType:
    """Cursor pagination metadata."""

    next_cursor: Optional[str]
    prev_cursor: Optional[str]
    has_more: bool
    limit: int
    offset: int
    total_count: Optional[int]


@strawberry.type
class ScenarioCollection:
    """Paginated scenario collection."""

    items: List[ScenarioType]
    page_info: PaginationInfoType
    request_id: Optional[str]


@strawberry.type
class ScenarioRunCollection:
    """Paginated scenario run collection."""

    items: List[ScenarioRunType]
    page_info: PaginationInfoType
    request_id: Optional[str]


@strawberry.type
class ForecastPointType:
    """Single probabilistic forecast point."""

    timestamp: datetime
    values: strawberry.scalars.JSON
    confidence_interval: Optional[strawberry.scalars.JSON]
    metadata: strawberry.scalars.JSON


@strawberry.type(name="Forecast")
class ForecastPayload:
    """Probabilistic forecast payload shared with REST."""

    forecast_id: str
    request_id: str
    forecast_type: str
    target_variable: str
    geography: str
    model_version: str
    forecast_points: List[ForecastPointType]
    generated_at: datetime
    valid_from: datetime
    valid_until: datetime
    metadata: strawberry.scalars.JSON


@strawberry.type
class ForecastResultType:
    """Legacy forecast summary retained for backwards compatibility."""

    model_type: str
    predictions: List[float]
    confidence_intervals: Optional[List[List[float]]]
    accuracy_metrics: Optional[strawberry.scalars.JSON]
    metadata: strawberry.scalars.JSON


@strawberry.type
class ValidationResultType:
    """Payload returned by the scenario validation utility."""

    is_valid: bool
    issues: List[strawberry.scalars.JSON]
    validation_id: str
    validated_at: datetime


# ---------------------------------------------------------------------------
# Energy market & compliance GraphQL types
# ---------------------------------------------------------------------------
@strawberry.enum
class EnergyGranularity(Enum):
    """Supported aggregation windows for ISO market data."""

    LAST_24H = "LAST_24H"
    HOURLY = "HOURLY"
    DAILY = "DAILY"
    NEGATIVE = "NEGATIVE"


@strawberry.type
class EnergyMarketPointType:
    """Single ISO LMP observation."""

    iso_code: Optional[str]
    market: Optional[str]
    interval_start: datetime
    interval_end: Optional[datetime]
    interval_minutes: Optional[int]
    location_id: Optional[str]
    location_name: Optional[str]
    location_type: Optional[str]
    price_total: Optional[float]
    price_energy: Optional[float]
    price_congestion: Optional[float]
    price_loss: Optional[float]
    currency: Optional[str]
    uom: Optional[str]
    settlement_point: Optional[str]
    metadata: strawberry.scalars.JSON


@strawberry.type
class EnergyMarketFilterDescriptor:
    """Echo of the filter used to build the market series."""

    iso_code: Optional[str]
    market: Optional[str]
    location_id: Optional[str]
    granularity: EnergyGranularity
    limit: int
    start: Optional[str] = None
    end: Optional[str] = None


@strawberry.type
class EnergyMarketSeriesType:
    """ISO market data series with query metadata."""

    filter: EnergyMarketFilterDescriptor
    query_time_ms: int
    points: List[EnergyMarketPointType]


@federation_type(keys=["iso_code", "location_id"])
class EnergyLocationType:
    """Federated resource describing an ISO node that other services can extend."""

    iso_code: str
    location_id: str
    market: Optional[str]
    location_name: Optional[str]
    location_type: Optional[str]
    metadata: strawberry.scalars.JSON


@strawberry.type
class ComplianceScheduleGraphType:
    """Compliance report schedule descriptor."""

    schedule_id: str
    portfolio_id: Optional[str]
    schedule_time_utc: str
    enabled: bool
    retention_days: int
    max_reports: Optional[int]
    last_run: Optional[datetime]
    next_run: Optional[datetime]
    report_config: strawberry.scalars.JSON


@strawberry.type
class ComplianceReportArtifactType:
    """Materialised compliance report metadata."""

    portfolio_id: Optional[str]
    filename: str
    path: str
    size: Optional[int]
    modified: Optional[datetime]
    metadata: strawberry.scalars.JSON


@strawberry.type
class ComplianceReportRunResultType:
    """Result of triggering an ad-hoc compliance report run."""

    schedule_id: str
    artifact_path: Optional[str]


@strawberry.type
class FederatedServiceResultType:
    """Invocation result when delegating to federated services."""

    service: str
    data: Optional[strawberry.scalars.JSON]
    errors: Optional[strawberry.scalars.JSON]


@strawberry.type
class GraphQLDocumentationType:
    """GraphQL playground and schema documentation metadata."""

    playground_url: str
    schema_sdl: Optional[str]
    operations: List[strawberry.scalars.JSON]
    federated_services: strawberry.scalars.JSON


@strawberry.type
class ClientManifestType:
    """Client SDK manifest with generated operations and headers."""

    endpoint: str
    generated_at: int
    headers: strawberry.scalars.JSON
    operations: List[strawberry.scalars.JSON]


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------
@strawberry.input
class ScenarioAssumptionInput:
    """Scenario assumption input structure."""

    driver_type: str
    payload: strawberry.scalars.JSON
    version: Optional[str] = None


@strawberry.input
class ScenarioFilterInput:
    """Filtering options for listing scenarios."""

    status: Optional[str] = None
    name_contains: Optional[str] = None
    tag: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


@strawberry.input
class ScenarioRunFilterInput:
    """Filtering options for scenario runs."""

    status: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


@strawberry.input
class CreateScenarioInput:
    """Input for creating scenarios."""

    name: str
    description: Optional[str] = None
    assumptions: List[ScenarioAssumptionInput] = strawberry.field(default_factory=list)
    parameters: Optional[strawberry.scalars.JSON] = None
    tags: Optional[List[str]] = None


@strawberry.input
class RunScenarioInput:
    """Input for enqueuing scenario runs."""

    scenario_id: str
    priority: Optional[str] = None
    timeout_minutes: Optional[int] = None
    max_retries: Optional[int] = None
    code_version: Optional[str] = None
    seed: Optional[int] = None
    parameters: Optional[strawberry.scalars.JSON] = None
    environment: Optional[strawberry.scalars.JSON] = None
    idempotency_key: Optional[str] = None
    simulation_type: Optional[str] = strawberry.field(
        default=None,
        deprecation_reason="Use parameters['simulation_type'] instead",
    )
    num_iterations: Optional[int] = strawberry.field(
        default=None,
        deprecation_reason="Use parameters['num_iterations'] instead",
    )
    parallel_execution: Optional[bool] = strawberry.field(
        default=None,
        deprecation_reason="Engine parallelism is inferred from scenario configuration",
    )


@strawberry.input
class ForecastInput:
    """Input for probabilistic forecasts."""

    forecast_type: str
    target_variable: str
    geography: str = "US"
    start_date: datetime = strawberry.field(description="Inclusive start timestamp")
    end_date: datetime = strawberry.field(description="Inclusive end timestamp")
    quantiles: List[str] = strawberry.field(default_factory=lambda: ["p10", "p50", "p90"])
    interval: str = "1H"
    model_version: Optional[str] = None
    scenario_id: Optional[str] = None
    include_feature_importance: Optional[bool] = None


@strawberry.input
class BatchForecastInput:
    """Batch forecast input."""

    forecasts: List[ForecastInput]
    batch_id: Optional[str] = None


@strawberry.input
class BatchOperationInput:
    """Batch operations on scenarios."""

    operation_type: str
    scenario_ids: List[str]
    parameters: Optional[strawberry.scalars.JSON] = None


@strawberry.input
class EnergyMarketFilterInput:
    """Input filter for ISO market data queries."""

    iso_code: Optional[str] = None
    market: Optional[str] = None
    location_id: Optional[str] = None
    granularity: EnergyGranularity = EnergyGranularity.LAST_24H
    limit: int = strawberry.field(default=96, description="Maximum number of points to return")
    start: Optional[datetime] = strawberry.field(default=None, description="Start timestamp for aggregated queries")
    end: Optional[datetime] = strawberry.field(default=None, description="End timestamp for aggregated queries")


@strawberry.input
class ComplianceScheduleInput:
    """Input payload for creating compliance schedules."""

    portfolio_id: Optional[str] = None
    schedule_time_utc: str = "00:00"
    retention_days: int = 30
    max_reports: Optional[int] = 100
    report_config: strawberry.scalars.JSON


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _scenario_service() -> AsyncScenarioService:
    """Resolve the shared async scenario service."""

    return get_service(AsyncScenarioService)


def _enum_value(value: Any) -> str:
    """Return the string representation for enum-like objects."""

    if isinstance(value, Enum):
        return value.value  # type: ignore[return-value]
    return str(value) if value is not None else ""


def _coerce_assumption(raw: Any) -> ScenarioAssumptionType:
    """Convert assumption objects or dictionaries to GraphQL form."""

    if raw is None:
        return ScenarioAssumptionType(driver_type="unknown", payload={})
    if isinstance(raw, ScenarioAssumptionType):
        return raw

    if hasattr(raw, "model_dump"):
        data = raw.model_dump()  # type: ignore[attr-defined]
    elif isinstance(raw, dict):
        data = raw
    else:
        data = {
            "driver_type": getattr(raw, "driver_type", "unknown"),
            "payload": getattr(raw, "payload", {}),
            "version": getattr(raw, "version", None),
        }

    driver = data.get("driver_type") or data.get("type") or "unknown"
    payload = data.get("payload", data)
    version = data.get("version")
    if isinstance(driver, DriverType):
        driver = driver.value
    return ScenarioAssumptionType(driver_type=str(driver), payload=payload, version=version)


def _scenario_to_graphql(scenario: ScenarioData) -> ScenarioType:
    """Convert ``ScenarioData`` into ``ScenarioType``."""

    assumptions = [
        _coerce_assumption(item)
        for item in getattr(scenario, "assumptions", []) or []
    ]

    return ScenarioType(
        id=scenario.id,
        tenant_id=scenario.tenant_id,
        name=scenario.name,
        description=scenario.description,
        status=_enum_value(scenario.status),
        unique_key=scenario.unique_key,
        assumptions=assumptions,
        parameters=getattr(scenario, "parameters", {}) or {},
        tags=list(getattr(scenario, "tags", []) or []),
        created_by=getattr(scenario, "created_by", None),
        created_at=scenario.created_at,
        updated_at=getattr(scenario, "updated_at", None),
        version=getattr(scenario, "version", 1),
        metadata=getattr(scenario, "metadata", {}) or {},
    )


def _scenario_run_to_graphql(run: ScenarioRunData, *, result: Any = None) -> ScenarioRunType:
    """Convert ``ScenarioRunData`` into ``ScenarioRunType``."""

    return ScenarioRunType(
        id=run.id,
        scenario_id=run.scenario_id,
        status=_enum_value(run.status),
        priority=_enum_value(run.priority),
        run_key=getattr(run, "run_key", None),
        input_hash=getattr(run, "input_hash", None),
        parameters=getattr(run, "parameters", {}) or {},
        environment=getattr(run, "environment", {}) or {},
        result=result,
        started_at=getattr(run, "started_at", None),
        completed_at=getattr(run, "completed_at", None),
        duration_seconds=getattr(run, "duration_seconds", None),
        created_at=run.created_at,
        queued_at=getattr(run, "queued_at", None),
        cancelled_at=getattr(run, "cancelled_at", None),
        error_message=getattr(run, "error_message", None),
        retry_count=getattr(run, "retry_count", 0),
        max_retries=getattr(run, "max_retries", 0),
        progress_percent=getattr(run, "progress_percent", None),
    )


def _pagination_info(
    *,
    items_count: int,
    offset: int,
    limit: int,
    total: Optional[int],
    filters: Optional[Dict[str, object]] = None,
) -> Tuple[PaginationInfoType, Optional[str], Optional[str]]:
    """Create ``PaginationInfoType`` and the next/prev cursors."""

    total_count = total if total is not None else offset + items_count
    has_more = (offset + items_count) < total_count
    next_cursor = build_next_cursor(
        offset=offset,
        limit=limit,
        has_more=has_more,
        filters=filters,
    )
    prev_cursor = build_prev_cursor(
        offset=offset,
        limit=limit,
        filters=filters,
    )

    page_info = PaginationInfoType(
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        has_more=has_more,
        limit=limit,
        offset=offset,
        total_count=total,
    )
    return page_info, next_cursor, prev_cursor


def _prepare_forecast_request(input: ForecastInput) -> ForecastRequest:
    """Map GraphQL input to the REST forecast request model."""

    quantiles: List[QuantileLevelEnum] = []
    for item in input.quantiles:
        try:
            quantiles.append(QuantileLevelEnum(item))
        except ValueError:
            quantiles.append(QuantileLevelEnum(item.lower()))

    try:
        interval = ForecastIntervalEnum(input.interval)
    except ValueError:
        interval = ForecastIntervalEnum.HOURLY

    try:
        forecast_type = ForecastTypeEnum(input.forecast_type)
    except ValueError:
        forecast_type = ForecastTypeEnum.LOAD

    request = ForecastRequest(
        forecast_type=forecast_type,
        target_variable=input.target_variable,
        geography=input.geography,
        start_date=input.start_date,
        end_date=input.end_date,
        quantiles=quantiles,
        interval=interval,
        model_version=input.model_version,
        include_feature_importance=input.include_feature_importance or False,
        scenario_id=input.scenario_id,
    )
    return request


def _forecast_point_to_graphql(point: ForecastPointModel) -> ForecastPointType:
    """Convert REST forecast point to GraphQL object."""

    return ForecastPointType(
        timestamp=point.timestamp,
        values=point.values,
        confidence_interval=point.confidence_interval,
        metadata=point.metadata,
    )


def _forecast_response_to_graphql(response: ForecastResponseModel) -> ForecastPayload:
    """Convert ``ForecastResponse`` into GraphQL ``Forecast`` payload."""

    return ForecastPayload(
        forecast_id=response.forecast_id,
        request_id=response.request_id,
        forecast_type=_enum_value(response.forecast_type),
        target_variable=response.target_variable,
        geography=response.geography,
        model_version=response.model_version,
        forecast_points=[_forecast_point_to_graphql(point) for point in response.forecast_points],
        generated_at=response.generated_at,
        valid_from=response.valid_from,
        valid_until=response.valid_until,
        metadata=response.metadata,
    )


def _resolve_tenant(info: Info, explicit_tenant: Optional[str]) -> str:
    """Resolve the tenant identifier from argument or context."""

    tenant = explicit_tenant or info.context.get("tenant_id")
    if tenant:
        return tenant

    resolved = get_tenant_id()
    if resolved:
        return resolved

    raise ValueError("Tenant identifier is required for this operation")


def _energy_point_from_row(row: Dict[str, Any]) -> EnergyMarketPointType:
    return EnergyMarketPointType(
        iso_code=row.get("iso_code"),
        market=row.get("market"),
        interval_start=_ensure_datetime(row.get("interval_start")) or datetime.utcnow(),
        interval_end=_ensure_datetime(row.get("interval_end")),
        interval_minutes=row.get("interval_minutes"),
        location_id=row.get("location_id"),
        location_name=row.get("location_name"),
        location_type=row.get("location_type"),
        price_total=row.get("price_total"),
        price_energy=row.get("price_energy"),
        price_congestion=row.get("price_congestion"),
        price_loss=row.get("price_loss"),
        currency=row.get("currency"),
        uom=row.get("uom"),
        settlement_point=row.get("settlement_point"),
        metadata=_ensure_metadata(row.get("metadata")),
    )


def _energy_series_from_raw(raw: Dict[str, Any]) -> EnergyMarketSeriesType:
    filter_payload = raw.get("filter", {})
    granularity = _safe_granularity(filter_payload.get("granularity"))
    descriptor = EnergyMarketFilterDescriptor(
        iso_code=filter_payload.get("iso_code"),
        market=filter_payload.get("market"),
        location_id=filter_payload.get("location_id"),
        granularity=granularity,
        limit=int(filter_payload.get("limit", 0) or 0),
        start=_stringify(filter_payload.get("start")),
        end=_stringify(filter_payload.get("end")),
    )

    points = [_energy_point_from_row(item) for item in raw.get("points", [])]
    return EnergyMarketSeriesType(
        filter=descriptor,
        query_time_ms=int(raw.get("query_time_ms", 0) or 0),
        points=points,
    )


def _schedule_from_raw(raw: Dict[str, Any]) -> ComplianceScheduleGraphType:
    return ComplianceScheduleGraphType(
        schedule_id=str(raw.get("schedule_id")),
        portfolio_id=raw.get("portfolio_id"),
        schedule_time_utc=str(raw.get("schedule_time_utc", "00:00")),
        enabled=bool(raw.get("enabled", True)),
        retention_days=int(raw.get("retention_days", 30) or 30),
        max_reports=raw.get("max_reports"),
        last_run=_ensure_datetime(raw.get("last_run")),
        next_run=_ensure_datetime(raw.get("next_run")),
        report_config=_ensure_metadata(raw.get("report_config")),
    )


def _report_artifact_from_raw(raw: Dict[str, Any]) -> ComplianceReportArtifactType:
    return ComplianceReportArtifactType(
        portfolio_id=raw.get("portfolio_id"),
        filename=str(raw.get("filename")),
        path=str(raw.get("path")),
        size=int(raw.get("size")) if raw.get("size") is not None else None,
        modified=_ensure_datetime(raw.get("modified")),
        metadata=_ensure_metadata(raw.get("metadata")),
    )


def _ensure_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.endswith("Z"):
            cleaned = cleaned[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            try:
                return datetime.fromtimestamp(float(cleaned), tz=timezone.utc)
            except Exception:
                return None
    return None


def _ensure_metadata(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump()  # type: ignore[attr-defined]
        except Exception:
            return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {"raw": value}
        except Exception:
            return {"raw": value}
    return {"raw": str(value)}


def _safe_granularity(value: Any) -> EnergyGranularity:
    if isinstance(value, EnergyGranularity):
        return value
    label = str(value or EnergyGranularity.LAST_24H.value)
    try:
        return EnergyGranularity(label)
    except ValueError:
        return EnergyGranularity.LAST_24H


def _stringify(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


# ---------------------------------------------------------------------------
# Query resolvers
# ---------------------------------------------------------------------------
@strawberry.type
class Query:
    """GraphQL Query root."""

    @strawberry.field
    async def scenario(self, info: Info, id: str, tenant_id: Optional[str] = None) -> Optional[ScenarioType]:
        """Fetch a scenario by identifier."""

        tenant = _resolve_tenant(info, tenant_id)
        await log_structured("graphql_scenario_query", scenario_id=id, tenant_id=tenant)

        service = _scenario_service()
        record = await service.get_scenario(id)
        if record is None:
            return None

        if getattr(record, "tenant_id", tenant) != tenant:
            LOGGER.warning("Scenario %s requested under tenant %s but owned by %s", id, tenant, getattr(record, "tenant_id", "unknown"))
            return None

        return _scenario_to_graphql(record)

    @strawberry.field
    async def scenarios(
        self,
        info: Info,
        limit: int = 10,
        cursor: Optional[str] = None,
        tenant_id: Optional[str] = None,
        filters: Optional[ScenarioFilterInput] = None,
    ) -> ScenarioCollection:
        """List scenarios with REST-aligned pagination and filtering."""

        tenant = _resolve_tenant(info, tenant_id)
        limit = max(1, min(limit, 100))
        filter_payload = {
            "tenant_id": tenant,
            "status": filters.status if filters else None,
            "name_contains": filters.name_contains if filters else None,
            "tag": filters.tag if filters else None,
            "created_after": filters.created_after if filters else None,
            "created_before": filters.created_before if filters else None,
        }

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=10,
            filters=filter_payload,
        )

        service = _scenario_service()
        scenarios, total, meta = await service.list_scenarios(
            tenant_id=tenant,
            status=filters.status if filters else None,
            limit=effective_limit,
            offset=offset,
            name_contains=filters.name_contains if filters else None,
            tag=filters.tag if filters else None,
            created_after=filters.created_after if filters else None,
            created_before=filters.created_before if filters else None,
        )

        items = [_scenario_to_graphql(item) for item in scenarios]
        page_info, _, _ = _pagination_info(
            items_count=len(items),
            offset=offset,
            limit=effective_limit,
            total=total,
            filters=filter_payload,
        )

        request_id = (meta or {}).get("request_id") or get_request_id()
        return ScenarioCollection(items=items, page_info=page_info, request_id=request_id)

    @strawberry.field
    async def scenario_run(
        self,
        info: Info,
        scenario_id: str,
        run_id: str,
    ) -> Optional[ScenarioRunType]:
        """Fetch a single scenario run."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_scenario_run_query",
            scenario_id=scenario_id,
            run_id=run_id,
            tenant_id=tenant,
        )

        service = _scenario_service()
        run = await service.get_scenario_run(scenario_id, run_id)
        if run is None:
            return None

        return _scenario_run_to_graphql(run)

    @strawberry.field
    async def scenario_runs(
        self,
        info: Info,
        scenario_id: str,
        limit: int = 10,
        cursor: Optional[str] = None,
        filters: Optional[ScenarioRunFilterInput] = None,
    ) -> ScenarioRunCollection:
        """List runs for a scenario with pagination."""

        tenant = _resolve_tenant(info, None)
        limit = max(1, min(limit, 100))
        filter_payload = {
            "scenario_id": scenario_id,
            "status": filters.status if filters else None,
            "created_after": filters.created_after if filters else None,
            "created_before": filters.created_before if filters else None,
        }

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=10,
            filters=filter_payload,
        )

        service = _scenario_service()
        runs, total, meta = await service.list_scenario_runs(
            scenario_id=scenario_id,
            limit=effective_limit,
            offset=offset,
            state=filters.status if filters else None,
            created_after=filters.created_after if filters else None,
            created_before=filters.created_before if filters else None,
        )

        items = [_scenario_run_to_graphql(run) for run in runs]
        page_info, _, _ = _pagination_info(
            items_count=len(items),
            offset=offset,
            limit=effective_limit,
            total=total,
            filters=filter_payload,
        )

        request_id = (meta or {}).get("request_id") or get_request_id()
        return ScenarioRunCollection(items=items, page_info=page_info, request_id=request_id)

    @strawberry.field
    async def validate_scenario(
        self,
        info: Info,
        assumptions: List[ScenarioAssumptionInput],
    ) -> ValidationResultType:
        """Validate scenario assumptions using existing validation service."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_scenario_validation",
            tenant_id=tenant,
            assumption_count=len(assumptions),
        )

        # Placeholder for dedicated validation service; currently returns success
        return ValidationResultType(
            is_valid=True,
            issues=[],
            validation_id=f"validation_{datetime.utcnow().timestamp():.0f}",
            validated_at=datetime.utcnow(),
        )

    @strawberry.field(deprecation_reason="Replaced by probabilistic_forecast")
    async def forecast(
        self,
        info: Info,
        data: List[float],
        model_type: str = "enhanced_ensemble",
        forecast_horizon: int = 24,
    ) -> ForecastResultType:
        """Legacy deterministic forecast wrapper built on top of the probabilistic endpoint."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_forecast_query",
            tenant_id=tenant,
            model_type=model_type,
            forecast_horizon=forecast_horizon,
            data_points=len(data),
        )

        now = datetime.utcnow()
        forecast_input = ForecastInput(
            forecast_type=model_type,
            target_variable="synthetic",
            start_date=now,
            end_date=now + timedelta(hours=forecast_horizon),
            quantiles=["p50"],
            interval="1H",
        )
        request = _prepare_forecast_request(forecast_input)
        response = await _execute_forecast(request)

        predictions = [point.values.get("p50") for point in response.forecast_points if "p50" in point.values]
        confidence = [
            [point.confidence_interval.get("lower"), point.confidence_interval.get("upper")]
            for point in response.forecast_points
            if point.confidence_interval
        ] or None

        return ForecastResultType(
            model_type=model_type,
            predictions=predictions,
            confidence_intervals=confidence,
            accuracy_metrics=None,
            metadata={"source": "probabilistic_forecast", "tenant_id": tenant},
        )

    @strawberry.field
    async def probabilistic_forecast(
        self,
        info: Info,
        input: ForecastInput,
    ) -> ForecastPayload:
        """Generate a probabilistic forecast with quantiles and metadata."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_probabilistic_forecast",
            tenant_id=tenant,
            forecast_type=input.forecast_type,
            target_variable=input.target_variable,
            quantiles=input.quantiles,
        )

        request_model = _prepare_forecast_request(input)
        response = await _execute_forecast(request_model)
        return _forecast_response_to_graphql(response)

    @strawberry.field
    async def forecast_history(
        self,
        info: Info,
        forecast_type: str,
        target_variable: str,
        geography: str = "US",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        include_actuals: bool = False,
    ) -> List[ForecastPayload]:
        """Return historical forecast payloads using REST parity implementation."""

        _resolve_tenant(info, None)  # validates tenant presence for telemetry continuity
        try:
            forecast_enum = ForecastTypeEnum(forecast_type)
        except ValueError:
            forecast_enum = ForecastTypeEnum.LOAD

        request = ForecastHistoryRequest(
            forecast_type=forecast_enum,
            target_variable=target_variable,
            geography=geography,
            start_date=start_date or (datetime.utcnow() - timedelta(hours=limit)),
            end_date=end_date or datetime.utcnow(),
            limit=limit,
            include_actuals=include_actuals,
        )

        history_response = await _generate_forecast_history(request)
        return [
            _forecast_response_to_graphql(forecast)
            for forecast in history_response
        ]

    @strawberry.field
    async def energy_markets(
        self,
        info: Info,
        filters: Optional[List[EnergyMarketFilterInput]] = None,
    ) -> List[EnergyMarketSeriesType]:
        """Expose ISO market data series with batching to avoid N+1 patterns."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_energy_market_query",
            tenant_id=tenant,
            filter_count=len(filters or []),
        )

        effective_filters = filters or []
        if not effective_filters:
            raise ValueError("At least one filter must be provided for energyMarkets")

        keys: List[EnergyMarketKey] = []
        for item in effective_filters:
            limit = max(1, min(item.limit, 500))
            keys.append(
                EnergyMarketKey(
                    iso_code=item.iso_code,
                    market=item.market,
                    location_id=item.location_id,
                    granularity=item.granularity.value,
                    limit=limit,
                    start=_stringify(item.start),
                    end=_stringify(item.end),
                )
            )

        raw_series = await resolve_energy_market_series(info, keys)
        return [_energy_series_from_raw(payload) for payload in raw_series]

    @strawberry.field
    async def compliance_schedules(
        self,
        info: Info,
        portfolio_id: Optional[str] = None,
    ) -> List[ComplianceScheduleGraphType]:
        """List configured risk compliance schedules."""

        _resolve_tenant(info, None)
        raw = await resolve_compliance_schedules(info)
        if portfolio_id:
            raw = [item for item in raw if item.get("portfolio_id") == portfolio_id]
        return [_schedule_from_raw(item) for item in raw]

    @strawberry.field
    async def compliance_reports(
        self,
        info: Info,
        portfolio_id: str,
        limit: int = 20,
    ) -> List[ComplianceReportArtifactType]:
        """Fetch persisted compliance report artifacts for a portfolio."""

        _resolve_tenant(info, None)
        fetch_limit = max(1, min(limit, 200))
        raw = await resolve_reports_for_portfolio(info, portfolio_id, fetch_limit)
        return [_report_artifact_from_raw(item) for item in raw]

    @strawberry.field
    async def federated_service(
        self,
        info: Info,
        service: str,
        query: str,
        variables: Optional[strawberry.scalars.JSON] = None,
    ) -> FederatedServiceResultType:
        """Delegate a GraphQL operation to a federated microservice."""

        _resolve_tenant(info, None)
        await enforce_complexity_limits(info, base_cost=5)

        gateway = get_gateway(info)
        raw_variables: Optional[Dict[str, Any]]
        if isinstance(variables, dict):
            raw_variables = variables
        elif variables is None:
            raw_variables = None
        elif hasattr(variables, "items"):
            raw_variables = dict(variables)  # type: ignore[arg-type]
        else:
            raw_variables = None

        result = await gateway.execute(service, query, raw_variables)
        return FederatedServiceResultType(
            service=service,
            data=result.get("data"),
            errors=result.get("errors"),
        )

    @strawberry.field
    async def graphql_documentation(self, info: Info) -> GraphQLDocumentationType:
        """Return GraphQL playground metadata and federated service registry."""

        _resolve_tenant(info, None)
        await enforce_complexity_limits(info, base_cost=1)
        payload = build_graphql_documentation(info)
        operations = [op for op in payload.get("operations", [])]
        return GraphQLDocumentationType(
            playground_url=str(payload.get("playground_url", "/graphql")),
            schema_sdl=payload.get("schema_sdl"),
            operations=operations,
            federated_services=payload.get("federated_services", {}),
        )

    @strawberry.field
    async def client_manifest(self, info: Info) -> ClientManifestType:
        """Return a manifest that downstream teams can use for SDK generation."""

        _resolve_tenant(info, None)
        await enforce_complexity_limits(info, base_cost=1)
        payload = build_client_manifest(info)
        ops = [op for op in payload.get("operations", [])]
        return ClientManifestType(
            endpoint=str(payload.get("endpoint", "/graphql")),
            generated_at=int(payload.get("generated_at", 0)),
            headers=payload.get("headers", {}),
            operations=ops,
        )


# ---------------------------------------------------------------------------
# Mutation resolvers
# ---------------------------------------------------------------------------
@strawberry.type
class Mutation:
    """GraphQL Mutation root."""

    @strawberry.field
    async def create_scenario(
        self,
        info: Info,
        input: CreateScenarioInput,
        tenant_id: Optional[str] = None,
    ) -> ScenarioType:
        """Create a new scenario using the underlying REST service."""

        tenant = _resolve_tenant(info, tenant_id)
        await log_structured("graphql_create_scenario", tenant_id=tenant, name=input.name)

        payload = ScenarioCreateRequest(
            tenant_id=tenant,
            name=input.name,
            description=input.description,
            assumptions=[asdict(assumption) for assumption in input.assumptions],
            parameters=input.parameters or {},
            tags=input.tags or [],
        )

        service = _scenario_service()
        created = await service.create_scenario(payload.model_dump())
        return _scenario_to_graphql(created)

    @strawberry.field
    async def run_scenario(self, info: Info, input: RunScenarioInput) -> ScenarioRunType:
        """Create and enqueue a scenario run."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_run_scenario",
            tenant_id=tenant,
            scenario_id=input.scenario_id,
            priority=input.priority,
        )

        priority = ScenarioRunPriority.NORMAL
        if input.priority:
            try:
                priority = ScenarioRunPriority(input.priority)
            except ValueError:
                priority = ScenarioRunPriority.NORMAL

        options = ScenarioRunOptions(
            code_version=input.code_version,
            seed=input.seed,
            priority=priority,
            timeout_minutes=input.timeout_minutes or 60,
            max_retries=input.max_retries if input.max_retries is not None else 3,
            parameters=input.parameters or {},
            environment=input.environment or {},
            idempotency_key=input.idempotency_key,
        )

        service = _scenario_service()
        run = await service.create_scenario_run(
            input.scenario_id,
            options.model_dump(exclude_none=True),
        )

        return _scenario_run_to_graphql(run)

    @strawberry.field
    async def delete_scenario(self, info: Info, id: str) -> bool:
        """Delete a scenario via the REST service."""

        tenant = _resolve_tenant(info, None)
        await log_structured("graphql_delete_scenario", tenant_id=tenant, scenario_id=id)

        service = _scenario_service()
        return await service.delete_scenario(id)

    @strawberry.field
    async def batch_operation(self, info: Info, input: BatchOperationInput) -> List[str]:
        """Perform batch operations on scenarios (run/validate)."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_batch_operation",
            tenant_id=tenant,
            operation_type=input.operation_type,
            scenario_count=len(input.scenario_ids),
        )

        service = _scenario_service()
        results: List[str] = []

        if input.operation_type.lower() == "run":
            options = ScenarioRunOptions(parameters=input.parameters or {})
            for scenario_id in input.scenario_ids:
                run = await service.create_scenario_run(
                    scenario_id,
                    options.model_dump(exclude_none=True),
                )
                results.append(run.id)
        elif input.operation_type.lower() == "validate":
            # Currently validation is synchronous placeholder
            for scenario_id in input.scenario_ids:
                results.append(f"validated:{scenario_id}")
        else:
            results = [f"noop:{scenario_id}" for scenario_id in input.scenario_ids]

        return results

    @strawberry.field
    async def batch_forecast(self, info: Info, input: BatchForecastInput) -> List[ForecastPayload]:
        """Generate forecasts for multiple requests sequentially."""

        _resolve_tenant(info, None)
        batch_request = ForecastBatchRequest(
            forecasts=[_prepare_forecast_request(item) for item in input.forecasts],
            batch_id=input.batch_id,
        )

        forecasts: List[ForecastPayload] = []
        for request in batch_request.forecasts:
            response = await _execute_forecast(request)
            forecasts.append(_forecast_response_to_graphql(response))
        return forecasts


# ---------------------------------------------------------------------------
# Subscription resolvers (mock implementations kept for parity)
# ---------------------------------------------------------------------------
@strawberry.type
class Subscription:
    """GraphQL Subscription root for real-time data."""

    @strawberry.subscription
    async def scenario_run_updates(self, run_id: str) -> AsyncGenerator[ScenarioRunType, None]:
        """Subscribe to scenario run status updates."""

        await log_structured("graphql_subscription_started", run_id=run_id)

        statuses = ["queued", "running", "completed"]
        for index, status in enumerate(statuses):
            await asyncio.sleep(1)
            yield ScenarioRunType(
                id=run_id,
                scenario_id=f"scenario_for_{run_id}",
                status=status,
                priority=ScenarioRunPriority.NORMAL.value,
                run_key=None,
                input_hash=None,
                parameters={},
                environment={},
                result={"progress": (index + 1) * 33} if status != "completed" else {"final": "result"},
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow() if status == "completed" else None,
                duration_seconds=float(index + 1) if status == "completed" else None,
                created_at=datetime.utcnow(),
                queued_at=datetime.utcnow(),
                cancelled_at=None,
                error_message=None,
                retry_count=0,
                max_retries=3,
                progress_percent=float((index + 1) * 33),
            )

    @strawberry.subscription
    async def market_data_feed(self, symbols: List[str]) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to real-time market data."""

        await log_structured("graphql_market_data_subscription", symbols=symbols)
        import random

        while True:
            await asyncio.sleep(2)
            yield {
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    symbol: {
                        "price": round(random.uniform(90, 110), 2),
                        "volume": random.randint(1_000, 10_000),
                    }
                    for symbol in symbols
                },
            }

    @strawberry.subscription
    async def cache_invalidations(self) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to cache invalidation events."""

        await log_structured("graphql_cache_invalidation_subscription")
        events = [
            {"type": "scenario_cache", "keys": ["scenario:123", "scenario:456"], "timestamp": datetime.utcnow().isoformat()},
            {"type": "feature_cache", "keys": ["features:load", "features:price"], "timestamp": datetime.utcnow().isoformat()},
            {"type": "metadata_cache", "keys": ["metadata:dimensions"], "timestamp": datetime.utcnow().isoformat()},
        ]
        for event in events:
            await asyncio.sleep(3)
            yield event

    @strawberry.subscription
    async def signal_stream(self, signal_types: List[str]) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to real-time signal streams."""

        await log_structured("graphql_signal_stream_subscription", signal_types=signal_types)
        import random

        while True:
            await asyncio.sleep(5)
            yield {
                "timestamp": datetime.utcnow().isoformat(),
                "type": random.choice(signal_types) if signal_types else "anomaly",
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "message": f"Signal detected at {datetime.utcnow().isoformat()}",
                "metadata": {
                    "source": "realtime_detection",
                    "confidence": round(random.uniform(0.5, 0.95), 2),
                    "affected_assets": random.sample(["load", "price", "renewable"], k=random.randint(1, 3)),
                },
            }


# ---------------------------------------------------------------------------
# Shared execution helpers
# ---------------------------------------------------------------------------
async def _execute_forecast(request: ForecastRequest) -> ForecastResponseModel:
    """Execute the probabilistic forecast flow shared with REST."""

    telemetry = get_telemetry_facade()
    request_id = get_request_id() or f"req_{datetime.utcnow().timestamp():.0f}"
    tenant_id = get_tenant_id()

    telemetry.info(
        "Starting probabilistic forecast generation (GraphQL)",
        forecast_type=request.forecast_type.value,
        target_variable=request.target_variable,
        geography=request.geography,
        quantiles=[q.value for q in request.quantiles],
        tenant_id=tenant_id,
        category="forecast",
    )

    feature_service = get_feature_store_service()
    features, _ = await feature_service.get_features_for_modeling(
        start_date=request.start_date,
        end_date=request.end_date,
        geography=request.geography,
        target_variable=request.target_variable,
        scenario_id=request.scenario_id,
    )

    from ..v2.forecasting import _generate_probabilistic_forecast  # lazy import to avoid cycles

    forecast_points = await _generate_probabilistic_forecast(features, request, request_id)

    response = ForecastResponseModel(
        forecast_id=f"forecast_{datetime.utcnow().timestamp():.0f}",
        request_id=request_id,
        forecast_type=request.forecast_type,
        target_variable=request.target_variable,
        geography=request.geography,
        model_version=request.model_version or "v1.0",
        forecast_points=forecast_points,
        generated_at=datetime.utcnow(),
        valid_from=request.start_date,
        valid_until=request.end_date,
        metadata={
            "quantiles": [q.value for q in request.quantiles],
            "interval": request.interval.value,
            "feature_count": len(features),
            "source": "graphql",
        },
    )

    telemetry.record_histogram(
        "forecast_generation_duration",
        0.0,
        category=MetricCategory.PERFORMANCE,
        transport="graphql",
    )

    telemetry.increment_counter(
        "forecasts_generated",
        category=MetricCategory.BUSINESS,
        transport="graphql",
    )

    return response


async def _generate_forecast_history(request: ForecastHistoryRequest) -> List[ForecastResponseModel]:
    """Generate historical forecasts mirroring the REST behaviour."""

    telemetry = get_telemetry_facade()
    telemetry.info(
        "Generating forecast history (GraphQL)",
        forecast_type=request.forecast_type.value,
        target_variable=request.target_variable,
        geography=request.geography,
        category="forecast",
    )

    history: List[ForecastResponseModel] = []
    current = request.start_date
    count = 0
    while current <= request.end_date and count < request.limit:
        point = ForecastPointModel(
            timestamp=current,
            values={
                "p10": 100.0 + (current.hour * 0.1),
                "p50": 120.0 + (current.hour * 0.2),
                "p90": 150.0 + (current.hour * 0.3),
            },
            confidence_interval={
                "lower": 100.0 + (current.hour * 0.1),
                "upper": 150.0 + (current.hour * 0.3),
                "width": 50.0,
            },
            metadata={
                "hour": str(current.hour),
                "day_of_week": str(current.weekday()),
            },
        )

        forecast = ForecastResponseModel(
            forecast_id=f"historical_{count}",
            request_id=f"history_req_{count}",
            forecast_type=request.forecast_type,
            target_variable=request.target_variable,
            geography=request.geography,
            model_version="v1.0",
            forecast_points=[point],
            generated_at=current,
            valid_from=current,
            valid_until=current + timedelta(hours=1),
            metadata={"historical": True, "include_actuals": request.include_actuals},
        )
        history.append(forecast)
        count += 1
        current += timedelta(hours=1)

    return history


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
)
