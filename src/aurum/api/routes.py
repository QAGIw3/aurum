from __future__ import annotations

"""FastAPI application exposing curve endpoints."""

import base64
import hashlib
import json
import math
import uuid
from pathlib import Path as FilePath
from datetime import date, datetime
import logging
import time
import threading
import time as _time
import httpx
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dataclasses import replace
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from fastapi import APIRouter, Depends, HTTPException, Response, Request
try:
    from opentelemetry import trace
except ImportError:  # pragma: no cover - optional dependency
    trace = None  # type: ignore[assignment]
from ._fastapi_compat import Query, Path
from fastapi.responses import StreamingResponse

from .config import CacheConfig, TrinoConfig
from . import service
from .container import get_service
from .cache.cache import CacheManager, AsyncCache, CacheBackend
from .exceptions import (
    AurumAPIException,
    ValidationException,
    NotFoundException,
    ServiceUnavailableException,
    DataProcessingException,
)
from .models import (
    CurveDiffPoint,
    CurveDiffResponse,
    CurvePoint,
    CurveResponse,
    DimensionsData,
    DimensionsResponse,
    DimensionCount,
    DimensionsCountData,
    Meta,
    CreateScenarioRequest,
    ScenarioResponse,
    ScenarioData,
    ScenarioListResponse,
    ScenarioRunOptions,
    ScenarioRunResponse,
    ScenarioRunData,
    ScenarioRunListResponse,
    ScenarioOutputResponse,
    ScenarioOutputPoint,
    ScenarioMetricLatestResponse,
    ScenarioMetricLatest,
    PpaValuationRequest,
    PpaValuationResponse,
    PpaValuationListResponse,
    PpaValuationRecord,
    PpaContractCreate,
    PpaContractUpdate,
    PpaContractOut,
    PpaContractResponse,
    PpaContractListResponse,
    PpaMetric,
    CachePurgeDetail,
    CachePurgeResponse,
    IsoLocationOut,
    IsoLocationsResponse,
    IsoLocationResponse,
    IsoLmpAggregatePoint,
    IsoLmpAggregateResponse,
    IsoLmpPoint,
    IsoLmpResponse,
    UnitsCanonicalResponse,
    UnitsCanonical,
    UnitMappingOut,
    UnitsMappingResponse,
    SeriesCurveMappingCreate,
    SeriesCurveMappingUpdate,
    SeriesCurveMappingOut,
    SeriesCurveMappingListResponse,
    SeriesCurveMappingSearchResponse,
    CalendarsResponse,
    CalendarOut,
    CalendarBlocksResponse,
    CalendarHoursResponse,
    EiaDatasetsResponse,
    EiaDatasetBriefOut,
    EiaDatasetResponse,
    EiaDatasetDetailOut,
    EiaSeriesPoint,
    EiaSeriesResponse,
    EiaSeriesDimensionsData,
    EiaSeriesDimensionsResponse,
    DroughtIndexPoint,
    DroughtIndexResponse,
    DroughtUsdmPoint,
    DroughtUsdmResponse,
    DroughtVectorEventPoint,
    DroughtVectorResponse,
    DroughtDimensions,
    DroughtDimensionsResponse,
    DroughtInfoResponse,
    # External models
    ExternalProvider,
    ExternalSeries,
    ExternalObservation,
    ExternalProvidersResponse,
    ExternalSeriesResponse,
    ExternalObservationsResponse,
    ExternalMetadataResponse,
    ExternalSeriesQueryParams,
    ExternalObservationsQueryParams,
)
from .scenarios.scenario_models import (
    CreateScenarioRequest,
    ScenarioResponse,
    ScenarioData,
    ScenarioListResponse,
    ScenarioRunOptions,
    ScenarioRunResponse,
    ScenarioRunData,
    ScenarioRunListResponse,
    ScenarioOutputResponse,
    ScenarioOutputPoint,
    ScenarioMetricLatestResponse,
    ScenarioMetricLatest,
    ScenarioRunPriority,
    ScenarioRunStatus,
    ScenarioStatus,
)
from .http import (
    DEFAULT_CACHE_SECONDS,
    build_cache_control_header,
    compute_etag,
    csv_response as http_csv_response,
    prepare_csv_value,
    respond_with_etag as http_respond_with_etag,
)
from .http.clients import request as http_request
try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as _PromCounter, REGISTRY as _PROM_REG
except Exception:  # pragma: no cover - metrics optional
    _PromCounter = None  # type: ignore[assignment]
    _PROM_REG = None  # type: ignore[assignment]

if _PromCounter:
    def _safe_counter(name: str, doc: str):
        try:
            existing = getattr(_PROM_REG, "_names_to_collectors", {}).get(name)
            if existing is not None:
                return existing
        except Exception:
            pass
        return _PromCounter(name, doc)

    SCENARIO_LIST_REQUESTS = _safe_counter(
        "aurum_api_scenario_list_requests_total",
        "Scenario list requests",
    )
    SCENARIO_RUN_LIST_REQUESTS = _safe_counter(
        "aurum_api_scenario_run_list_requests_total",
        "Scenario run list requests",
    )
else:  # pragma: no cover - metrics optional
    SCENARIO_LIST_REQUESTS = None  # type: ignore[assignment]
    SCENARIO_RUN_LIST_REQUESTS = None  # type: ignore[assignment]
import aurum.observability.metrics as observability_metrics
from aurum.observability.metrics import (
    CONTENT_TYPE_LATEST,
    METRICS_MIDDLEWARE,
    METRICS_PATH,
    PROMETHEUS_AVAILABLE,
    REQUEST_COUNTER,
    REQUEST_LATENCY,
    TILE_CACHE_COUNTER,
    TILE_FETCH_LATENCY,
    generate_latest,
)
from .auth import AuthMiddleware, OIDCConfig
from .scenarios.scenario_service import STORE as ScenarioStore
from aurum.reference import iso_locations as ref_iso
from aurum.core import AurumSettings
from .state import get_settings
from aurum.reference import units as ref_units
from aurum.reference import calendars as ref_cal
from aurum.reference import eia_catalog as ref_eia
try:  # pragma: no cover - optional dependency
    from aurum.drought.catalog import load_catalog, DroughtCatalog
except ModuleNotFoundError:  # pragma: no cover - drought features optional
    load_catalog = None  # type: ignore[assignment]
    DroughtCatalog = None  # type: ignore[assignment]
from aurum.telemetry.context import (
    correlation_context,
    get_request_id,
    get_trace_span_ids,
    log_structured,
    reset_request_id,
    set_request_id,
)

# Import external API router (optional during tooling/doc generation)
try:  # pragma: no cover
    from .handlers.external import router as external_router
except Exception:  # pragma: no cover - optional
    external_router = None  # type: ignore

router = APIRouter()

def _settings() -> AurumSettings:
    return get_settings()

def _trino_config() -> TrinoConfig:
    return TrinoConfig.from_settings(_settings())

def _cache_config(*, ttl_override: int | None = None) -> CacheConfig:
    return CacheConfig.from_settings(_settings(), ttl_override=ttl_override)

def configure_routes(settings: AurumSettings) -> None:
    global ADMIN_GROUPS, _TILE_CACHE_CFG, _TILE_CACHE, _INMEM_TTL, _CACHE
    global METADATA_CACHE_TTL, SCENARIO_OUTPUT_CACHE_TTL, SCENARIO_METRIC_CACHE_TTL
    global CURVE_MAX_LIMIT, SCENARIO_OUTPUT_MAX_LIMIT, SCENARIO_METRIC_MAX_LIMIT, EIA_SERIES_MAX_LIMIT
    global CURVE_CACHE_TTL, CURVE_DIFF_CACHE_TTL, CURVE_STRIP_CACHE_TTL
    global EIA_SERIES_CACHE_TTL, EIA_SERIES_DIMENSIONS_CACHE_TTL
    global _METRICS_ENABLED, _METRICS_PATH
    ADMIN_GROUPS = _load_admin_groups(settings)
    _TILE_CACHE_CFG = CacheConfig.from_settings(settings)
    _TILE_CACHE = None
    _INMEM_TTL = settings.api.cache.in_memory_ttl
    _CACHE = _SimpleCache(default_ttl=_INMEM_TTL)
    METADATA_CACHE_TTL = settings.api.cache.metadata_ttl
    SCENARIO_OUTPUT_CACHE_TTL = settings.api.cache.scenario_output_ttl
    SCENARIO_METRIC_CACHE_TTL = settings.api.cache.scenario_metric_ttl
    CURVE_CACHE_TTL = settings.api.cache.curve_ttl
    CURVE_DIFF_CACHE_TTL = settings.api.cache.curve_diff_ttl
    CURVE_STRIP_CACHE_TTL = settings.api.cache.curve_strip_ttl
    EIA_SERIES_CACHE_TTL = settings.api.cache.eia_series_ttl
    EIA_SERIES_DIMENSIONS_CACHE_TTL = settings.api.cache.eia_series_dimensions_ttl
    CURVE_MAX_LIMIT = settings.pagination.curves_max_limit
    SCENARIO_OUTPUT_MAX_LIMIT = settings.pagination.scenario_output_max_limit
    SCENARIO_METRIC_MAX_LIMIT = settings.pagination.scenario_metric_max_limit
    EIA_SERIES_MAX_LIMIT = settings.pagination.eia_series_max_limit
    _METRICS_ENABLED = bool(
        settings.api.metrics.enabled and PROMETHEUS_AVAILABLE and METRICS_MIDDLEWARE is not None
    )
    path = settings.api.metrics.path
    _METRICS_PATH = path if path.startswith("/") else f"/{path}"

    if _METRICS_ENABLED:
        observability_metrics.METRICS_PATH = _METRICS_PATH

    _register_metrics_route()
_PPA_DECIMAL_QUANTIZER = Decimal("0.000001")


def _quantize_ppa_decimal(value: Any) -> Optional[Decimal]:
    """Normalize numeric inputs to a Decimal with 6 digits of scale."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        if value.is_nan():
            return None
        try:
            return value.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return None
    if isinstance(value, float) and math.isnan(value):
        return None
    try:
        as_decimal = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    try:
        return as_decimal.quantize(_PPA_DECIMAL_QUANTIZER, rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None


def _current_request_id() -> str:
    existing = get_request_id()
    if existing:
        return existing
    return str(uuid.uuid4())


def _load_admin_groups(settings: AurumSettings) -> Set[str]:
    groups = {item.strip().lower() for item in settings.auth.admin_groups}
    if settings.auth.disabled:
        return set()
    return groups


ADMIN_GROUPS: Set[str] = set()


def _get_principal(request: Request) -> Dict[str, Any] | None:
    principal = getattr(request.state, "principal", None)
    if isinstance(principal, dict):
        return principal
    return None


def _is_admin(principal: Dict[str, Any] | None) -> bool:
    if not ADMIN_GROUPS:
        return True
    if not principal:
        return False

    # Check admin groups (existing logic)
    groups = principal.get("groups") or []
    normalized = {str(group).lower() for group in groups if group}
    if any(group in normalized for group in ADMIN_GROUPS):
        return True

    # Check OIDC scopes for admin access
    claims = principal.get("claims") or {}
    scopes = claims.get("scope", "")
    if isinstance(scopes, str):
        scope_list = scopes.split()
    else:
        scope_list = scopes if isinstance(scopes, list) else []

    # Check for specific admin scopes
    admin_scopes = {
        "admin",
        "admin:read",
        "admin:write",
        "aurum:admin",
        "admin:feature_flags",
        "admin:rate_limits",
        "admin:trino"
    }
    return any(scope in scope_list for scope in admin_scopes)


def _require_admin(principal: Dict[str, Any] | None) -> None:
    if not _is_admin(principal):
        raise HTTPException(status_code=403, detail="admin_required")


def _parse_region_param(region: str) -> tuple[str, str]:
    region = region.strip()
    if not region:
        raise HTTPException(status_code=400, detail="invalid_region")
    if ":" not in region:
        raise HTTPException(status_code=400, detail="invalid_region" )
    region_type, region_id = region.split(":", 1)
    region_type = region_type.strip().upper()
    region_id = region_id.strip()
    if not region_type or not region_id:
        raise HTTPException(status_code=400, detail="invalid_region")
    return region_type, region_id


_CATALOG_PATH = FilePath(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"
_DROUGHT_CATALOG: DroughtCatalog | None = None
_TILE_CACHE_CFG: CacheConfig | None = None
_TILE_CACHE = None


def _drought_catalog() -> DroughtCatalog:
    if load_catalog is None or DroughtCatalog is None:  # pragma: no cover - optional
        raise RuntimeError("drought catalog features require optional dependencies")
    global _DROUGHT_CATALOG
    if _DROUGHT_CATALOG is None:
        _DROUGHT_CATALOG = load_catalog(_CATALOG_PATH)
    return _DROUGHT_CATALOG


def _tile_cache():
    global _TILE_CACHE
    if _TILE_CACHE is not None:
        return _TILE_CACHE
    cfg = _TILE_CACHE_CFG or _cache_config()
    try:
        client = service._maybe_redis_client(cfg)
    except Exception:
        client = None
    _TILE_CACHE = client
    return _TILE_CACHE



# Structured access logging
ACCESS_LOGGER = logging.getLogger("aurum.api.access")
LOGGER = logging.getLogger(__name__)


async def access_log_middleware(request, call_next):  # type: ignore[no-redef]
    import os

    # Extract or generate identifiers
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    correlation_id = request.headers.get("x-correlation-id") or request_id
    tenant_id = request.headers.get("x-aurum-tenant")
    user_id = request.headers.get("x-user-id")

    # Set up correlation context for the entire request
    with correlation_context(
        correlation_id=correlation_id,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=request_id
    ) as context:
        request.state.request_id = context["request_id"]
        request.state.correlation_id = context["correlation_id"]
        request.state.tenant_id = context["tenant_id"]
        request.state.user_id = context["user_id"]
        request.state.session_id = context["session_id"]

        query_params = dict(request.query_params.multi_items()) if request.query_params else {}

        # Set span attributes for tracing
        span = trace.get_current_span() if trace else None
        if span is not None and getattr(span, "is_recording", lambda: False)():  # pragma: no branch - inexpensive check
            span.set_attribute("aurum.request_id", context["request_id"])
            span.set_attribute("aurum.correlation_id", context["correlation_id"])
            span.set_attribute("aurum.tenant_id", context["tenant_id"])
            span.set_attribute("aurum.user_id", context["user_id"])
            span.set_attribute("http.request_id", context["request_id"])
            span.set_attribute("http.correlation_id", context["correlation_id"])

        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            principal = getattr(request.state, "principal", {}) or {}
            trace_id, span_id = get_trace_span_ids()
            if trace_id:
                request.state.trace_id = trace_id
            if span_id:
                request.state.span_id = span_id

            log_structured(
                "error",
                "http_request_failed",
                method=request.method,
                path=request.url.path,
                status=500,
                duration_ms=round(duration_ms, 2),
                client_ip=request.client.host if request.client else None,
                tenant=principal.get("tenant"),
                subject=principal.get("sub"),
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-api"),
                query_params=query_params or None,
                trace_id=trace_id,
                span_id=span_id,
            )
            raise

        duration_ms = (time.perf_counter() - start) * 1000.0
        principal = getattr(request.state, "principal", {}) or {}
        trace_id, span_id = get_trace_span_ids()
        if trace_id:
            request.state.trace_id = trace_id
        if span_id:
            request.state.span_id = span_id

        # Set response headers with correlation IDs
        try:
            response.headers["X-Request-Id"] = context["request_id"]
            response.headers["X-Correlation-Id"] = context["correlation_id"]
            if context["tenant_id"]:
                response.headers["X-Aurum-Tenant"] = context["tenant_id"]
            if trace_id:
                response.headers["X-Trace-Id"] = trace_id
            if span_id:
                response.headers["X-Span-Id"] = span_id
        except Exception:
            pass

        # Structured access log
        log_structured(
            "info",
            "http_request_completed",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
            client_ip=request.client.host if request.client else None,
            tenant=principal.get("tenant"),
            subject=principal.get("sub"),
            response_size=len(response.body) if hasattr(response, 'body') else 0,
            user_agent=request.headers.get("user-agent", "unknown"),
            service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-api"),
            query_params=query_params or None,
            trace_id=trace_id,
            span_id=span_id,
        )
    return response


# --- Simple in-memory cache for hot, mostly-static endpoints ---

class _SimpleCache:
    def __init__(self, default_ttl: int = 60) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self._default_ttl = default_ttl

    def get(self, key: str) -> Any | None:
        now = _time.time()
        with self._lock:
            item = self._store.get(key)
            if not item:
                return None
            exp, val = item
            if now >= exp:
                self._store.pop(key, None)
                return None
            return val

    def set(self, key: str, value: Any, *, ttl: int | None = None) -> None:
        ttl_eff = ttl if ttl is not None else self._default_ttl
        with self._lock:
            self._store[key] = (_time.time() + ttl_eff, value)

    def get_or_set(self, key: str, supplier, *, ttl: int | None = None) -> Any:
        cached = self.get(key)
        if cached is not None:
            return cached
        value = supplier()
        self.set(key, value, ttl=ttl)
        return value

    def purge_prefix(self, prefix: str) -> int:
        with self._lock:
            keys = [cache_key for cache_key in self._store if cache_key.startswith(prefix)]
            for cache_key in keys:
                self._store.pop(cache_key, None)
            return len(keys)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_INMEM_TTL = 60
_CACHE = _SimpleCache(default_ttl=_INMEM_TTL)


def _model_dump(value: Any) -> Dict[str, Any]:
    dump_method = getattr(value, "model_dump", None)
    if callable(dump_method):
        return dump_method()
    if isinstance(value, dict):
        return value
    try:
        return dict(value)
    except TypeError as exc:  # pragma: no cover - defensive
        raise TypeError(f"Unsupported payload type for ETag generation: {type(value)!r}") from exc


def _metadata_cache_get_or_set(key_suffix: str, supplier):
    cache_cfg = replace(_cache_config(), ttl_seconds=METADATA_CACHE_TTL)
    client = service._maybe_redis_client(cache_cfg)
    redis_key = None
    if client is not None:
        namespace = cache_cfg.namespace or "aurum"
        redis_key = f"{namespace}:metadata:{key_suffix}"
        cached = client.get(redis_key)
        if cached:
            try:
                return json.loads(cached)
            except json.JSONDecodeError:
                pass

    cached_local = _CACHE.get(key_suffix)
    if cached_local is not None:
        return cached_local

    try:
        value = supplier()
    except LookupError:
        raise

    _CACHE.set(key_suffix, value, ttl=METADATA_CACHE_TTL)
    if client is not None and redis_key is not None:
        try:  # pragma: no cover - best effort Redis population
            client.setex(redis_key, METADATA_CACHE_TTL, json.dumps(value, default=str))
            index_key = f"{namespace}:metadata:index"
            client.sadd(index_key, redis_key)
            client.expire(index_key, METADATA_CACHE_TTL)
        except Exception:
            pass
    return value


def _metadata_cache_purge(prefixes: Sequence[str]) -> int:
    removed = 0
    for prefix in prefixes:
        removed += _CACHE.purge_prefix(prefix)
    return removed


def _respond_with_etag(
    model,
    request: Request,
    response: Response,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
    cache_seconds: Optional[int] = None,
    cache_control: Optional[str] = None,
):
    ttl = cache_seconds if cache_seconds is not None else _cache_config().ttl_seconds
    return http_respond_with_etag(
        model,
        request,
        response,
        extra_headers=extra_headers,
        cache_seconds=ttl,
        cache_control=cache_control,
    )


def _csv_response(
    request_id: str,
    rows: Iterable[Mapping[str, Any]],
    fieldnames: Sequence[str],
    filename: str,
    *,
    cache_control: Optional[str] = None,
    next_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None,
) -> StreamingResponse:
    normalised_rows = (
        {field: prepare_csv_value(row.get(field)) for field in fieldnames}
        for row in rows
    )
    return http_csv_response(
        request_id,
        normalised_rows,
        fieldnames,
        filename,
        cache_control=cache_control,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
    )

# Prometheus metrics integration (shared with observability utilities)
_METRICS_ENABLED = bool(PROMETHEUS_AVAILABLE and METRICS_MIDDLEWARE)
_METRICS_PATH = METRICS_PATH


def metrics():
    if not _METRICS_ENABLED:
        raise HTTPException(status_code=503, detail="metrics_unavailable")
    data = generate_latest()
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


_METRICS_ROUTE_NAME = "metrics"


def _register_metrics_route() -> None:
    to_remove = [route for route in router.routes if getattr(route, "name", None) == _METRICS_ROUTE_NAME]
    for route in to_remove:
        try:
            router.routes.remove(route)
        except ValueError:  # pragma: no cover - defensive
            continue
    if not _METRICS_ENABLED:
        return
    router.add_api_route(_METRICS_PATH, metrics, name=_METRICS_ROUTE_NAME, methods=["GET"])


if _METRICS_ENABLED:
    _register_metrics_route()


def _record_tile_cache_metric(endpoint: str, result: str) -> None:
    if TILE_CACHE_COUNTER is None:  # pragma: no cover - metrics disabled
        return
    try:
        TILE_CACHE_COUNTER.labels(endpoint=endpoint, result=result).inc()
    except Exception:  # pragma: no cover - guard against label errors
        LOGGER.debug("Failed to record tile cache metric", exc_info=True)


def _observe_tile_fetch(endpoint: str, status: str, duration: float) -> None:
    if TILE_FETCH_LATENCY is None:  # pragma: no cover - metrics disabled
        return
    try:
        TILE_FETCH_LATENCY.labels(endpoint=endpoint, status=status).observe(duration)
    except Exception:  # pragma: no cover
        LOGGER.debug("Failed to record tile fetch latency", exc_info=True)


@router.get("/health")
@router.get("/healthz")
@router.get("/health/live")
def health() -> dict:
    """Liveness check returning the current service health."""
    return {"status": "ok"}


@router.get("/live")
@router.get("/livez")
def live() -> dict:
    """Alias for health checks used by legacy probes."""
    return {"status": "alive"}


@router.get("/ready")
@router.get("/readyz")
def ready() -> dict:
    """Deep readiness check with comprehensive dependency validation."""
    trino_cfg = _trino_config()
    cache_cfg = _cache_config()
    settings = _settings()

    checks = {
        "trino": _check_trino_deep_health(trino_cfg),
        "timescale": _check_timescale_ready(settings.database.timescale_dsn),
        "redis": _check_redis_ready(cache_cfg),
        "schema_registry": _check_schema_registry_ready(),
    }

    if all(checks.values()):
        return {"status": "ready", "checks": checks}
    raise HTTPException(status_code=503, detail={"status": "unavailable", "checks": checks})


def _check_trino_deep_health(cfg: TrinoConfig) -> dict:
    """Perform deep health check on Trino including query performance and schema validation."""
    try:
        connect = service._require_trino()
    except RuntimeError:
        return {"status": "unavailable", "error": "Trino client not available"}

    try:
        import time
        start_time = time.perf_counter()

        with connect(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            http_scheme=cfg.http_scheme,
        ) as conn:
            cur = conn.cursor()

            # Basic connectivity test
            cur.execute("SELECT 1")
            cur.fetchone()

            # Schema validation - check key tables exist
            schema_check_start = time.perf_counter()
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'iceberg' AND table_catalog = 'market'
                AND table_name IN ('curve_observation', 'lmp_observation', 'series_observation')
                LIMIT 3
            """)
            schema_tables = [row[0] for row in cur.fetchall()]
            schema_check_time = time.perf_counter() - schema_check_start

            # Performance test - simple aggregation
            perf_check_start = time.perf_counter()
            cur.execute("""
                SELECT COUNT(*) as total_count
                FROM iceberg.market.curve_observation
                TABLESAMPLE SYSTEM (1)
            """)
            sample_result = cur.fetchone()
            perf_check_time = time.perf_counter() - perf_check_start

            total_time = time.perf_counter() - start_time

            return {
                "status": "healthy",
                "connectivity": "ok",
                "schema_tables_found": len(schema_tables),
                "schema_check_time_ms": round(schema_check_time * 1000, 2),
                "performance_check_time_ms": round(perf_check_time * 1000, 2),
                "total_check_time_ms": round(total_time * 1000, 2),
                "sample_query_result": sample_result[0] if sample_result else 0,
            }

    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}


def _check_schema_registry_ready() -> dict:
    """Check Schema Registry connectivity and health."""
    try:
        # Try to import and check schema registry
        from aurum.api import kafka
        import requests

        # Get schema registry URL from settings
        settings = _settings()
        schema_registry_url = getattr(settings.kafka, 'schema_registry_url', None)

        if not schema_registry_url:
            return {"status": "disabled", "error": "Schema Registry URL not configured"}

        # Ping schema registry health endpoint
        try:
            response = requests.get(f"{schema_registry_url}/subjects", timeout=5)
            if response.status_code == 200:
                subjects = response.json()
                return {
                    "status": "healthy",
                    "subjects_count": len(subjects),
                    "response_time_ms": round(response.elapsed.total_seconds() * 1000, 2),
                }
            else:
                return {"status": "unavailable", "error": f"HTTP {response.status_code}"}
        except requests.RequestException as exc:
            return {"status": "unavailable", "error": f"Connection failed: {exc}"}

    except Exception as exc:
        return {"status": "unavailable", "error": str(exc)}

# Register auth middleware after health/metrics route definitions


# Frozen cursor field definitions - these are now defined in pagination.py
# Use FROZEN_CURSOR_SCHEMAS instead of these legacy field lists
# These constants are kept for backward compatibility but should be deprecated

METADATA_CACHE_TTL = 300
SCENARIO_OUTPUT_CACHE_TTL = 60
SCENARIO_METRIC_CACHE_TTL = 60

CURVE_MAX_LIMIT = 500
SCENARIO_OUTPUT_MAX_LIMIT = 500
SCENARIO_METRIC_MAX_LIMIT = 500
CURVE_CACHE_TTL = 120
CURVE_DIFF_CACHE_TTL = 120
CURVE_STRIP_CACHE_TTL = 120

EIA_SERIES_CURSOR_FIELDS = ["series_id", "period_start", "period"]
EIA_SERIES_MAX_LIMIT = 1000
EIA_SERIES_CACHE_TTL = 120
EIA_SERIES_DIMENSIONS_CACHE_TTL = 300

# Import the hardened cursor functions
from .http.pagination import (
    CursorPayload,
    SortDirection,
    DEFAULT_PAGE_SIZE,
    MAX_PAGE_SIZE,
    FROZEN_CURSOR_SCHEMAS,
    decode_cursor,
    encode_cursor,
    extract_cursor_values,
    validate_cursor_schema,
)

# Legacy functions for backward compatibility - these will be deprecated
def _encode_cursor(payload: dict) -> str:
    """Legacy cursor encoding function - deprecated."""
    return encode_cursor(payload)


def _decode_cursor(token: str) -> CursorPayload:
    """Legacy cursor decoding function - returns CursorPayload."""
    return decode_cursor(token)


def _resolve_tenant(request: Request, explicit: Optional[str]) -> str:
    # First priority: check request.state.tenant (set by auth middleware)
    if tenant := getattr(request.state, "tenant", None):
        return tenant

    # Second priority: check principal from auth middleware
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant

    # Third priority: check X-Aurum-Tenant header
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant

    # Last priority: explicit parameter
    if explicit:
        return explicit

    raise HTTPException(status_code=400, detail="tenant_id is required")


def _resolve_tenant_optional(request: Request, explicit: Optional[str]) -> Optional[str]:
    # First priority: check request.state.tenant (set by auth middleware)
    if tenant := getattr(request.state, "tenant", None):
        return tenant

    # Second priority: check principal from auth middleware
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant

    # Third priority: check X-Aurum-Tenant header
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant

    return explicit


def _extract_cursor_payload(row: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
    """Legacy function - use the hardened cursor system instead."""
    payload: Dict[str, Any] = {}
    for field in fields:
        value = row.get(field)
        if isinstance(value, date):
            payload[field] = value.isoformat()
        elif isinstance(value, datetime):
            payload[field] = value.isoformat()
        elif value is not None:
            payload[field] = value
        else:
            payload[field] = None
    return payload


def create_cursor_from_row(row: Dict[str, Any], schema_name: str, direction: SortDirection = SortDirection.ASC) -> CursorPayload:
    """Create a cursor payload from a database row using the hardened cursor system.

    Args:
        row: Database row data
        schema_name: Name of the cursor schema to use
        direction: Sort direction

    Returns:
        CursorPayload object
    """
    if schema_name not in FROZEN_CURSOR_SCHEMAS:
        raise ValueError(f"Unknown cursor schema: {schema_name}")

    schema = FROZEN_CURSOR_SCHEMAS[schema_name]
    values = {}

    for sort_key in schema.sort_keys:
        value = row.get(sort_key.name)
        if isinstance(value, date):
            values[sort_key.name] = value.isoformat()
        elif isinstance(value, datetime):
            values[sort_key.name] = value.isoformat()
        elif value is not None:
            values[sort_key.name] = value
        else:
            values[sort_key.name] = None

    return CursorPayload(
        schema_name=schema_name,
        values=values,
        direction=direction
    )


def extract_cursor_values_for_query(cursor: CursorPayload, schema_name: str) -> Dict[str, Any]:
    """Extract cursor values for database query construction.

    Args:
        cursor: CursorPayload object
        schema_name: Schema name to validate against

    Returns:
        Dictionary of values for query construction
    """
    return extract_cursor_values(cursor, schema_name)


def _normalise_cursor_input(payload: dict) -> tuple[Optional[int], Optional[dict]]:
    """Legacy function - use extract_cursor_values_for_query instead."""
    if "offset" in payload:
        try:
            return int(payload.get("offset", 0)), None
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid offset cursor") from exc
    return 0, payload


def build_stable_cursor_condition(
    cursor: CursorPayload,
    schema_name: str,
    comparison: str = ">",
    include_equals: bool = False
) -> tuple[str, Dict[str, Any]]:
    """Build a stable cursor condition for database queries that handles concurrent inserts.

    Args:
        cursor: CursorPayload object
        schema_name: Schema name to validate against
        comparison: Comparison operator ("<" or ">")
        include_equals: Whether to include equality condition for tie-breaking

    Returns:
        Tuple of (SQL condition, parameters dict)
    """
    validate_cursor_schema(cursor, schema_name)
    schema = FROZEN_CURSOR_SCHEMAS[schema_name]

    if not cursor.values:
        return "1=1", {}  # No cursor condition

    conditions = []
    params = {}

    for i, sort_key in enumerate(schema.sort_keys):
        value = cursor.values[sort_key.name]
        param_name = f"cursor_{sort_key.name}_{i}"

        if value is None:
            if comparison == ">":
                condition = f"{sort_key.name} IS NOT NULL"
            else:
                condition = f"{sort_key.name} IS NULL"
        else:
            operator = ">=" if include_equals else ">"
            condition = f"{sort_key.name} {operator} :{param_name}"
            params[param_name] = value

        conditions.append(condition)

    sql_condition = f"({') AND ('.join(conditions)})"
    return sql_condition, params


def ensure_cursor_stability(
    rows: List[Dict[str, Any]],
    schema_name: str,
    max_items: int
) -> List[Dict[str, Any]]:
    """Ensure cursor pagination stability by handling ties and duplicates.

    Args:
        rows: Database query results
        schema_name: Cursor schema name
        max_items: Maximum number of items to return

    Returns:
        List of stable results with proper tie handling
    """
    if not rows:
        return rows

    schema = FROZEN_CURSOR_SCHEMAS[schema_name]
    if not schema.sort_keys:
        return rows[:max_items]

    # Group by sort key values to handle ties
    grouped_rows = {}
    for row in rows:
        key_values = tuple(row.get(key.name) for key in schema.sort_keys)
        if key_values not in grouped_rows:
            grouped_rows[key_values] = []
        grouped_rows[key_values].append(row)

    # Sort groups by sort key values
    sorted_groups = sorted(grouped_rows.items())

    # Flatten and limit results
    stable_rows = []
    for _, group_rows in sorted_groups:
        stable_rows.extend(group_rows)
        if len(stable_rows) >= max_items:
            break

    return stable_rows[:max_items]


def _scenario_outputs_enabled() -> bool:
    return _settings().api.scenario_outputs_enabled


def _persist_ppa_valuation_records(
    *,
    ppa_contract_id: str,
    scenario_id: str,
    tenant_id: Optional[str],
    asof_date: date,
    metrics: List[Dict[str, Any]],
) -> None:
    if not _settings().api.ppa_write_enabled:
        return
    if not metrics:
        return
    try:
        import pandas as pd
    except ModuleNotFoundError:
        LOGGER.debug("Skipping PPA valuation persistence: pandas not available")
        return
    try:
        from aurum.parsers.iceberg_writer import write_ppa_valuation
    except ModuleNotFoundError:
        LOGGER.debug("Skipping PPA valuation persistence: pyiceberg not available")
        return

    records: List[Dict[str, Any]] = []
    now = datetime.utcnow().replace(microsecond=0)
    curve_hint = next((m.get("curve_key") for m in metrics if m.get("curve_key")), None)

    for metric in metrics:
        metric_name = str(metric.get("metric"))
        raw_value = metric.get("value")
        decimal_value = _quantize_ppa_decimal(raw_value)
        period_start = metric.get("period_start") or asof_date
        period_end = metric.get("period_end") or period_start
        curve_key = metric.get("curve_key") or curve_hint
        if curve_key:
            curve_hint = curve_key

        if isinstance(decimal_value, Decimal):
            value_for_hash = format(decimal_value, "f")
        elif raw_value is None:
            value_for_hash = ""
        else:
            value_for_hash = str(raw_value)

        record = {
            "asof_date": asof_date,
            "ppa_contract_id": ppa_contract_id,
            "scenario_id": scenario_id,
            "tenant_id": tenant_id,
            "curve_key": curve_key,
            "period_start": period_start,
            "period_end": period_end,
            "cashflow": None,
            "npv": None,
            "irr": None,
            "metric": metric_name,
            "value": decimal_value,
            "version_hash": hashlib.sha256(
                f"{ppa_contract_id}|{scenario_id}|{metric_name}|{period_start}|{period_end}|{value_for_hash}".encode("utf-8")
            ).hexdigest()[:16],
            "_ingest_ts": now,
        }

        metric_lower = metric_name.lower()
        if metric_lower == "cashflow":
            record["cashflow"] = decimal_value
        elif metric_lower == "npv":
            record["npv"] = decimal_value
        elif metric_lower == "irr":
            irr_value: Optional[float]
            try:
                irr_value = float(raw_value) if raw_value is not None else None
            except (TypeError, ValueError):
                irr_value = float(decimal_value) if isinstance(decimal_value, Decimal) else None
            record["irr"] = irr_value

        records.append(record)

    if not records:
        return

    try:
        frame = pd.DataFrame(records)
        write_ppa_valuation(frame)
    except Exception as exc:  # pragma: no cover - integration failure
        LOGGER.warning("Failed to persist PPA valuation for %s/%s: %s", scenario_id, ppa_contract_id, exc)

def _check_trino_ready(cfg: TrinoConfig) -> bool:
    try:
        connect = service._require_trino()
    except RuntimeError:
        return False
    try:
        with connect(  # type: ignore[arg-type]
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            http_scheme=cfg.http_scheme,
        ) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchone()
        return True
    except Exception:
        return False


def _check_timescale_ready(dsn: str) -> bool:
    if not dsn:
        return False
    try:  # pragma: no cover - import guard
        import psycopg  # type: ignore
    except ModuleNotFoundError:
        LOGGER.debug("Timescale readiness check skipped: psycopg not installed")
        return False

    try:
        with psycopg.connect(dsn, connect_timeout=3) as conn:  # type: ignore[attr-defined]
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return True
    except Exception:
        LOGGER.debug("Timescale readiness check failed", exc_info=True)
        return False


def _check_redis_ready(cache_cfg: CacheConfig) -> bool:
    try:
        client = service._maybe_redis_client(cache_cfg)
    except Exception:
        LOGGER.debug("Redis readiness check failed", exc_info=True)
        return False

    if client is None:
        redis_configured = bool(
            cache_cfg.redis_url
            or cache_cfg.sentinel_endpoints
            or cache_cfg.cluster_nodes
        )
        if str(cache_cfg.mode).lower() == "disabled":
            redis_configured = False
        return not redis_configured

    return True


@router.get("/v1/curves", response_model=CurveResponse)
def list_curves(
    request: Request,
    asof: Optional[date] = Query(None, description="As-of date filter (YYYY-MM-DD)"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    prev_cursor: Optional[str] = Query(
        None,
        description="Cursor pointing to the previous page; obtained from meta.prev_cursor",
    ),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    response: Response,
) -> CurveResponse:
    """List curve observations with cursor pagination and optional CSV.

    Supports forward (`cursor`/`since_cursor`) and backward (`prev_cursor`)
    iteration using opaque tokens produced in `meta`. When `format=csv`, the
    endpoint streams a CSV response with appropriate Cache-Control headers.
    """
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache_cfg = _cache_config()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=CURVE_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    effective_offset = offset
    cursor_after: Optional[dict] = None
    cursor_before: Optional[dict] = None
    descending = False

    # Validate cursor consistency with query parameters
    current_filters = {
        "asof": asof,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    prev_token = prev_cursor
    forward_token = cursor or since_cursor
    if prev_token:
        try:
            cursor_obj = Cursor.from_string(prev_token)
            # Check if cursor has expired
            if cursor_obj.is_expired():
                raise HTTPException(status_code=400, detail="Cursor has expired")

            # Check if cursor filters match current query
            if not cursor_obj.matches_filters(current_filters):
                raise HTTPException(
                    status_code=400,
                    detail="Cursor filters do not match current query parameters"
                )

            before_payload = cursor_obj.filters
            cursor_before = before_payload
            descending = True
            effective_offset = 0
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc
    elif forward_token:
        try:
            cursor_obj = Cursor.from_string(forward_token)
            # Check if cursor has expired
            if cursor_obj.is_expired():
                raise HTTPException(status_code=400, detail="Cursor has expired")

            # Check if cursor filters match current query
            if not cursor_obj.matches_filters(current_filters):
                raise HTTPException(
                    status_code=400,
                    detail="Cursor filters do not match current query parameters"
                )

            effective_offset = cursor_obj.offset
            cursor_after = cursor_obj.filters
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc

    # Validation and guardrails for dimension combinations
    dimension_count = sum([
        1 for dim in [curve_key, asset_class, iso, location, market, product, block, tenor_type]
        if dim is not None
    ])

    if dimension_count == 0 and limit > 500:
        raise HTTPException(
            status_code=400,
            detail="Queries without dimension filters with high limits may be too broad. Consider adding filters or reducing limit."
        )

    if dimension_count == 1 and limit > 1000:
        raise HTTPException(
            status_code=400,
            detail="Single-dimension queries with high limits may be too broad. Consider adding more filters or reducing limit."
        )

    # Check for potentially expensive dimension combinations
    expensive_combinations = [
        {"iso": None, "market": None, "product": None},  # Too broad
        {"asset_class": None, "location": None},  # Too broad
        {"curve_key": None},  # May be too broad without other filters
    ]

    for combo in expensive_combinations:
        if all(current_dims.get(k) is None for k in combo.keys()) and limit > 500:
            raise HTTPException(
                status_code=400,
                detail=f"Query with dimensions {list(combo.keys())} and limit > 500 may be too expensive. Consider adding more specific filters."
            )

    try:
        rows, elapsed_ms = service.query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=effective_limit + 1,
            offset=effective_offset,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    else:
        if more:
            rows = rows[:effective_limit]

    next_cursor = None
    if more and rows:
        next_payload = _extract_cursor_payload(rows[-1], CURVE_CURSOR_FIELDS)
        next_cursor_obj = Cursor(
            offset=effective_offset + len(rows),
            limit=effective_limit,
            timestamp=time.time(),
            filters=current_filters
        )
        next_cursor = next_cursor_obj.to_string()

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = _extract_cursor_payload(rows[0], CURVE_CURSOR_FIELDS)
                prev_cursor_obj = Cursor(
                    offset=effective_offset,
                    limit=effective_limit,
                    timestamp=time.time(),
                    filters={**current_filters, "before": prev_payload}
                )
                prev_cursor_value = prev_cursor_obj.to_string()
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = _extract_cursor_payload(rows[0], CURVE_CURSOR_FIELDS)
                prev_cursor_obj = Cursor(
                    offset=max(effective_offset - effective_limit, 0),
                    limit=effective_limit,
                    timestamp=time.time(),
                    filters={**current_filters, "before": prev_payload}
                )
                prev_cursor_value = prev_cursor_obj.to_string()

    if format.lower() == "csv":
        cache_seconds = CURVE_CACHE_TTL
        cache_header = build_cache_control_header(cache_seconds)
        fieldnames = [
            "curve_key",
            "tenor_label",
            "tenor_type",
            "contract_month",
            "asof_date",
            "mid",
            "bid",
            "ask",
            "price_type",
        ]
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows, "cursor": next_cursor})
        incoming = request.headers.get("if-none-match")
        not_modified_headers = {
            "ETag": etag,
            "X-Request-Id": request_id,
            "Cache-Control": cache_header,
        }
        if next_cursor:
            not_modified_headers["X-Next-Cursor"] = next_cursor
        if prev_cursor_value:
            not_modified_headers["X-Prev-Cursor"] = prev_cursor_value
        if incoming and incoming == etag:
            return Response(status_code=304, headers=not_modified_headers)

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="curves.csv",
            cache_seconds=cache_seconds,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    model = CurveResponse(
        meta=Meta(
            request_id=request_id,
            query_time_ms=int(elapsed_ms),
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        ),
        data=[CurvePoint(**row) for row in rows],
    )

    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=CURVE_CACHE_TTL,
    )


@router.get("/v1/curves/diff", response_model=CurveDiffResponse)
def list_curves_diff(
    request: Request,
    asof_a: date = Query(..., description="First as-of date"),
    asof_b: date = Query(..., description="Second as-of date"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    *,
    response: Response,
) -> CurveDiffResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache_cfg = _cache_config()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=CURVE_DIFF_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    cursor_after: Optional[dict] = None
    effective_cursor = cursor or since_cursor

    # Validate cursor consistency with query parameters
    current_filters = {
        "asof_a": asof_a,
        "asof_b": asof_b,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    if effective_cursor:
        try:
            cursor_obj = Cursor.from_string(effective_cursor)
            # Check if cursor has expired
            if cursor_obj.is_expired():
                raise HTTPException(status_code=400, detail="Cursor has expired")

            # Check if cursor filters match current query
            if not cursor_obj.matches_filters(current_filters):
                raise HTTPException(
                    status_code=400,
                    detail="Cursor filters do not match current query parameters"
                )

            offset = cursor_obj.offset
            cursor_after = cursor_obj.filters
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc

    # Validation and guardrails for date ranges and dimensions
    if asof_a == asof_b:
        raise HTTPException(
            status_code=400,
            detail="asof_a and asof_b must be different dates for comparison"
        )

    # Check date range - prevent queries spanning too many days
    days_diff = (asof_b - asof_a).days
    if abs(days_diff) > 365:  # Max 1 year difference
        raise HTTPException(
            status_code=400,
            detail=f"Date range too large: {abs(days_diff)} days. Maximum allowed: 365 days"
        )

    # Validate dimension combinations to prevent overly broad queries
    dimension_count = sum([
        1 for dim in [curve_key, asset_class, iso, location, market, product, block, tenor_type]
        if dim is not None
    ])

    if dimension_count == 0:
        raise HTTPException(
            status_code=400,
            detail="At least one dimension filter must be specified to avoid excessive result sets"
        )

    if dimension_count == 1 and limit > 1000:
        raise HTTPException(
            status_code=400,
            detail="Single-dimension queries with high limits may be too broad. Consider adding more filters or reducing limit."
        )

    # Check for potentially expensive dimension combinations
    expensive_combinations = [
        {"iso": None, "market": None, "product": None},  # Too broad
        {"asset_class": None, "location": None},  # Too broad
    ]

    current_dims = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    for combo in expensive_combinations:
        if all(current_dims.get(k) is None for k in combo.keys()) and limit > 500:
            raise HTTPException(
                status_code=400,
                detail=f"Query with dimensions {list(combo.keys())} and limit > 500 may be too expensive. Consider adding more specific filters."
            )

    try:
        rows, elapsed_ms = service.query_curves_diff(
            trino_cfg,
            cache_cfg,
            asof_a=asof_a,
            asof_b=asof_b,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=effective_limit + 1,
            offset=offset,
            cursor_after=cursor_after,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > effective_limit:
        rows = rows[:effective_limit]
        next_payload = _extract_cursor_payload(rows[-1], CURVE_DIFF_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    model = CurveDiffResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurveDiffPoint(**row) for row in rows],
    )

    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=CURVE_DIFF_CACHE_TTL,
    )


@router.get("/v1/metadata/dimensions", response_model=DimensionsResponse, tags=["Metadata"])
def list_dimensions(
    asof: Optional[date] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    per_dim_limit: int = Query(MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    prefix: Optional[str] = Query(None, description="Optional case-insensitive startswith filter applied to each dimension list"),
    include_counts: bool = Query(False, description="Include per-dimension value counts when true"),
    *,
    request: Request,
    response: Response,
) -> DimensionsResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache = _cache_config()
    cache_cfg = CacheConfig(redis_url=base_cache.redis_url, ttl_seconds=METADATA_CACHE_TTL)

    try:
        results, counts_raw = service.query_dimensions(
            trino_cfg,
            cache_cfg,
            asof=asof,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            per_dim_limit=per_dim_limit,
            include_counts=include_counts,
        )
    except RuntimeError as exc:  # pragma: no cover
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    counts_filtered = counts_raw
    if prefix:
        pfx = prefix.lower()
        for k, vals in list(results.items()):
            results[k] = [v for v in vals if isinstance(v, str) and v.lower().startswith(pfx)]
        if counts_filtered:
            counts_filtered = {
                key: [
                    item
                    for item in value
                    if isinstance(item.get("value"), str)
                    and item["value"].lower().startswith(pfx)
                ]
                for key, value in counts_filtered.items()
            }

    counts_model = None
    if counts_filtered:
        counts_payload: Dict[str, list[DimensionCount]] = {}
        for key, items in counts_filtered.items():
            typed_items = [
                DimensionCount(value=str(item["value"]), count=int(item["count"]))
                for item in items
                if item.get("value") is not None
            ]
            if typed_items:
                counts_payload[key] = typed_items
        if counts_payload:
            counts_model = DimensionsCountData(**counts_payload)

    model = DimensionsResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=DimensionsData(**results),
        counts=counts_model,
    )
    return _respond_with_etag(model, request, response)


@router.get(
    "/v1/metadata/locations",
    response_model=IsoLocationsResponse,
    tags=["Metadata"],
    summary="List ISO locations",
    description="Return known ISO locations with optional filters by ISO code and a case-insensitive prefix applied to id or name.",
)
def list_locations(
    iso: Optional[str] = Query(None, description="Filter by ISO code (e.g., PJM, CAISO)", examples={"default": {"value": "PJM"}}),
    prefix: Optional[str] = Query(None, description="Case-insensitive startswith on id or name", examples={"default": {"value": "AE"}}),
    *,
    request: Request,
    response: Response,
) -> IsoLocationsResponse:
    request_id = _current_request_id()
    def _base_locations() -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []
        for loc in ref_iso.iter_locations(iso):
            data.append(
                {
                    "iso": loc.iso,
                    "location_id": loc.location_id,
                    "location_name": loc.location_name,
                    "location_type": loc.location_type,
                    "zone": loc.zone,
                    "hub": loc.hub,
                    "timezone": loc.timezone,
                }
            )
        return data

    cache_key = f"iso:locations:{(iso or '').upper()}"
    base = _metadata_cache_get_or_set(cache_key, _base_locations)
    items: list[IsoLocationOut] = []
    if prefix:
        needle = prefix.lower()
        for rec in base:
            name = (rec.get("location_name") or "").lower()
            if rec["location_id"].lower().startswith(needle) or name.startswith(needle):
                items.append(IsoLocationOut(**rec))
    else:
        items = [IsoLocationOut(**rec) for rec in base]
    model = IsoLocationsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


@router.get(
    "/v1/metadata/locations/{iso}/{location_id}",
    response_model=IsoLocationResponse,
    tags=["Metadata"],
    summary="Get ISO location",
    description="Return metadata for a specific ISO location id (case-insensitive match).",
)
def get_location(
    iso: str = Path(..., description="ISO code", examples={"default": {"value": "PJM"}}),
    location_id: str = Path(..., description="Location identifier", examples={"default": {"value": "AECO"}}),
    *,
    request: Request,
    response: Response,
) -> IsoLocationResponse:
    request_id = _current_request_id()
    def _load_loc() -> dict[str, Any]:
        loc = ref_iso.get_location(iso, location_id)
        if loc is None:
            # propagate and handle below
            raise LookupError("not found")
        return {
            "iso": loc.iso,
            "location_id": loc.location_id,
            "location_name": loc.location_name,
            "location_type": loc.location_type,
            "zone": loc.zone,
            "hub": loc.hub,
            "timezone": loc.timezone,
        }

    try:
        rec = _metadata_cache_get_or_set(
            f"iso:loc:{iso.upper()}:{location_id.upper()}", _load_loc
        )
    except LookupError:
        raise HTTPException(status_code=404, detail="Location not found")
    item = IsoLocationOut(**rec)
    model = IsoLocationResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=item)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


# --- Units metadata ---


@router.get(
    "/v1/metadata/units",
    response_model=UnitsCanonicalResponse,
    tags=["Metadata"],
    summary="List canonical currencies and units",
    description="Return distinct currencies and units derived from the units mapping file. Optionally filter by case-insensitive prefix.",
)
def list_units(
    prefix: Optional[str] = Query(None, description="Optional startswith filter", examples={"sample": {"value": "US"}}),
    *,
    request: Request,
    response: Response,
) -> UnitsCanonicalResponse:
    request_id = _current_request_id()
    def _base():
        mapper = ref_units.UnitsMapper()
        mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
        curr = sorted({rec.currency for rec in mapping.values() if rec.currency})
        unit_list = sorted({rec.per_unit for rec in mapping.values() if rec.per_unit})
        return {"currencies": curr, "units": unit_list}

    base = _metadata_cache_get_or_set("units:canonical", _base)
    currencies = base["currencies"]
    units = base["units"]
    if prefix:
        pfx = prefix.lower()
        currencies = [c for c in currencies if c.lower().startswith(pfx)]
        units = [u for u in units if u.lower().startswith(pfx)]
    model = UnitsCanonicalResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=UnitsCanonical(currencies=currencies, units=units),
    )
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


@router.get(
    "/v1/metadata/units/mapping",
    response_model=UnitsMappingResponse,
    tags=["Metadata", "Units"],
    summary="List unit mappings",
    description="Return raw-to-canonical unit mappings. Optionally filter by case-insensitive prefix on the raw units string.",
)
def list_unit_mappings(
    prefix: Optional[str] = Query(None, description="Startswith filter on units_raw", examples={"sample": {"value": "USD/"}}),
    *,
    request: Request,
    response: Response,
) -> UnitsMappingResponse:
    request_id = _current_request_id()
    def _base_list():
        mapper = ref_units.UnitsMapper()
        mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
        tuples: list[tuple[str, str | None, str | None]] = []
        for _, rec in mapping.items():
            tuples.append((rec.raw, rec.currency or None, rec.per_unit or None))
        tuples.sort(key=lambda t: t[0])
        return tuples

    base = _metadata_cache_get_or_set("units:mapping", _base_list)
    items: list[UnitMappingOut] = []
    if prefix:
        pfx = prefix.lower()
        for raw, cur, per in base:
            if raw.lower().startswith(pfx):
                items.append(UnitMappingOut(units_raw=raw, currency=cur, per_unit=per))
    else:
        items = [UnitMappingOut(units_raw=raw, currency=cur, per_unit=per) for raw, cur, per in base]
    model = UnitsMappingResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


# --- Calendars metadata ---


@router.get(
    "/v1/metadata/calendars",
    response_model=CalendarsResponse,
    tags=["Metadata"],
    summary="List calendars",
    description="Return available trading calendars and their timezones.",
)
def list_calendars(request: Request, response: Response) -> CalendarsResponse:
    request_id = _current_request_id()
    def _load_calendars() -> list[dict[str, str]]:
        calendars = ref_cal.get_calendars()
        records = [
            {"name": name, "timezone": cfg.timezone}
            for name, cfg in calendars.items()
        ]
        records.sort(key=lambda item: item["name"])
        return records

    base = _metadata_cache_get_or_set("calendars:all", _load_calendars)
    items = [CalendarOut(**record) for record in base]
    model = CalendarsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


@router.get(
    "/v1/metadata/calendars/{name}/blocks",
    response_model=CalendarBlocksResponse,
    tags=["Metadata"],
    summary="List calendar blocks",
    description="Return block names for a given calendar.",
)
def list_calendar_blocks(
    name: str = Path(..., description="Calendar name", examples={"default": {"value": "us"}}),
    *,
    request: Request,
    response: Response,
) -> CalendarBlocksResponse:
    request_id = _current_request_id()
    def _load_blocks() -> list[str]:
        calendars = ref_cal.get_calendars()
        cfg = calendars.get(name.lower())
        if cfg is None:
            raise LookupError("calendar not found")
        return sorted(cfg.blocks.keys())

    try:
        blocks = _metadata_cache_get_or_set(f"calendars:{name.lower()}:blocks", _load_blocks)
    except LookupError:
        raise HTTPException(status_code=404, detail="Calendar not found")
    model = CalendarBlocksResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=blocks)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


@router.get(
    "/v1/metadata/calendars/{name}/hours",
    response_model=CalendarHoursResponse,
    tags=["Metadata"],
    summary="Block hours for a date",
    description="Return timezone-aware datetimes for the specified block on a given date.",
)
def calendar_hours(
    name: str = Path(..., description="Calendar name", examples={"default": {"value": "us"}}),
    block: str = Query(..., description="Block identifier", examples={"default": {"value": "ON_PEAK"}}),
    date: date = Query(..., description="Local calendar date (YYYY-MM-DD)", examples={"default": {"value": "2024-01-02"}}),
    *,
    request: Request,
    response: Response,
) -> CalendarHoursResponse:
    request_id = _current_request_id()
    cache_key = f"calendars:{name.lower()}:{block.upper()}:{date.isoformat()}"

    def _load_hours() -> list[str]:
        datetimes = ref_cal.hours_for_block(name, block, date)
        return [dt.isoformat() for dt in datetimes]

    iso_list = _metadata_cache_get_or_set(cache_key, _load_hours)
    model = CalendarHoursResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=iso_list)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


@router.get(
    "/v1/metadata/calendars/{name}/expand",
    response_model=CalendarHoursResponse,
    tags=["Metadata"],
    summary="Expand block over date range",
    description="Return timezone-aware datetimes for each date in the range where the block applies.",
)
def calendar_expand(
    name: str = Path(..., description="Calendar name", examples={"default": {"value": "us"}}),
    block: str = Query(..., description="Block identifier", examples={"default": {"value": "ON_PEAK"}}),
    start: date = Query(..., description="Start date (YYYY-MM-DD)", examples={"default": {"value": "2024-01-01"}}),
    end: date = Query(..., description="End date (YYYY-MM-DD)", examples={"default": {"value": "2024-01-02"}}),
    *,
    request: Request,
    response: Response,
) -> CalendarHoursResponse:
    request_id = _current_request_id()
    cache_key = (
        f"calendars:{name.lower()}:{block.upper()}:{start.isoformat()}:{end.isoformat()}"
    )

    def _load_range() -> list[str]:
        datetimes = ref_cal.expand_block_range(name, block, start, end)
        return [dt.isoformat() for dt in datetimes]

    iso_list = _metadata_cache_get_or_set(cache_key, _load_range)
    model = CalendarHoursResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=iso_list)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=METADATA_CACHE_TTL,
    )


# --- EIA Catalog metadata ---


@router.get(
    "/v1/iso/lmp/last-24h",
    response_model=IsoLmpResponse,
    tags=["ISO"],
    summary="Latest ISO LMP observations",
    description="Return the most recent 24 hours of ISO LMP records across all markets.",
)
def iso_lmp_last_24h(
    iso_code: Optional[str] = Query(None, description="Filter by ISO code"),
    market: Optional[str] = Query(None, description="Filter by market"),
    location_id: Optional[str] = Query(None, description="Filter by settlement location id"),
    limit: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    request: Request,
    response: Response,
) -> IsoLmpResponse:
    request_id = _current_request_id()
    rows, elapsed = service.query_iso_lmp_last_24h(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        limit=limit,
        cache_cfg=_cache_config(),
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "delivery_date",
            "interval_start",
            "interval_end",
            "interval_minutes",
            "location_id",
            "location_name",
            "location_type",
            "price_total",
            "price_energy",
            "price_congestion",
            "price_loss",
            "currency",
            "uom",
            "settlement_point",
            "source_run_id",
            "ingest_ts",
            "record_hash",
            "metadata",
        ]
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows})
        cache_seconds = _cache_config().ttl_seconds
        cache_header = build_cache_control_header(cache_seconds)
        incoming = request.headers.get("if-none-match")
        if incoming and incoming == etag:
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "X-Request-Id": request_id,
                    "Cache-Control": cache_header,
                },
            )

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="iso-lmp-last-24h.csv",
            cache_seconds=cache_seconds,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@router.get(
    "/v1/iso/lmp/hourly",
    response_model=IsoLmpAggregateResponse,
    tags=["ISO"],
    summary="Hourly ISO LMP aggregates",
    description="Return aggregated hourly ISO LMP values for the last 30 days.",
)
def iso_lmp_hourly(
    iso_code: Optional[str] = Query(None, description="Filter by ISO code"),
    market: Optional[str] = Query(None, description="Filter by market"),
    location_id: Optional[str] = Query(None, description="Filter by settlement location id"),
    start: Optional[datetime] = Query(None, description="Earliest interval_start (ISO 8601)", examples={"default": {"value": "2024-01-01T00:00:00Z"}}),
    end: Optional[datetime] = Query(None, description="Latest interval_start (ISO 8601)", examples={"default": {"value": "2024-01-31T23:59:59Z"}}),
    limit: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    request: Request,
    response: Response,
) -> IsoLmpAggregateResponse:
    request_id = _current_request_id()
    if start and end and start > end:
        raise HTTPException(status_code=400, detail="start_after_end")
    rows, elapsed = service.query_iso_lmp_hourly(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        start=start,
        end=end,
        limit=limit,
        cache_cfg=_cache_config(),
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "interval_start",
            "location_id",
            "currency",
            "uom",
            "price_avg",
            "price_min",
            "price_max",
            "price_stddev",
            "sample_count",
        ]
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows})
        cache_seconds = _cache_config().ttl_seconds
        cache_header = build_cache_control_header(cache_seconds)
        incoming = request.headers.get("if-none-match")
        if incoming and incoming == etag:
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "X-Request-Id": request_id,
                    "Cache-Control": cache_header,
                },
            )

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="iso-lmp-hourly.csv",
            cache_seconds=cache_seconds,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@router.get(
    "/v1/iso/lmp/daily",
    response_model=IsoLmpAggregateResponse,
    tags=["ISO"],
    summary="Daily ISO LMP aggregates",
    description="Return daily ISO LMP aggregates for the past year.",
)
def iso_lmp_daily(
    iso_code: Optional[str] = Query(None, description="Filter by ISO code"),
    market: Optional[str] = Query(None, description="Filter by market"),
    location_id: Optional[str] = Query(None, description="Filter by settlement location id"),
    start: Optional[datetime] = Query(None, description="Earliest interval_start (ISO 8601)", examples={"default": {"value": "2023-01-01T00:00:00Z"}}),
    end: Optional[datetime] = Query(None, description="Latest interval_start (ISO 8601)", examples={"default": {"value": "2023-12-31T23:59:59Z"}}),
    limit: int = Query(500, ge=1, le=MAX_PAGE_SIZE),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    request: Request,
    response: Response,
) -> IsoLmpAggregateResponse:
    request_id = _current_request_id()
    if start and end and start > end:
        raise HTTPException(status_code=400, detail="start_after_end")
    rows, elapsed = service.query_iso_lmp_daily(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        start=start,
        end=end,
        limit=limit,
        cache_cfg=_cache_config(),
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "interval_start",
            "location_id",
            "currency",
            "uom",
            "price_avg",
            "price_min",
            "price_max",
            "price_stddev",
            "sample_count",
        ]
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows})
        cache_seconds = _cache_config().ttl_seconds
        cache_header = build_cache_control_header(cache_seconds)
        incoming = request.headers.get("if-none-match")
        if incoming and incoming == etag:
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "X-Request-Id": request_id,
                    "Cache-Control": cache_header,
                },
            )

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="iso-lmp-daily.csv",
            cache_seconds=cache_seconds,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@router.get(
    "/v1/iso/lmp/negative",
    response_model=IsoLmpResponse,
    tags=["ISO"],
    summary="Recent negative ISO LMP events",
    description="Return the most negative ISO LMP observations across the last seven days.",
)
def iso_lmp_negative(
    iso_code: Optional[str] = Query(None, description="Filter by ISO code"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    request: Request,
    response: Response,
) -> IsoLmpResponse:
    request_id = _current_request_id()
    rows, elapsed = service.query_iso_lmp_negative(
        iso_code=iso_code,
        market=market,
        limit=limit,
        cache_cfg=_cache_config(),
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "delivery_date",
            "interval_start",
            "interval_end",
            "interval_minutes",
            "location_id",
            "location_name",
            "location_type",
            "price_total",
            "price_energy",
            "price_congestion",
            "price_loss",
            "currency",
            "uom",
            "settlement_point",
            "source_run_id",
            "ingest_ts",
            "record_hash",
            "metadata",
        ]
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows})
        cache_seconds = _cache_config().ttl_seconds
        cache_header = build_cache_control_header(cache_seconds)
        incoming = request.headers.get("if-none-match")
        if incoming and incoming == etag:
            return Response(
                status_code=304,
                headers={
                    "ETag": etag,
                    "X-Request-Id": request_id,
                    "Cache-Control": cache_header,
                },
            )

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="iso-lmp-negative.csv",
            cache_seconds=cache_seconds,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)



@router.get(
    "/v1/metadata/eia/datasets",
    response_model=EiaDatasetsResponse,
    tags=["Metadata", "EIA"],
    summary="List EIA datasets",
    description="Return EIA datasets from the harvested catalog with optional case-insensitive prefix filter on path or name.",
)
def list_eia_datasets(
    prefix: Optional[str] = Query(None, description="Startswith filter on path or name", examples={"default": {"value": "natural-gas/"}}),
    *,
    request: Request,
    response: Response,
) -> EiaDatasetsResponse:
    request_id = _current_request_id()
    def _base_eia():
        lst: list[dict[str, Any]] = []
        for ds in ref_eia.iter_datasets():
            lst.append(
                {
                    "path": ds.path,
                    "name": ds.name,
                    "description": ds.description,
                    "default_frequency": ds.default_frequency,
                    "start_period": ds.start_period,
                    "end_period": ds.end_period,
                }
            )
        lst.sort(key=lambda d: d["path"])  # type: ignore[index]
        return lst

    base = _CACHE.get_or_set("eia:datasets", _base_eia)
    if prefix:
        pfx = prefix.lower()
        filtered = [d for d in base if d.get("path", "").lower().startswith(pfx) or (d.get("name") or "").lower().startswith(pfx)]
    else:
        filtered = base
    items = [EiaDatasetBriefOut(**d) for d in filtered]
    model = EiaDatasetsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=_INMEM_TTL,
    )


@router.get(
    "/v1/metadata/eia/datasets/{dataset_path:path}",
    response_model=EiaDatasetResponse,
    tags=["Metadata", "EIA"],
    summary="Get EIA dataset",
    description="Return dataset metadata including facets, frequencies, and data columns for the specified dataset path.",
)
def get_eia_dataset(
    dataset_path: str = Path(..., description="Dataset path (e.g., natural-gas/stor/wkly)", examples={"default": {"value": "natural-gas/stor/wkly"}}),
    *,
    request: Request,
    response: Response,
) -> EiaDatasetResponse:
    request_id = _current_request_id()
    def _detail() -> dict[str, Any]:
        ds = ref_eia.get_dataset(dataset_path)
        return {
            "path": ds.path,
            "name": ds.name,
            "description": ds.description,
            "frequencies": list(ds.frequencies),
            "facets": list(ds.facets),
            "data_columns": list(ds.data_columns),
            "start_period": ds.start_period,
            "end_period": ds.end_period,
            "default_frequency": ds.default_frequency,
            "default_date_format": ds.default_date_format,
        }
    try:
        data = _CACHE.get_or_set(f"eia:detail:{dataset_path}", _detail)
    except ref_eia.DatasetNotFoundError as exc:  # type: ignore[attr-defined]
        raise HTTPException(status_code=404, detail="Dataset not found") from exc
    item = EiaDatasetDetailOut(**data)
    model = EiaDatasetResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=item)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=_INMEM_TTL,
    )


# --- EIA Series data ---


@router.get(
    "/v1/ref/eia/series",
    response_model=EiaSeriesResponse,
    tags=["Metadata", "EIA"],
    summary="List EIA series observations",
    description="Return EIA observations from the canonical Timescale table with optional filters and cursor-based pagination.",
)
def list_eia_series(
    request: Request,
    series_id: Optional[str] = Query(None, description="Filter by exact series identifier"),
    frequency: Optional[str] = Query(None, description="Frequency label (ANNUAL, MONTHLY, etc.)"),
    area: Optional[str] = Query(None, description="Area/region filter"),
    sector: Optional[str] = Query(None, description="Sector/category filter"),
    dataset: Optional[str] = Query(None, description="EIA dataset identifier"),
    unit: Optional[str] = Query(None, description="Unit filter"),
    canonical_unit: Optional[str] = Query(None, description="Normalized unit filter"),
    canonical_currency: Optional[str] = Query(None, description="Canonical currency filter"),
    source: Optional[str] = Query(None, description="Source attribution filter"),
    start: Optional[datetime] = Query(None, description="Filter observations with period_start >= this timestamp (ISO 8601)"),
    end: Optional[datetime] = Query(None, description="Filter observations with period_start <= this timestamp (ISO 8601)"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' when possible)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for forward pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume from a previous next_cursor value"),
    prev_cursor: Optional[str] = Query(None, description="Cursor obtained from meta.prev_cursor to navigate backwards"),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    response: Response,
) -> EiaSeriesResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache_cfg = _cache_config()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=EIA_SERIES_CACHE_TTL)

    effective_limit = min(limit, EIA_SERIES_MAX_LIMIT)
    effective_offset = offset
    cursor_after: Optional[dict] = None
    cursor_before: Optional[dict] = None
    descending = False

    prev_token = prev_cursor
    forward_token = cursor or since_cursor
    if prev_token:
        payload = _decode_cursor(prev_token)
        before_payload = payload.get("before") if isinstance(payload, dict) else None
        if before_payload:
            cursor_before = before_payload
            descending = True
            effective_offset = 0
        elif isinstance(payload, dict) and set(payload.keys()) <= {"offset"}:
            try:
                effective_offset = max(int(payload.get("offset", 0)), 0)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="Invalid cursor") from exc
        else:
            cursor_before = payload if isinstance(payload, dict) else None
            descending = True
            effective_offset = 0
    elif forward_token:
        payload = _decode_cursor(forward_token)
        effective_offset, cursor_after = _normalise_cursor_input(payload)

    try:
        rows, elapsed_ms = service.query_eia_series(
            trino_cfg,
            cache_cfg,
            series_id=series_id,
            frequency=frequency.upper() if frequency else None,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
            start=start,
            end=end,
            limit=effective_limit + 1,
            offset=effective_offset,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    else:
        if more:
            rows = rows[:effective_limit]

    next_cursor = None
    if more and rows:
        next_payload = _extract_cursor_payload(rows[-1], EIA_SERIES_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = _extract_cursor_payload(rows[0], EIA_SERIES_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = _extract_cursor_payload(rows[0], EIA_SERIES_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})

    if not rows and forward_token and prev_token is None and effective_offset > 0:
        prev_cursor_value = _encode_cursor({"offset": max(effective_offset - effective_limit, 0)})

    if format.lower() == "csv":
        cache_header = f"public, max-age={EIA_SERIES_CACHE_TTL}"
        fieldnames = [
            "series_id",
            "period",
            "period_start",
            "period_end",
            "frequency",
            "value",
            "raw_value",
            "unit",
            "canonical_unit",
            "canonical_currency",
            "canonical_value",
            "conversion_factor",
            "area",
            "sector",
            "seasonal_adjustment",
            "description",
            "source",
            "dataset",
            "metadata",
            "ingest_ts",
        ]
        return _csv_response(
            request_id,
            rows,
            fieldnames,
            filename="eia_series.csv",
            cache_control=cache_header,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        )

    def _coerce_dt(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _coerce_meta(value: Any) -> Optional[dict[str, str]]:
        if value is None:
            return None
        if isinstance(value, dict):
            return {str(k): str(v) for k, v in value.items()}
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return None
            if isinstance(parsed, dict):
                return {str(k): str(v) for k, v in parsed.items()}
        return None

    items: List[EiaSeriesPoint] = []
    for row in rows:
        series_value = row.get("series_id")
        if series_value is None:
            continue
        period_start = _coerce_dt(row.get("period_start"))
        period_end = _coerce_dt(row.get("period_end"))
        ingest_ts = _coerce_dt(row.get("ingest_ts"))
        if period_start is None:
            continue
        value = row.get("value")
        if isinstance(value, Decimal):
            value = float(value)
        elif value is not None:
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = None
        raw_value = row.get("raw_value")
        if isinstance(raw_value, Decimal):
            raw_value = format(raw_value, "f")
        elif raw_value is not None:
            raw_value = str(raw_value)

        canonical_value = row.get("canonical_value")
        if isinstance(canonical_value, Decimal):
            canonical_value = float(canonical_value)
        elif canonical_value is not None:
            try:
                canonical_value = float(canonical_value)
            except (TypeError, ValueError):
                canonical_value = None

        conversion_factor = row.get("conversion_factor")
        if conversion_factor is not None:
            try:
                conversion_factor = float(conversion_factor)
            except (TypeError, ValueError):
                conversion_factor = None

        item = EiaSeriesPoint(
            series_id=str(series_value),
            period=str(row.get("period")),
            period_start=period_start,
            period_end=period_end,
            frequency=row.get("frequency"),
            value=value,
            raw_value=raw_value,
            unit=row.get("unit"),
            canonical_unit=row.get("canonical_unit"),
            canonical_currency=row.get("canonical_currency"),
            canonical_value=canonical_value,
            conversion_factor=conversion_factor,
            area=row.get("area"),
            sector=row.get("sector"),
            seasonal_adjustment=row.get("seasonal_adjustment"),
            description=row.get("description"),
            source=row.get("source"),
            dataset=row.get("dataset"),
            metadata=_coerce_meta(row.get("metadata")),
            ingest_ts=ingest_ts,
        )
        items.append(item)

    meta = Meta(
        request_id=request_id,
        query_time_ms=int(round(elapsed_ms, 2)),
        next_cursor=next_cursor,
        prev_cursor=prev_cursor_value,
    )
    model = EiaSeriesResponse(meta=meta, data=items)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=EIA_SERIES_CACHE_TTL,
    )


@router.get(
    "/v1/ref/eia/series/dimensions",
    response_model=EiaSeriesDimensionsResponse,
    tags=["Metadata", "EIA"],
    summary="List EIA series dimensions",
    description="Return distinct dimension values (dataset, area, sector, unit, canonical_unit, canonical_currency, frequency, source) for EIA series observations.",
)
def list_eia_series_dimensions(
    request: Request,
    series_id: Optional[str] = Query(None, description="Optional exact series identifier filter"),
    frequency: Optional[str] = Query(None, description="Optional frequency filter"),
    area: Optional[str] = Query(None, description="Area filter"),
    sector: Optional[str] = Query(None, description="Sector filter"),
    dataset: Optional[str] = Query(None, description="Dataset filter"),
    unit: Optional[str] = Query(None, description="Unit filter"),
    canonical_unit: Optional[str] = Query(None, description="Normalized unit filter"),
    canonical_currency: Optional[str] = Query(None, description="Canonical currency filter"),
    source: Optional[str] = Query(None, description="Source filter"),
    *,
    response: Response,
) -> EiaSeriesDimensionsResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache_cfg = _cache_config()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=EIA_SERIES_DIMENSIONS_CACHE_TTL)

    try:
        values, elapsed_ms = service.query_eia_series_dimensions(
            trino_cfg,
            cache_cfg,
            series_id=series_id,
            frequency=frequency.upper() if frequency else None,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    data = EiaSeriesDimensionsData(
        dataset=values.get("dataset"),
        area=values.get("area"),
        sector=values.get("sector"),
        unit=values.get("unit"),
        frequency=values.get("frequency"),
        source=values.get("source"),
    )
    meta = Meta(request_id=request_id, query_time_ms=int(round(elapsed_ms, 2)))
    model = EiaSeriesDimensionsResponse(meta=meta, data=data)
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=EIA_SERIES_DIMENSIONS_CACHE_TTL,
    )


@router.get("/v1/curves/strips", response_model=CurveResponse)
def list_strips(
    request: Request,
    asof: Optional[date] = Query(None),
    type: str = Query(..., pattern="^(CALENDAR|SEASON|QUARTER)$", description="Strip type"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    *,
    response: Response,
) -> CurveResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    base_cache_cfg = _cache_config()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=CURVE_STRIP_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    effective_offset = offset
    effective_cursor = cursor or since_cursor
    if effective_cursor:
        payload = _decode_cursor(effective_cursor)
        effective_offset = int(payload.get("offset", 0))

    try:
        rows, elapsed_ms = service.query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=type,
            limit=effective_limit + 1,
            offset=effective_offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > effective_limit:
        rows = rows[:effective_limit]
        next_cursor = _encode_cursor({"offset": effective_offset + effective_limit})

    model = CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )

    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=CURVE_STRIP_CACHE_TTL,
    )


__all__ = ["router", "configure_routes", "access_log_middleware", "METRICS_MIDDLEWARE"]


# --- Scenario endpoints (stubbed service behavior for now) ---


def _to_datetime(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _assumption_to_dict(assumption: Any) -> Dict[str, Any]:
    if hasattr(assumption, "dict"):
        return assumption.model_dump()
    if isinstance(assumption, dict):
        return dict(assumption)
    return {}


def _scenario_record_to_data(record) -> ScenarioData:
    status_value = getattr(record, "status", ScenarioStatus.CREATED)
    if isinstance(status_value, str):
        try:
            status_value = ScenarioStatus(status_value.lower())
        except ValueError:
            status_value = ScenarioStatus.CREATED

    assumptions = [_assumption_to_dict(item) for item in getattr(record, "assumptions", [])]
    created_at = _to_datetime(getattr(record, "created_at", None)) or datetime.utcnow()
    updated_at = _to_datetime(getattr(record, "updated_at", None))

    return ScenarioData(
        id=getattr(record, "id", None),
        tenant_id=getattr(record, "tenant_id", ""),
        name=getattr(record, "name", ""),
        description=getattr(record, "description", None),
        status=status_value,
        unique_key=getattr(record, "unique_key", ""),
        assumptions=assumptions,
        parameters=dict(getattr(record, "parameters", {}) or {}),
        tags=list(getattr(record, "tags", []) or []),
        created_at=created_at,
        updated_at=updated_at,
        created_by=getattr(record, "created_by", None),
        version=getattr(record, "version", 1),
        metadata=dict(getattr(record, "metadata", {}) or {}),
    )


def _scenario_run_to_data(run) -> ScenarioRunData:
    status_value = getattr(run, "state", ScenarioRunStatus.QUEUED)
    if isinstance(status_value, str):
        try:
            status_value = ScenarioRunStatus(status_value.lower())
        except ValueError:
            status_value = ScenarioRunStatus.QUEUED

    priority_value = getattr(run, "priority", ScenarioRunPriority.NORMAL)
    if isinstance(priority_value, str):
        try:
            priority_value = ScenarioRunPriority(priority_value.lower())
        except ValueError:
            priority_value = ScenarioRunPriority.NORMAL

    created_at = _to_datetime(getattr(run, "created_at", None)) or datetime.utcnow()
    started_at = _to_datetime(getattr(run, "started_at", None))
    completed_at = _to_datetime(getattr(run, "completed_at", None))
    queued_at = _to_datetime(getattr(run, "queued_at", None)) or created_at
    cancelled_at = _to_datetime(getattr(run, "cancelled_at", None))

    return ScenarioRunData(
        id=getattr(run, "run_id", getattr(run, "id", "")),
        scenario_id=getattr(run, "scenario_id", ""),
        status=status_value,
        priority=priority_value,
        run_key=getattr(run, "run_key", None),
        input_hash=getattr(run, "input_hash", None),
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=getattr(run, "duration_seconds", None),
        error_message=getattr(run, "error_message", None),
        retry_count=getattr(run, "retry_count", 0),
        max_retries=getattr(run, "max_retries", 3),
        progress_percent=getattr(run, "progress_percent", None),
        parameters=dict(getattr(run, "parameters", {}) or {}),
        environment=dict(getattr(run, "environment", {}) or {}),
        created_at=created_at,
        queued_at=queued_at,
        cancelled_at=cancelled_at,
    )


def _ppa_contract_to_model(record) -> PpaContractOut:
    if record is None:
        raise ValueError("PPA contract record is required")
    terms = record.terms if isinstance(record.terms, dict) else {}
    return PpaContractOut(
        ppa_contract_id=record.id,
        tenant_id=record.tenant_id,
        instrument_id=record.instrument_id,
        terms=terms,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@router.get("/v1/scenarios", response_model=ScenarioListResponse)
def list_scenarios(
    request: Request,
    response: Response,
    status: Optional[str] = Query(None, pattern="^[A-Z_]+$", description="Optional scenario status filter"),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    name: Optional[str] = Query(None, description="Filter by scenario name (case-insensitive substring match)"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    created_after: Optional[datetime] = Query(None, description="Return scenarios created at or after this timestamp (ISO 8601)"),
    created_before: Optional[datetime] = Query(None, description="Return scenarios created at or before this timestamp (ISO 8601)"),
) -> ScenarioListResponse:
    """List scenarios for the authenticated tenant with offset/cursor support.

    Accepts optional filters (status, name, tag, created_{after,before}). When
    the number of results equals `limit`, a `meta.next_cursor` is returned that
    can be supplied via `cursor` on subsequent requests.
    """
    if SCENARIO_LIST_REQUESTS:
        try:
            SCENARIO_LIST_REQUESTS.inc()
        except Exception:
            pass
    tenant_id = _resolve_tenant_optional(request, None)
    request_id = _current_request_id()
    start = time.perf_counter()
    effective_offset = offset or 0
    cursor_token = cursor or since_cursor
    if cursor_token:
        payload = _decode_cursor(cursor_token)
        effective_offset, _cursor_after = _normalise_cursor_input(payload)

    if created_after and created_before and created_after > created_before:
        raise HTTPException(status_code=400, detail="created_after must be before created_before")

    records = ScenarioStore.list_scenarios(
        tenant_id=tenant_id,
        status=status,
        limit=limit,
        offset=effective_offset,
        name_contains=name,
        tag=tag,
        created_after=created_after,
        created_before=created_before,
    )
    duration_ms = int((time.perf_counter() - start) * 1000.0)
    next_cursor = None
    if len(records) == limit:
        next_cursor = _encode_cursor({"offset": effective_offset + limit})
    data = [_scenario_record_to_data(record) for record in records]
    model = ScenarioListResponse(
        meta=Meta(request_id=request_id, query_time_ms=duration_ms, next_cursor=next_cursor),
        data=data,
    )
    return _respond_with_etag(model, request, response)


@router.post("/v1/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario(payload: CreateScenarioRequest, request: Request) -> ScenarioResponse:
    """Create a new scenario for the resolved tenant and return its definition."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, payload.tenant_id)

    # Use idempotency context if key is provided
    idempotency_key = getattr(request.state, 'idempotency_key', None)

    if idempotency_key:
        from .idempotency import get_idempotency_manager

        # Create a context manager for idempotent operation
        async with get_idempotency_manager().idempotent_operation(
            idempotency_key,
            "create_scenario",
            payload.model_dump()
        ) as cached_result:
            if cached_result:
                # Return cached result
                return ScenarioResponse(
                    meta=Meta(request_id=request_id, query_time_ms=0),
                    data=cached_result['scenario_data']
                )

            # Perform the operation
            record = ScenarioStore.create_scenario(
                tenant_id=tenant_id,
                name=payload.name,
                description=payload.description,
                assumptions=payload.assumptions,
            )

            scenario_data = _scenario_record_to_data(record)

            # Cache the result
            await get_idempotency_manager().set_result(
                idempotency_key,
                "create_scenario",
                payload.model_dump(),
                {"scenario_data": scenario_data}
            )

            return ScenarioResponse(
                meta=Meta(request_id=request_id, query_time_ms=0),
                data=scenario_data
            )
    else:
        # Original non-idempotent behavior
        record = ScenarioStore.create_scenario(
            tenant_id=tenant_id,
            name=payload.name,
            description=payload.description,
            assumptions=payload.assumptions,
        )
        return ScenarioResponse(
            meta=Meta(request_id=request_id, query_time_ms=0),
            data=_scenario_record_to_data(record),
        )


@router.get("/v1/scenarios/{scenario_id}", response_model=ScenarioResponse)
def get_scenario(scenario_id: str, request: Request, response: Response) -> ScenarioResponse:
    """Fetch a scenario by id with ETag support for conditional responses."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    record = ScenarioStore.get_scenario(scenario_id, tenant_id=tenant_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    model = ScenarioResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_record_to_data(record),
    )
    return _respond_with_etag(model, request, response)


@router.delete("/v1/scenarios/{scenario_id}", status_code=204)
def delete_scenario(scenario_id: str, request: Request) -> Response:
    """Delete a scenario by id. Returns 204 when deletion succeeds."""
    tenant_id = _resolve_tenant(request, None)
    deleted = ScenarioStore.delete_scenario(scenario_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return Response(status_code=204)


@router.get("/v1/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
def list_scenario_runs(
    scenario_id: str,
    request: Request,
    response: Response,
    state: Optional[str] = Query(None, pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)$"),
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    created_after: Optional[datetime] = Query(None, description="Return runs queued at or after this timestamp (ISO 8601)"),
    created_before: Optional[datetime] = Query(None, description="Return runs queued at or before this timestamp (ISO 8601)"),
) -> ScenarioRunListResponse:
    """List runs for a scenario with cursor/offset pagination and time filters."""
    if SCENARIO_RUN_LIST_REQUESTS:
        try:
            SCENARIO_RUN_LIST_REQUESTS.inc()
        except Exception:
            pass
    tenant_id = _resolve_tenant_optional(request, None)
    request_id = _current_request_id()
    start = time.perf_counter()
    effective_offset = offset or 0
    cursor_token = cursor or since_cursor
    if cursor_token:
        payload = _decode_cursor(cursor_token)
        effective_offset, _cursor_after = _normalise_cursor_input(payload)

    if created_after and created_before and created_after > created_before:
        raise HTTPException(status_code=400, detail="created_after must be before created_before")

    runs = ScenarioStore.list_runs(
        tenant_id=tenant_id,
        scenario_id=scenario_id,
        state=state,
        limit=limit,
        offset=effective_offset,
        created_after=created_after,
        created_before=created_before,
    )
    duration_ms = int((time.perf_counter() - start) * 1000.0)
    next_cursor = None
    if len(runs) == limit:
        next_cursor = _encode_cursor({"offset": effective_offset + limit})
    data = [_scenario_run_to_data(run) for run in runs]
    model = ScenarioRunListResponse(
        meta=Meta(request_id=request_id, query_time_ms=duration_ms, next_cursor=next_cursor),
        data=data,
    )
    return _respond_with_etag(model, request, response)


@router.post("/v1/scenarios/{scenario_id}/run", response_model=ScenarioRunResponse, status_code=202)
def run_scenario(
    scenario_id: str,
    request: Request,
    options: ScenarioRunOptions | None = None,
) -> ScenarioRunResponse:
    """Create or reuse a scenario run (idempotent by input fingerprint)."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    record = ScenarioStore.get_scenario(scenario_id, tenant_id=tenant_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    effective_tenant = tenant_id or record.tenant_id
    run = ScenarioStore.create_run(
        scenario_id=scenario_id,
        tenant_id=effective_tenant,
        code_version=options.code_version if options else None,
        seed=options.seed if options else None,
        parameters=options.parameters if options else None,
        environment=options.environment if options else None,
        priority=options.priority if options else ScenarioRunPriority.NORMAL,
        max_retries=getattr(options, "max_retries", 3) if options else 3,
        idempotency_key=getattr(options, "idempotency_key", None) if options else None,
    )
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@router.get("/v1/scenarios/{scenario_id}/runs/{run_id}", response_model=ScenarioRunResponse)
def get_scenario_run(scenario_id: str, run_id: str, request: Request, response: Response) -> ScenarioRunResponse:
    """Fetch a specific run for a scenario; responds with ETag headers."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.get_run_for_scenario(scenario_id, run_id, tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    model = ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )
    return _respond_with_etag(model, request, response)


@router.post("/v1/scenarios/runs/{run_id}/state", response_model=ScenarioRunResponse)
def update_scenario_run_state(
    run_id: str,
    request: Request,
    state: str = Query(..., pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)$"),
) -> ScenarioRunResponse:
    """Update the state of an existing run (e.g., RUNNING, SUCCEEDED, FAILED)."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.update_run_state(run_id, state=state, tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@router.post("/v1/scenarios/runs/{run_id}/cancel", response_model=ScenarioRunResponse)
def cancel_scenario_run(run_id: str, request: Request) -> ScenarioRunResponse:
    """Cancel a run by id and return its updated state."""
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.update_run_state(run_id, state="CANCELLED", tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@router.post(
    "/v1/admin/cache/scenario/{scenario_id}/invalidate",
    status_code=204,
    tags=["Admin"],
    summary="Invalidate cached scenario outputs",
)
def invalidate_scenario_cache(  # type: ignore[no-redef]
    scenario_id: str,
    request: Request,
    tenant_id: Optional[str] = Query(
        None,
        description="Optional tenant override; defaults to the calling principal's tenant",
    ),
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> Response:
    _require_admin(principal)
    effective_tenant = tenant_id or (principal or {}).get("tenant")
    service.invalidate_scenario_outputs_cache(_cache_config(), effective_tenant, scenario_id)
    return Response(status_code=204)


@router.post(
    "/v1/admin/cache/curves/invalidate",
    response_model=CachePurgeResponse,
    tags=["Admin"],
    include_in_schema=False,
    summary="Invalidate curve caches",
)
def invalidate_curve_cache_admin(
    request: Request,
    response: Response,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> CachePurgeResponse:
    _require_admin(principal)
    request_id = _current_request_id()
    cache_cfg = _cache_config()
    client = service._maybe_redis_client(cache_cfg)

    removed_counts: Dict[str, int] = {"curves": 0, "curves-diff": 0}
    prefixes = (("curves:", "curves"), ("curves-diff:", "curves-diff"))

    if client is not None:
        for prefix, scope in prefixes:
            pattern = f"{prefix}*"
            keys: list[Any] = []
            iterator = getattr(client, "scan_iter", None)
            try:
                if callable(iterator):  # pragma: no branch - runtime check
                    keys = list(iterator(pattern))  # type: ignore[arg-type]
                elif hasattr(client, "keys"):
                    keys = list(client.keys(pattern))  # type: ignore[attr-defined]
            except Exception:
                LOGGER.debug("Curve cache scan failed for prefix %s", prefix, exc_info=True)
                keys = []

            if keys:
                try:
                    deleted = client.delete(*keys)
                    removed_counts[scope] = int(deleted or 0)
                except Exception:
                    LOGGER.debug("Curve cache delete failed for prefix %s", prefix, exc_info=True)

    details = [
        CachePurgeDetail(scope="curves", redis_keys_removed=removed_counts["curves"], local_entries_removed=0),
        CachePurgeDetail(scope="curves-diff", redis_keys_removed=removed_counts["curves-diff"], local_entries_removed=0),
    ]
    model = CachePurgeResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=details)
    return _respond_with_etag(model, request, response)


@router.post(
    "/v1/admin/cache/eia/series/invalidate",
    response_model=CachePurgeResponse,
    tags=["Admin"],
    include_in_schema=False,
    summary="Invalidate cached EIA series queries",
)
def invalidate_eia_series_cache_admin(
    request: Request,
    response: Response,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> CachePurgeResponse:
    _require_admin(principal)
    request_id = _current_request_id()
    cache_cfg = _cache_config()
    results = service.invalidate_eia_series_cache(cache_cfg)
    data = [
        CachePurgeDetail(
            scope="eia-series",
            redis_keys_removed=results.get("eia-series", 0),
            local_entries_removed=0,
        ),
        CachePurgeDetail(
            scope="eia-series-dimensions",
            redis_keys_removed=results.get("eia-series-dimensions", 0),
            local_entries_removed=0,
        ),
    ]
    model = CachePurgeResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=data)
    return _respond_with_etag(model, request, response)


@router.post(
    "/v1/admin/cache/metadata/{scope}/invalidate",
    response_model=CachePurgeResponse,
    tags=["Admin"],
    include_in_schema=False,
    summary="Invalidate metadata caches",
)
def invalidate_metadata_cache_admin(
    scope: str,
    request: Request,
    response: Response,
    iso: Optional[str] = Query(
        None,
        description="Optional ISO code when purging location metadata",
    ),
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> CachePurgeResponse:
    _require_admin(principal)
    request_id = _current_request_id()
    scope_normalized = scope.strip().lower()
    if scope_normalized != "locations" and iso is not None:
        raise HTTPException(status_code=400, detail="iso_not_applicable")

    base_cache_cfg = _cache_config()
    meta_cache_cfg = replace(base_cache_cfg, ttl_seconds=METADATA_CACHE_TTL)

    prefixes: list[str]
    local_removed = 0
    redis_removed = 0

    if scope_normalized == "locations":
        if iso:
            iso_upper = iso.strip().upper()
            if not iso_upper:
                raise HTTPException(status_code=400, detail="invalid_iso")
            prefixes = [f"iso:locations:{iso_upper}", f"iso:loc:{iso_upper}:"]
        else:
            prefixes = ["iso:locations:", "iso:loc:"]
        local_removed = _metadata_cache_purge(prefixes)
        redis_counts = service.invalidate_metadata_cache(meta_cache_cfg, prefixes)
        redis_removed = sum(redis_counts.values())
    elif scope_normalized == "units":
        prefixes = ["units:canonical", "units:mapping"]
        local_removed = _metadata_cache_purge(prefixes)
        redis_counts = service.invalidate_metadata_cache(meta_cache_cfg, prefixes)
        redis_removed = sum(redis_counts.values())
    elif scope_normalized == "dimensions":
        redis_removed = service.invalidate_dimensions_cache(meta_cache_cfg)
    else:
        raise HTTPException(status_code=404, detail="unknown_metadata_scope")

    data = [
        CachePurgeDetail(
            scope=f"metadata:{scope_normalized}",
            redis_keys_removed=redis_removed,
            local_entries_removed=local_removed,
        )
    ]

    model = CachePurgeResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=data)
    return _respond_with_etag(model, request, response)


# === Series-Curve Mapping Admin Endpoints ===

@router.post(
    "/v1/admin/mappings",
    response_model=SeriesCurveMappingOut,
    tags=["Admin", "Mappings"],
    summary="Create a new series-curve mapping",
    description="Create a new mapping between an external series and an internal curve key. Requires admin privileges.",
)
async def create_series_curve_mapping(
    mapping: SeriesCurveMappingCreate,
    request: Request,
    response: Response,
    principal: Dict[str, Any] = Depends(_get_principal),
) -> SeriesCurveMappingOut:
    """Create a new series-curve mapping with audit logging."""
    _require_admin(principal)
    request_id = _current_request_id()

    await log_structured(
        "info",
        "series_curve_mapping_create_attempt",
        request_id=request_id,
        user_id=principal.get("sub"),
        external_provider=mapping.external_provider,
        external_series_id=mapping.external_series_id,
        curve_key=mapping.curve_key
    )

    # Placeholder implementation - will be implemented with database layer
    raise NotImplementedError("Series-curve mapping creation not yet implemented")


@router.get(
    "/v1/admin/mappings",
    response_model=SeriesCurveMappingListResponse,
    tags=["Admin", "Mappings"],
    summary="List series-curve mappings",
    description="List series-curve mappings with optional filtering.",
)
async def list_series_curve_mappings(
    request: Request,
    response: Response,
    external_provider: Optional[str] = Query(None, description="Filter by external provider"),
    active_only: bool = Query(True, description="Show only active mappings"),
    limit: int = Query(100, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for pagination"),
    principal: Dict[str, Any] = Depends(_get_principal),
) -> SeriesCurveMappingListResponse:
    """List series-curve mappings."""
    _require_admin(principal)
    request_id = _current_request_id()

    await log_structured(
        "info",
        "series_curve_mapping_list_access",
        request_id=request_id,
        user_id=principal.get("sub"),
        external_provider=external_provider,
        active_only=active_only
    )

    # Placeholder implementation - will be implemented with database layer
    raise NotImplementedError("Series-curve mapping listing not yet implemented")


@router.put(
    "/v1/admin/mappings/{provider}/{series_id}",
    response_model=SeriesCurveMappingOut,
    tags=["Admin", "Mappings"],
    summary="Update a series-curve mapping",
    description="Update an existing series-curve mapping.",
)
async def update_series_curve_mapping(
    provider: str,
    series_id: str,
    mapping_update: SeriesCurveMappingUpdate,
    request: Request,
    response: Response,
    principal: Dict[str, Any] = Depends(_get_principal),
) -> SeriesCurveMappingOut:
    """Update an existing series-curve mapping."""
    _require_admin(principal)
    request_id = _current_request_id()

    await log_structured(
        "info",
        "series_curve_mapping_update_attempt",
        request_id=request_id,
        user_id=principal.get("sub"),
        external_provider=provider,
        external_series_id=series_id
    )

    # Placeholder implementation - will be implemented with database layer
    raise NotImplementedError("Series-curve mapping update not yet implemented")


@router.delete(
    "/v1/admin/mappings/{provider}/{series_id}",
    response_model=SeriesCurveMappingOut,
    tags=["Admin", "Mappings"],
    summary="Deactivate a series-curve mapping",
    description="Deactivate an existing series-curve mapping.",
)
async def deactivate_series_curve_mapping(
    provider: str,
    series_id: str,
    request: Request,
    response: Response,
    principal: Dict[str, Any] = Depends(_get_principal),
) -> SeriesCurveMappingOut:
    """Deactivate a series-curve mapping."""
    _require_admin(principal)
    request_id = _current_request_id()

    await log_structured(
        "info",
        "series_curve_mapping_deactivate_attempt",
        request_id=request_id,
        user_id=principal.get("sub"),
        external_provider=provider,
        external_series_id=series_id
    )

    # Placeholder implementation - will be implemented with database layer
    raise NotImplementedError("Series-curve mapping deactivation not yet implemented")


@router.get(
    "/v1/admin/mappings/search",
    response_model=SeriesCurveMappingSearchResponse,
    tags=["Admin", "Mappings"],
    summary="Find potential curve mappings",
    description="Find potential curve mappings for unmapped external series.",
)
async def find_potential_mappings(
    request: Request,
    response: Response,
    external_provider: str = Query(..., description="External provider name"),
    external_series_id: str = Query(..., description="External series identifier"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of suggestions"),
    principal: Dict[str, Any] = Depends(_get_principal),
) -> SeriesCurveMappingSearchResponse:
    """Find potential curve mappings for unmapped series."""
    _require_admin(principal)
    request_id = _current_request_id()

    await log_structured(
        "info",
        "series_curve_mapping_search",
        request_id=request_id,
        user_id=principal.get("sub"),
        external_provider=external_provider,
        external_series_id=external_series_id
    )

    # Placeholder implementation - will be implemented with database layer
    raise NotImplementedError("Potential mapping search not yet implemented")


@router.get("/v1/ppa/contracts", response_model=PpaContractListResponse)
def list_ppa_contracts(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
) -> PpaContractListResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    start = time.perf_counter()
    effective_offset = offset
    token = cursor or since_cursor
    if token:
        payload = _decode_cursor(token)
        try:
            effective_offset = max(int(payload.get("offset", 0)), 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    effective_limit = limit
    records = ScenarioStore.list_ppa_contracts(
        tenant_id,
        limit=effective_limit + 1,
        offset=effective_offset,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    more = len(records) > effective_limit
    if more:
        records = records[:effective_limit]

    next_cursor = None
    if more:
        next_cursor = _encode_cursor({"offset": effective_offset + effective_limit})

    prev_cursor_value = None
    if effective_offset > 0:
        prev_offset = max(effective_offset - effective_limit, 0)
        prev_cursor_value = _encode_cursor({"offset": prev_offset})

    data = [_ppa_contract_to_model(record) for record in records]
    meta = Meta(
        request_id=request_id,
        query_time_ms=int(elapsed_ms),
        next_cursor=next_cursor,
        prev_cursor=prev_cursor_value,
    )
    model = PpaContractListResponse(meta=meta, data=data)
    return _respond_with_etag(model, request, response)


@router.post("/v1/ppa/contracts", response_model=PpaContractResponse, status_code=201)
def create_ppa_contract(
    payload: PpaContractCreate,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> PpaContractResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, None)
    record = ScenarioStore.create_ppa_contract(
        tenant_id=tenant_id,
        instrument_id=payload.instrument_id,
        terms=payload.terms,
    )
    return PpaContractResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_ppa_contract_to_model(record),
    )


@router.get("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
def get_ppa_contract(contract_id: str, request: Request, response: Response) -> PpaContractResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    record = ScenarioStore.get_ppa_contract(contract_id, tenant_id=tenant_id)
    if record is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    model = PpaContractResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_ppa_contract_to_model(record),
    )
    return _respond_with_etag(model, request, response)


@router.patch("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
def update_ppa_contract(
    contract_id: str,
    payload: PpaContractUpdate,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> PpaContractResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, None)
    record = ScenarioStore.update_ppa_contract(
        contract_id,
        tenant_id=tenant_id,
        instrument_id=payload.instrument_id,
        terms=payload.terms,
    )
    if record is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    return PpaContractResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_ppa_contract_to_model(record),
    )


@router.delete("/v1/ppa/contracts/{contract_id}", status_code=204)
def delete_ppa_contract(
    contract_id: str,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> Response:
    tenant_id = _resolve_tenant(request, None)
    deleted = ScenarioStore.delete_ppa_contract(contract_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    return Response(status_code=204)


@router.get("/v1/ppa/contracts/{contract_id}/valuations", response_model=PpaValuationListResponse)
def list_ppa_contract_valuations(
    contract_id: str,
    request: Request,
    response: Response,
    scenario_id: Optional[str] = Query(None, description="Filter by scenario identifier"),
    metric: Optional[str] = Query(None, description="Filter by valuation metric name"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    prev_cursor: Optional[str] = Query(None),
) -> PpaValuationListResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    contract = ScenarioStore.get_ppa_contract(contract_id, tenant_id=tenant_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")

    effective_offset = offset
    token = prev_cursor or cursor or since_cursor
    if token:
        payload = _decode_cursor(token)
        try:
            effective_offset = max(int(payload.get("offset", 0)), 0)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    effective_limit = limit
    trino_cfg = _trino_config()
    try:
        rows, elapsed_ms = service.query_ppa_contract_valuations(
            trino_cfg,
            ppa_contract_id=contract_id,
            scenario_id=scenario_id,
            metric=metric,
            limit=effective_limit + 1,
            offset=effective_offset,
            tenant_id=tenant_id,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if more:
        rows = rows[:effective_limit]

    data: list[PpaValuationRecord] = []
    for row in rows:
        data.append(
            PpaValuationRecord(
                asof_date=row.get("asof_date"),
                scenario_id=row.get("scenario_id"),
                period_start=row.get("period_start"),
                period_end=row.get("period_end"),
                metric=row.get("metric"),
                value=row.get("value"),
                cashflow=row.get("cashflow"),
                npv=row.get("npv"),
                irr=row.get("irr"),
                curve_key=row.get("curve_key"),
                version_hash=row.get("version_hash"),
                ingested_at=row.get("_ingest_ts"),
            )
        )

    next_cursor = None
    if more:
        next_cursor = _encode_cursor({"offset": effective_offset + effective_limit})

    prev_cursor_value = None
    if effective_offset > 0:
        prev_offset = max(effective_offset - effective_limit, 0)
        prev_cursor_value = _encode_cursor({"offset": prev_offset})

    meta = Meta(
        request_id=request_id,
        query_time_ms=int(elapsed_ms),
        next_cursor=next_cursor,
        prev_cursor=prev_cursor_value,
    )
    model = PpaValuationListResponse(meta=meta, data=data)
    return _respond_with_etag(model, request, response)


@router.post("/v1/ppa/valuate", response_model=PpaValuationResponse)
def valuate_ppa(payload: PpaValuationRequest, request: Request) -> PpaValuationResponse:
    request_id = _current_request_id()
    if not payload.scenario_id:
        raise HTTPException(status_code=400, detail="scenario_id is required for valuation")
    tenant_id = _resolve_tenant(request, None)
    contract = ScenarioStore.get_ppa_contract(payload.ppa_contract_id, tenant_id=tenant_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    trino_cfg = _trino_config()

    try:
        rows, elapsed_ms = service.query_ppa_valuation(
            trino_cfg,
            scenario_id=payload.scenario_id,
            tenant_id=tenant_id,
            asof_date=payload.asof_date,
            options=payload.options or None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    metrics: list[PpaMetric] = []
    fallback_date = payload.asof_date or date.today()
    for row in rows:
        period_start = row.get("period_start") or fallback_date
        period_end = row.get("period_end") or period_start
        metrics.append(
            PpaMetric(
                period_start=period_start,
                period_end=period_end,
                metric=str(row.get("metric") or "mid"),
                value=float(row.get("value") or 0.0),
                currency=row.get("currency"),
                unit=row.get("unit"),
                run_id=row.get("run_id"),
                curve_key=row.get("curve_key"),
                tenor_type=row.get("tenor_type"),
            )
        )

    try:
        _persist_ppa_valuation_records(
            ppa_contract_id=payload.ppa_contract_id,
            scenario_id=payload.scenario_id,
            tenant_id=tenant_id,
            asof_date=fallback_date,
            metrics=[metric.model_dump() if hasattr(metric, "model_dump") else metric for metric in metrics],
        )
    except Exception as exc:  # pragma: no cover - persistence should not break API
        LOGGER.warning("Failed to persist PPA valuation: %s", exc)

    return PpaValuationResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms)),
        data=metrics,
    )


@router.get("/v1/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputResponse)
def list_scenario_outputs(
    scenario_id: str,
    request: Request,
    response: Response,
    tenant_id_param: Optional[str] = Query(
        None,
        alias="tenant_id",
        description="Tenant identifier override; defaults to the authenticated tenant",
    ),
    metric: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None),
    curve_key: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(
        None,
        description="Alias for 'cursor' to resume iteration from a previously returned next_cursor",
    ),
    prev_cursor: Optional[str] = Query(
        None,
        description="Cursor pointing to the previous page; obtained from meta.prev_cursor",
    ),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
) -> ScenarioOutputResponse:
    """List scenario outputs with hardened cursor pagination and CSV option.

    Uses `cursor`/`since_cursor` for forward iteration and `prev_cursor` to
    fetch the previous page. When `format=csv`, streams a CSV with `next_cursor`
    and `prev_cursor` included in headers.
    """
    if not _scenario_outputs_enabled():
        raise HTTPException(status_code=503, detail="Scenario outputs not enabled")

    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, tenant_id_param)
    trino_cfg = _trino_config()
    base_cache = _cache_config()
    cache_cfg = replace(base_cache, ttl_seconds=SCENARIO_OUTPUT_CACHE_TTL)

    effective_limit = min(limit, SCENARIO_OUTPUT_MAX_LIMIT)
    effective_offset = 0
    cursor_after: Optional[dict] = None
    cursor_before: Optional[dict] = None
    descending = False

    prev_token = prev_cursor
    forward_token = cursor or since_cursor
    if prev_token:
        payload = _decode_cursor(prev_token)
        before_payload = payload.get("before") if isinstance(payload, dict) else None
        if before_payload:
            cursor_before = before_payload
            descending = True
        elif isinstance(payload, dict) and set(payload.keys()) <= {"offset"}:
            try:
                effective_offset = max(int(payload.get("offset", 0)), 0)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="Invalid cursor") from exc
        else:
            cursor_before = payload if isinstance(payload, dict) else None
            descending = True
    elif forward_token:
        decoded = _decode_cursor(forward_token)
        effective_offset, cursor_after = _normalise_cursor_input(decoded)

    try:
        rows, elapsed_ms = service.query_scenario_outputs(
            trino_cfg,
            cache_cfg,
            tenant_id=tenant_id,
            scenario_id=scenario_id,
            curve_key=curve_key,
            tenor_type=tenor_type,
            metric=metric,
            limit=effective_limit + 1,
            offset=effective_offset,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    else:
        if more:
            rows = rows[:effective_limit]

    next_cursor = None
    if more and rows:
        next_payload = _extract_cursor_payload(rows[-1], SCENARIO_OUTPUT_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = _extract_cursor_payload(rows[0], SCENARIO_OUTPUT_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = _extract_cursor_payload(rows[0], SCENARIO_OUTPUT_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})

    if format.lower() == "csv":
        cache_header = f"public, max-age={SCENARIO_OUTPUT_CACHE_TTL}"
        fieldnames = [
            "tenant_id",
            "scenario_id",
            "run_id",
            "asof_date",
            "curve_key",
            "tenor_type",
            "contract_month",
            "tenor_label",
            "metric",
            "value",
            "band_lower",
            "band_upper",
            "attribution",
            "version_hash",
        ]
        return _csv_response(
            request_id,
            rows,
            fieldnames,
            filename=f"scenario-{scenario_id}-outputs.csv",
            cache_control=cache_header,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        )

    meta = Meta(
        request_id=request_id,
        query_time_ms=int(elapsed_ms),
        next_cursor=next_cursor,
        prev_cursor=prev_cursor_value,
    )
    model = ScenarioOutputResponse(
        meta=meta,
        data=[ScenarioOutputPoint(**row) for row in rows],
    )
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=SCENARIO_OUTPUT_CACHE_TTL,
    )


@router.get("/v1/scenarios/{scenario_id}/metrics/latest", response_model=ScenarioMetricLatestResponse)
def list_scenario_metrics_latest(
    scenario_id: str,
    request: Request,
    response: Response,
    tenant_id_param: Optional[str] = Query(
        None,
        alias="tenant_id",
        description="Tenant identifier override; defaults to the authenticated tenant",
    ),
    metric: Optional[str] = Query(None, description="Filter by metric name (e.g., mid)"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(
        None,
        description="Alias for 'cursor' to resume iteration from a previously returned next_cursor",
    ),
    prev_cursor: Optional[str] = Query(
        None,
        description="Cursor pointing to the previous page; obtained from meta.prev_cursor",
    ),
) -> ScenarioMetricLatestResponse:
    """Return the latest metrics for a scenario with stable pagination.

    Designed to fetch most recent metric points per curve/metric, with
    `cursor` tokens enabling resumable iteration.
    """
    if not _scenario_outputs_enabled():
        raise HTTPException(status_code=503, detail="Scenario outputs not enabled")

    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, tenant_id_param)
    trino_cfg = _trino_config()
    base_cache = _cache_config()
    cache_cfg = replace(base_cache, ttl_seconds=SCENARIO_METRIC_CACHE_TTL)

    effective_limit = min(limit, SCENARIO_METRIC_MAX_LIMIT)
    effective_offset = 0
    cursor_after: Optional[dict] = None
    cursor_before: Optional[dict] = None
    descending = False

    prev_token = prev_cursor
    forward_token = cursor or since_cursor
    if prev_token:
        payload = _decode_cursor(prev_token)
        before_payload = payload.get("before") if isinstance(payload, dict) else None
        if before_payload:
            cursor_before = before_payload
            descending = True
        elif isinstance(payload, dict) and set(payload.keys()) <= {"offset"}:
            try:
                effective_offset = max(int(payload.get("offset", 0)), 0)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=400, detail="Invalid cursor") from exc
        else:
            cursor_before = payload if isinstance(payload, dict) else None
            descending = True
    elif forward_token:
        decoded = _decode_cursor(forward_token)
        effective_offset, cursor_after = _normalise_cursor_input(decoded)

    try:
        rows, elapsed_ms = service.query_scenario_metrics_latest(
            trino_cfg,
            cache_cfg,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            metric=metric,
            limit=effective_limit + 1,
            offset=effective_offset,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    else:
        if more:
            rows = rows[:effective_limit]

    next_cursor_value = None
    if more and rows:
        next_payload = _extract_cursor_payload(rows[-1], SCENARIO_METRIC_CURSOR_FIELDS)
        next_cursor_value = _encode_cursor(next_payload)

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = _extract_cursor_payload(rows[0], SCENARIO_METRIC_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = _extract_cursor_payload(rows[0], SCENARIO_METRIC_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})

    meta = Meta(
        request_id=request_id,
        query_time_ms=int(elapsed_ms),
        next_cursor=next_cursor_value,
        prev_cursor=prev_cursor_value,
    )
    model = ScenarioMetricLatestResponse(
        meta=meta,
        data=[ScenarioMetricLatest(**row) for row in rows],
    )
    return _respond_with_etag(
        model,
        request,
        response,
        cache_seconds=SCENARIO_METRIC_CACHE_TTL,
    )


@router.get("/v1/drought/dimensions", response_model=DroughtDimensionsResponse)
def get_drought_dimensions() -> DroughtDimensionsResponse:
    catalog = _drought_catalog()
    datasets = sorted({item.dataset for item in catalog.raster_indices})
    indices = sorted({idx for item in catalog.raster_indices for idx in item.indices})
    timescales = sorted({ts for item in catalog.raster_indices for ts in item.timescales})
    layers = sorted({layer.layer for layer in catalog.vector_layers if layer.layer != 'usdm'})
    region_types = sorted({region.region_type for region in catalog.region_sets})
    meta = Meta(request_id=_current_request_id(), query_time_ms=0)
    data = DroughtDimensions(
        datasets=datasets,
        indices=indices,
        timescales=timescales,
        layers=layers,
        region_types=region_types,
    )
    return DroughtDimensionsResponse(meta=meta, data=data)


@router.get("/v1/drought/indices", response_model=DroughtIndexResponse)
def get_drought_indices(
    request: Request,
    dataset: str | None = Query(None, description="Source dataset (nclimgrid, acis, cpc, prism, eddi)."),
    index: str | None = Query(None, description="Index name (SPI, SPEI, EDDI, PDSI, QuickDRI, VHI)."),
    timescale: str | None = Query(None, description="Accumulation window such as 1M, 3M, 6M."),
    region: str | None = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: str | None = Query(None),
    region_id: str | None = Query(None),
    start: date | None = Query(None, description="Inclusive start date (YYYY-MM-DD)."),
    end: date | None = Query(None, description="Inclusive end date (YYYY-MM-DD)."),
    limit: int = Query(250, ge=1, le=MAX_PAGE_SIZE),
) -> DroughtIndexResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = _trino_config()
    rows, elapsed = service.query_drought_indices(
        trino_cfg,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        dataset=dataset,
        index_id=index,
        timescale=timescale,
        start_date=start,
        end_date=end,
        limit=limit,
    )
    payload: list[DroughtIndexPoint] = []
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None
        payload.append(
            DroughtIndexPoint(
                series_id=row["series_id"],
                dataset=row["dataset"],
                index=row["index"],
                timescale=row["timescale"],
                valid_date=row["valid_date"],
                as_of=row.get("as_of"),
                value=row.get("value"),
                unit=row.get("unit"),
                poc=row.get("poc"),
                region_type=row["region_type"],
                region_id=row["region_id"],
                region_name=row.get("region_name"),
                parent_region_id=row.get("parent_region_id"),
                source_url=row.get("source_url"),
                metadata=metadata if isinstance(metadata, dict) else None,
            )
        )
    meta = Meta(request_id=_current_request_id(), query_time_ms=int(elapsed))
    return DroughtIndexResponse(meta=meta, data=payload)


@router.get("/v1/drought/usdm", response_model=DroughtUsdmResponse)
def get_drought_usdm(
    region: str | None = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: str | None = Query(None),
    region_id: str | None = Query(None),
    start: date | None = Query(None),
    end: date | None = Query(None),
    limit: int = Query(250, ge=1, le=MAX_PAGE_SIZE),
) -> DroughtUsdmResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = _trino_config()
    rows, elapsed = service.query_drought_usdm(
        trino_cfg,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        start_date=start,
        end_date=end,
        limit=limit,
    )
    payload: list[DroughtUsdmPoint] = []
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except json.JSONDecodeError:
                metadata = None
        payload.append(
            DroughtUsdmPoint(
                region_type=row["region_type"],
                region_id=row["region_id"],
                region_name=row.get("region_name"),
                parent_region_id=row.get("parent_region_id"),
                valid_date=row["valid_date"],
                as_of=row.get("as_of"),
                d0_frac=row.get("d0_frac"),
                d1_frac=row.get("d1_frac"),
                d2_frac=row.get("d2_frac"),
                d3_frac=row.get("d3_frac"),
                d4_frac=row.get("d4_frac"),
                source_url=row.get("source_url"),
                metadata=metadata if isinstance(metadata, dict) else None,
            )
        )
    meta = Meta(request_id=_current_request_id(), query_time_ms=int(elapsed))
    return DroughtUsdmResponse(meta=meta, data=payload)


@router.get(
    "/v1/drought/layers",
    response_model=DroughtVectorResponse,
    include_in_schema=False,
)
def get_drought_layers(
    layer: str | None = Query(None, description="Layer key (qpf, ahps_flood, aqi, wildfire)."),
    region: str | None = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: str | None = Query(None),
    region_id: str | None = Query(None),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(250, ge=1, le=MAX_PAGE_SIZE),
) -> DroughtVectorResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = _trino_config()
    rows, elapsed = service.query_drought_vector_events(
        trino_cfg,
        layer=layer.lower() if layer else None,
        region_type=parsed_region_type,
        region_id=parsed_region_id,
        start_time=start,
        end_time=end,
        limit=limit,
    )
    payload: list[DroughtVectorEventPoint] = []
    for row in rows:
        props = row.get("properties")
        if isinstance(props, str):
            try:
                props = json.loads(props)
            except json.JSONDecodeError:
                props = None
        payload.append(
            DroughtVectorEventPoint(
                layer=row["layer"],
                event_id=row["event_id"],
                region_type=row.get("region_type"),
                region_id=row.get("region_id"),
                region_name=row.get("region_name"),
                parent_region_id=row.get("parent_region_id"),
                valid_start=row.get("valid_start"),
                valid_end=row.get("valid_end"),
                value=row.get("value"),
                unit=row.get("unit"),
                category=row.get("category"),
                severity=row.get("severity"),
                source_url=row.get("source_url"),
                geometry_wkt=row.get("geometry_wkt"),
                properties=props if isinstance(props, dict) else None,
            )
        )
    meta = Meta(request_id=_current_request_id(), query_time_ms=int(elapsed))
    return DroughtVectorResponse(meta=meta, data=payload)


@router.get(
    "/v1/drought/tiles/{dataset}/{index}/{timescale}/{z}/{x}/{y}.png",
    response_class=Response,
)
def proxy_drought_tile(
    dataset: str,
    index: str,
    timescale: str,
    z: int,
    x: int,
    y: int,
    valid_date: date = Query(..., description="Valid date for the tile (YYYY-MM-DD)."),
) -> Response:
    catalog = _drought_catalog()
    try:
        raster = catalog.raster_by_name(dataset.lower())
    except KeyError:
        raise HTTPException(status_code=404, detail="unknown_dataset")
    slug = valid_date.isoformat()
    try:
        tile_url = raster.xyz_url(index.upper(), timescale.upper(), slug, z, x, y)
    except Exception:
        raise HTTPException(status_code=404, detail="tile_not_available")

    cache_client = _tile_cache()
    cache_key = f"drought:tile:{dataset.lower()}:{index.upper()}:{timescale.upper()}:{slug}:{z}:{x}:{y}"
    if cache_client is not None:
        try:
            cached = cache_client.get(cache_key)
        except Exception:
            cached = None
            _record_tile_cache_metric("tile", "error")
        if cached:
            _record_tile_cache_metric("tile", "hit")
            return Response(content=cached, media_type="image/png")
        _record_tile_cache_metric("tile", "miss")
    else:
        _record_tile_cache_metric("tile", "bypass")

    start_fetch = time.perf_counter()
    try:
        response = http_request("GET", tile_url, timeout=20.0)
    except httpx.HTTPStatusError as exc:
        _observe_tile_fetch("tile", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=exc.response.status_code, detail="tile_fetch_failed") from exc
    except httpx.RequestError as exc:
        _observe_tile_fetch("tile", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=502, detail=f"tile_fetch_failed: {exc}") from exc

    duration = time.perf_counter() - start_fetch
    _observe_tile_fetch("tile", "success", duration)

    content = response.content
    if cache_client is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, content)
        except Exception:
            pass
    media_type = response.headers.get("Content-Type", "image/png")
    return Response(content=content, media_type=media_type)


@router.get("/v1/drought/info/{dataset}/{index}/{timescale}", response_model=DroughtInfoResponse)
def get_drought_tile_metadata(
    dataset: str,
    index: str,
    timescale: str,
    valid_date: date = Query(..., description="Valid date for the raster (YYYY-MM-DD)."),
) -> DroughtInfoResponse:
    catalog = _drought_catalog()
    try:
        raster = catalog.raster_by_name(dataset.lower())
    except KeyError:
        raise HTTPException(status_code=404, detail="unknown_dataset")
    slug = valid_date.isoformat()
    try:
        info_url = raster.info_url(index.upper(), timescale.upper(), slug)
    except Exception:
        raise HTTPException(status_code=404, detail="info_not_available")

    cache_client = _tile_cache()
    cache_key = f"drought:info:{dataset.lower()}:{index.upper()}:{timescale.upper()}:{slug}"
    if cache_client is not None:
        try:
            cached = cache_client.get(cache_key)
        except Exception:
            cached = None
            _record_tile_cache_metric("info", "error")
        if cached:
            _record_tile_cache_metric("info", "hit")
            try:
                payload = json.loads(cached)
                meta = Meta(request_id=_current_request_id(), query_time_ms=0)
                return DroughtInfoResponse(meta=meta, data=payload)
            except json.JSONDecodeError:
                LOGGER.debug("Failed to decode cached info payload", exc_info=True)
        else:
            _record_tile_cache_metric("info", "miss")
    else:
        _record_tile_cache_metric("info", "bypass")

    start_fetch = time.perf_counter()
    try:
        response = http_request("GET", info_url, timeout=15.0)
    except httpx.HTTPStatusError as exc:
        _observe_tile_fetch("info", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=exc.response.status_code, detail="info_fetch_failed") from exc
    except httpx.RequestError as exc:
        _observe_tile_fetch("info", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=502, detail=f"info_fetch_failed: {exc}") from exc

    duration = time.perf_counter() - start_fetch
    _observe_tile_fetch("info", "success", duration)

    payload = response.json()
    if cache_client is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, response.text)
        except Exception:
            pass

    meta = Meta(request_id=_current_request_id(), query_time_ms=0)
    return DroughtInfoResponse(meta=meta, data=payload)


# Include external API router
if external_router is not None:
    router.include_router(external_router)

# Include database routers
try:
    from .database import (
        performance_router,
        query_analysis_router,
        optimization_router,
        connections_router,
        health_router,
        trino_admin_router,
    )
    router.include_router(performance_router, prefix="/v1/admin/db", tags=["Database"])
    router.include_router(query_analysis_router, prefix="/v1/admin/db", tags=["Database"])
    router.include_router(optimization_router, prefix="/v1/admin/db", tags=["Database"])
    router.include_router(connections_router, prefix="/v1/admin/db", tags=["Database"])
    router.include_router(health_router, prefix="/v1/admin/db", tags=["Database"])
    router.include_router(trino_admin_router, prefix="/v1/admin/trino", tags=["Trino"])
except Exception:  # pragma: no cover - optional during tooling
    pass
