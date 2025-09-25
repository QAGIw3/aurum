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
from fastapi.responses import JSONResponse
from ._fastapi_compat import Query, Path
from fastapi.responses import StreamingResponse

from .config import CacheConfig, TrinoConfig
from . import service
from .container import get_service
from .ppa_utils import _persist_ppa_valuation_records, _scenario_outputs_enabled
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
    Meta,
    IsoLmpAggregatePoint,
    IsoLmpAggregateResponse,
    IsoLmpPoint,
    IsoLmpResponse,
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
from .health_checks import (
    check_redis_ready,
    check_timescale_ready,
    check_trino_ready,
)
from .cache.cache import CacheManager
try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as _PromCounter, REGISTRY as _PROM_REG
except Exception:  # pragma: no cover - metrics optional
    _PromCounter = None  # type: ignore[assignment]
    _PROM_REG = None  # type: ignore[assignment]

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
from aurum.core import AurumSettings
from aurum.core.pagination import Cursor
from .state import get_settings
from aurum.reference import eia_catalog as ref_eia
try:  # pragma: no cover - optional dependency
    from aurum.drought.catalog import load_catalog, DroughtCatalog
except ModuleNotFoundError:  # pragma: no cover - drought features optional
    load_catalog = None  # type: ignore[assignment]
    DroughtCatalog = None  # type: ignore[assignment]
from aurum.telemetry.context import get_request_id

# Import external API router (optional during tooling/doc generation)
try:  # pragma: no cover
    from .handlers.external import router as external_router
except Exception:  # pragma: no cover - optional
    external_router = None  # type: ignore

router = APIRouter()

# Basic health check endpoint
@router.get("/", tags=["health"])
async def root():
    """Root endpoint for basic health check."""
    return {"message": "Aurum API is running", "status": "healthy"}

@router.get("/health", tags=["health"])
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "aurum-api"}

# Mock endpoint for testing
@router.get("/v1/metadata/units", tags=["metadata"])
async def get_units():
    """Mock endpoint for units metadata."""
    import hashlib
    import json

    data = {
        "data": [
            {"id": "MWh", "name": "Megawatt Hours", "category": "Energy"},
            {"id": "MW", "name": "Megawatts", "category": "Power"},
        ],
        "meta": {"total": 2, "page": 1, "page_size": 100}
    }

    # Generate ETag from data
    data_str = json.dumps(data, sort_keys=True)
    etag = hashlib.md5(data_str.encode()).hexdigest()

    response = JSONResponse(content=data)
    response.headers["ETag"] = etag
    response.headers["Cache-Control"] = "public, max-age=300"
    return response

def _settings() -> AurumSettings:
    return get_settings()

def _trino_config() -> TrinoConfig:
    return TrinoConfig.from_settings(_settings())

def _cache_config(*, ttl_override: int | None = None) -> CacheConfig:
    return CacheConfig.from_settings(_settings(), ttl_override=ttl_override)

def configure_routes(settings: AurumSettings) -> None:
    global ADMIN_GROUPS, _TILE_CACHE_CFG, _TILE_CACHE, _INMEM_TTL
    global METADATA_CACHE_TTL
    global CURVE_MAX_LIMIT, EIA_SERIES_MAX_LIMIT
    global CURVE_CACHE_TTL, CURVE_DIFF_CACHE_TTL, CURVE_STRIP_CACHE_TTL
    global EIA_SERIES_CACHE_TTL, EIA_SERIES_DIMENSIONS_CACHE_TTL
    global _METRICS_ENABLED, _METRICS_PATH
    ADMIN_GROUPS = _load_admin_groups(settings)
    _TILE_CACHE_CFG = CacheConfig.from_settings(settings)
    _TILE_CACHE = None
    _INMEM_TTL = settings.api.cache.in_memory_ttl
    METADATA_CACHE_TTL = settings.api.cache.metadata_ttl
    CURVE_CACHE_TTL = settings.api.cache.curve_ttl
    CURVE_DIFF_CACHE_TTL = settings.api.cache.curve_diff_ttl
    CURVE_STRIP_CACHE_TTL = settings.api.cache.curve_strip_ttl
    EIA_SERIES_CACHE_TTL = settings.api.cache.eia_series_ttl
    EIA_SERIES_DIMENSIONS_CACHE_TTL = settings.api.cache.eia_series_dimensions_ttl
    CURVE_MAX_LIMIT = settings.pagination.curves_max_limit
    EIA_SERIES_MAX_LIMIT = settings.pagination.eia_series_max_limit
    _METRICS_ENABLED = bool(
        settings.api.metrics.enabled and PROMETHEUS_AVAILABLE and METRICS_MIDDLEWARE is not None
    )
    path = settings.api.metrics.path
    _METRICS_PATH = path if path.startswith("/") else f"/{path}"

    if _METRICS_ENABLED:
        observability_metrics.METRICS_PATH = _METRICS_PATH

    with _METADATA_FALLBACK_LOCK:
        _METADATA_FALLBACK_CACHE.clear()

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
LOGGER = logging.getLogger(__name__)


# --- Metadata caching helpers using CacheManager with in-process fallback ---

_CACHE_MANAGER: CacheManager | None = None
_METADATA_FALLBACK_CACHE: dict[str, tuple[float, Any]] = {}
_METADATA_FALLBACK_LOCK = threading.Lock()
_INMEM_TTL = 60


def set_cache_manager(manager: CacheManager | None) -> None:
    """Register the active CacheManager for synchronous helpers."""
    global _CACHE_MANAGER
    _CACHE_MANAGER = manager


def _metadata_fallback_get(key: str) -> Any | None:
    now = _time.time()
    with _METADATA_FALLBACK_LOCK:
        entry = _METADATA_FALLBACK_CACHE.get(key)
        if not entry:
            return None
        expires_at, value = entry
        if now >= expires_at:
            _METADATA_FALLBACK_CACHE.pop(key, None)
            return None
        return value


def _metadata_fallback_set(key: str, value: Any, ttl: int) -> None:
    with _METADATA_FALLBACK_LOCK:
        _METADATA_FALLBACK_CACHE[key] = (_time.time() + ttl, value)


async def _cache_get_or_set(
    request: Request | None,
    key_suffix: str,
    supplier,
    ttl: int,
) -> Any:
    manager: CacheManager | None = _CACHE_MANAGER
    if request is not None:
        maybe_manager = getattr(getattr(request, "app", None), "state", None)
        if maybe_manager is not None:
            manager = getattr(maybe_manager, "cache_manager", manager)

    if isinstance(manager, CacheManager):
        cached = await manager.get_cache_entry(key_suffix)
        if cached is not None:
            return cached
        value = supplier()
        await manager.set_cache_entry(key_suffix, value, ttl_seconds=ttl)
        return value

    fallback = _metadata_fallback_get(key_suffix)
    if fallback is not None:
        return fallback

    value = supplier()
    _metadata_fallback_set(key_suffix, value, ttl)
    return value


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
    cached = _metadata_fallback_get(key_suffix)
    if cached is not None:
        return cached

    value = supplier()
    _metadata_fallback_set(key_suffix, value, METADATA_CACHE_TTL)
    return value


def _metadata_cache_purge(prefixes: Sequence[str]) -> int:
    removed = 0
    with _METADATA_FALLBACK_LOCK:
        for prefix in prefixes:
            keys = [k for k in _METADATA_FALLBACK_CACHE if k.startswith(prefix)]
            for key in keys:
                _METADATA_FALLBACK_CACHE.pop(key, None)
            removed += len(keys)
    return removed


def _generate_etag(data: Dict[str, Any]) -> str:
    """Generate an ETag from data dictionary.

    Args:
        data: Dictionary containing data to generate ETag from

    Returns:
        ETag string
    """
    import hashlib
    import json

    # Sort keys for consistent hashing
    sorted_data = json.dumps(data, sort_keys=True)
    return hashlib.md5(sorted_data.encode()).hexdigest()


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


# Register auth middleware after health/metrics route definitions


# Frozen cursor field definitions - these are now defined in pagination.py
# Use FROZEN_CURSOR_SCHEMAS instead of these legacy field lists
# These constants are kept for backward compatibility but should be deprecated

METADATA_CACHE_TTL = 300

CURVE_MAX_LIMIT = 500
CURVE_CACHE_TTL = 120
CURVE_DIFF_CACHE_TTL = 120
CURVE_STRIP_CACHE_TTL = 120

CURVE_CURSOR_FIELDS = [
    "curve_key",
    "tenor_label",
    "contract_month",
    "asof_date",
    "price_type",
]
CURVE_DIFF_CURSOR_FIELDS = [
    "curve_key",
    "tenor_label",
    "contract_month",
]

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
    extract_cursor_payload_from_row,
    extract_cursor_values,
    validate_cursor_schema,
)

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


async def _check_trino_ready(cfg: TrinoConfig) -> bool:
    return await check_trino_ready(cfg)


def _check_timescale_ready(dsn: str) -> bool:
    result = check_timescale_ready(dsn)
    if not result:
        LOGGER.debug("Timescale readiness check failed")
    return result


def _check_redis_ready(cache_cfg: CacheConfig) -> bool:
    result = check_redis_ready(cache_cfg)
    if not result:
        LOGGER.debug("Redis readiness check failed")
    return result


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
    effective_offset = offset or 0
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

    # Guardrails disabled in simplified mode to keep lightweight behaviour

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

    normalised_rows: list[dict[str, Any]] = []
    for row in rows:
        coerced = dict(row)
        coerced.setdefault("tenor_label", coerced.get("tenor_type", "unknown"))
        coerced.setdefault("asof_date", date.today())
        normalised_rows.append(coerced)

    rows = normalised_rows

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    else:
        if more:
            rows = rows[:effective_limit]

    next_cursor = None
    if more and rows:
        next_payload = extract_cursor_payload_from_row(rows[-1], CURVE_CURSOR_FIELDS)
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
                prev_payload = extract_cursor_payload_from_row(rows[0], CURVE_CURSOR_FIELDS)
                prev_cursor_obj = Cursor(
                    offset=effective_offset,
                    limit=effective_limit,
                    timestamp=time.time(),
                    filters={**current_filters, "before": prev_payload}
                )
                prev_cursor_value = prev_cursor_obj.to_string()
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = extract_cursor_payload_from_row(rows[0], CURVE_CURSOR_FIELDS)
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
        next_payload = extract_cursor_payload_from_row(rows[-1], CURVE_DIFF_CURSOR_FIELDS)
        next_cursor = encode_cursor(next_payload)

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
async def list_eia_datasets(
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

    base = await _cache_get_or_set(
        request,
        "eia:datasets",
        _base_eia,
        METADATA_CACHE_TTL,
    )
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
async def get_eia_dataset(
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
        data = await _cache_get_or_set(
            request,
            f"eia:detail:{dataset_path}",
            _detail,
            METADATA_CACHE_TTL,
        )
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
        payload = decode_cursor(prev_token)
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
        payload = decode_cursor(forward_token)
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
        next_payload = extract_cursor_payload_from_row(rows[-1], EIA_SERIES_CURSOR_FIELDS)
        next_cursor = encode_cursor(next_payload)

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = extract_cursor_payload_from_row(rows[0], EIA_SERIES_CURSOR_FIELDS)
                prev_cursor_value = encode_cursor({"before": prev_payload})
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = extract_cursor_payload_from_row(rows[0], EIA_SERIES_CURSOR_FIELDS)
                prev_cursor_value = encode_cursor({"before": prev_payload})

    if not rows and forward_token and prev_token is None and effective_offset > 0:
        prev_cursor_value = encode_cursor({"offset": max(effective_offset - effective_limit, 0)})

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
        payload = decode_cursor(effective_cursor)
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
        next_cursor = encode_cursor({"offset": effective_offset + effective_limit})

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


__all__ = [
    "router",
    "configure_routes",
    "METRICS_MIDDLEWARE",
]


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
