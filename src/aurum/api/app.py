from __future__ import annotations

"""FastAPI application exposing curve endpoints."""

import base64
import json
import uuid
from datetime import date
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Response, Request
from ._fastapi_compat import Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware

from .config import CacheConfig, TrinoConfig
from . import service
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
    ScenarioRunOptions,
    ScenarioRunResponse,
    ScenarioRunData,
    ScenarioOutputResponse,
    ScenarioOutputPoint,
    PpaValuationRequest,
    PpaValuationResponse,
    PpaMetric,
    IsoLocationOut,
    IsoLocationsResponse,
    IsoLocationResponse,
    UnitsCanonicalResponse,
    UnitsCanonical,
    UnitMappingOut,
    UnitsMappingResponse,
    CalendarsResponse,
    CalendarOut,
    CalendarBlocksResponse,
    CalendarHoursResponse,
    EiaDatasetsResponse,
    EiaDatasetBriefOut,
    EiaDatasetResponse,
    EiaDatasetDetailOut,
)
from .ratelimit import RateLimitConfig, RateLimitMiddleware
from .auth import AuthMiddleware, OIDCConfig
from .scenario_service import STORE as ScenarioStore
from aurum.reference import iso_locations as ref_iso
from aurum.reference import units as ref_units
from aurum.reference import calendars as ref_cal
from aurum.reference import eia_catalog as ref_eia


app = FastAPI(title="Aurum API", version="0.1.0")

# CORS (configurable via env AURUM_API_CORS_ORIGINS, comma-separated)
import os
import threading
import time as _time
origins_raw = os.getenv("AURUM_API_CORS_ORIGINS", "")
origins = [o.strip() for o in origins_raw.split(",") if o.strip()] or ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# GZip compression (configurable minimum size via env AURUM_API_GZIP_MIN_BYTES)
try:
    min_bytes = int(os.getenv("AURUM_API_GZIP_MIN_BYTES", "500") or 500)
except ValueError:
    min_bytes = 500
app.add_middleware(GZipMiddleware, minimum_size=min_bytes)

# Structured access logging
ACCESS_LOGGER = logging.getLogger("aurum.api.access")


@app.middleware("http")
async def _access_log_middleware(request, call_next):  # type: ignore[no-redef]
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    start = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000.0
        principal = getattr(request.state, "principal", {}) or {}
        ACCESS_LOGGER.info(
            json.dumps(
                {
                    "event": "access",
                    "method": request.method,
                    "path": request.url.path,
                    "status": 500,
                    "duration_ms": round(duration_ms, 2),
                    "request_id": request_id,
                    "client_ip": request.client.host if request.client else None,
                    "tenant": principal.get("tenant"),
                    "subject": principal.get("sub"),
                }
            )
        )
        raise
    duration_ms = (time.perf_counter() - start) * 1000.0
    principal = getattr(request.state, "principal", {}) or {}
    try:
        response.headers["X-Request-Id"] = request_id
    except Exception:
        pass
    ACCESS_LOGGER.info(
        json.dumps(
            {
                "event": "access",
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": round(duration_ms, 2),
                "request_id": request_id,
                "client_ip": request.client.host if request.client else None,
                "tenant": principal.get("tenant"),
                "subject": principal.get("sub"),
            }
        )
    )
    return response

# Attach simple rate limiting middleware (Redis-backed when configured)
app.add_middleware(
    RateLimitMiddleware,
    cache_cfg=CacheConfig.from_env(),
    rl_cfg=RateLimitConfig.from_env(),
)

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


_INMEM_TTL = int(os.getenv("AURUM_API_INMEMORY_TTL", "60") or 60)
_CACHE = _SimpleCache(default_ttl=_INMEM_TTL)

# Prometheus metrics
try:  # pragma: no cover
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

    REQUEST_COUNTER = Counter(
        "aurum_api_requests_total", "Total API requests", ["method", "path", "status"]
    )
    REQUEST_LATENCY = Histogram(
        "aurum_api_request_duration_seconds", "API request duration in seconds", ["method", "path"]
    )

    @app.middleware("http")
    async def _metrics_middleware(request, call_next):  # type: ignore[no-redef]
        method = request.method
        path = request.url.path
        with REQUEST_LATENCY.labels(method=method, path=path).time():
            response = await call_next(request)
        REQUEST_COUNTER.labels(method=method, path=path, status=str(response.status_code)).inc()
        return response

    @app.get("/metrics")
    def metrics():
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)
except Exception:  # pragma: no cover
    pass


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict:
    trino_cfg = TrinoConfig.from_env()
    if _check_trino_ready(trino_cfg):
        return {"status": "ready"}
    raise HTTPException(status_code=503, detail="Upstream dependencies unavailable")

# Register auth middleware after health/metrics route definitions
app.add_middleware(AuthMiddleware, config=OIDCConfig.from_env())


CURVE_CURSOR_FIELDS = ["curve_key", "tenor_label", "contract_month", "asof_date", "price_type"]
CURVE_DIFF_CURSOR_FIELDS = ["curve_key", "tenor_label", "contract_month"]
SCENARIO_OUTPUT_CURSOR_FIELDS = [
    "scenario_id",
    "curve_key",
    "tenor_label",
    "contract_month",
    "metric",
]


def _encode_cursor(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_cursor(token: str) -> dict:
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        return json.loads(raw.decode("utf-8"))
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc


def _resolve_tenant(request: Request, explicit: Optional[str]) -> str:
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant
    if explicit:
        return explicit
    raise HTTPException(status_code=400, detail="tenant_id is required")


def _resolve_tenant_optional(request: Request, explicit: Optional[str]) -> Optional[str]:
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant
    return explicit


def _extract_cursor_payload(row: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {}
    for field in fields:
        value = row.get(field)
        if isinstance(value, date):
            payload[field] = value.isoformat()
        elif value is not None:
            payload[field] = value
        else:
            payload[field] = None
    return payload


def _normalise_cursor_input(payload: dict) -> tuple[Optional[int], Optional[dict]]:
    if "offset" in payload:
        try:
            return int(payload.get("offset", 0)), None
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid offset cursor") from exc
    return 0, payload


def _scenario_outputs_enabled() -> bool:
    flag = os.getenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "0").lower()
    return flag in {"1", "true", "yes", "on"}


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


@app.get("/v1/curves", response_model=CurveResponse)
def list_curves(
    asof: Optional[date] = Query(None, description="As-of date filter (YYYY-MM-DD)"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
) -> CurveResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    cursor_after: Optional[dict] = None
    if cursor:
        payload = _decode_cursor(cursor)
        offset, cursor_after = _normalise_cursor_input(payload)

    try:
        # Fetch one extra row to determine if there is a next page
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
            limit=limit + 1,
            offset=offset,
            cursor_after=cursor_after,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_payload = _extract_cursor_payload(rows[-1], CURVE_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    return CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )


@app.get("/v1/curves/diff", response_model=CurveDiffResponse)
def list_curves_diff(
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
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
) -> CurveDiffResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    cursor_after: Optional[dict] = None
    if cursor:
        payload = _decode_cursor(cursor)
        offset, cursor_after = _normalise_cursor_input(payload)

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
            limit=limit + 1,
            offset=offset,
            cursor_after=cursor_after,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_payload = _extract_cursor_payload(rows[-1], CURVE_DIFF_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    return CurveDiffResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurveDiffPoint(**row) for row in rows],
    )


@app.get("/v1/metadata/dimensions", response_model=DimensionsResponse, tags=["Metadata"])
def list_dimensions(
    asof: Optional[date] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    per_dim_limit: int = Query(1000, ge=1, le=5000),
    prefix: Optional[str] = Query(None, description="Optional case-insensitive startswith filter applied to each dimension list"),
    include_counts: bool = Query(False, description="Include per-dimension value counts when true"),
) -> DimensionsResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

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

    return DimensionsResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=DimensionsData(**results),
        counts=counts_model,
    )


@app.get(
    "/v1/metadata/locations",
    response_model=IsoLocationsResponse,
    tags=["Metadata"],
    summary="List ISO locations",
    description="Return known ISO locations with optional filters by ISO code and a case-insensitive prefix applied to id or name.",
)
def list_locations(
    iso: Optional[str] = Query(None, description="Filter by ISO code (e.g., PJM, CAISO)", examples={"default": {"value": "PJM"}}),
    prefix: Optional[str] = Query(None, description="Case-insensitive startswith on id or name", examples={"default": {"value": "AE"}}),
    response: Response = None,  # type: ignore[assignment]
) -> IsoLocationsResponse:
    request_id = str(uuid.uuid4())
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

    base = _CACHE.get_or_set(f"iso:locations:{(iso or '').upper()}", _base_locations)
    items: list[IsoLocationOut] = []
    if prefix:
        needle = prefix.lower()
        for rec in base:
            name = (rec.get("location_name") or "").lower()
            if rec["location_id"].lower().startswith(needle) or name.startswith(needle):
                items.append(IsoLocationOut(**rec))
    else:
        items = [IsoLocationOut(**rec) for rec in base]
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return IsoLocationsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)


@app.get(
    "/v1/metadata/locations/{iso}/{location_id}",
    response_model=IsoLocationResponse,
    tags=["Metadata"],
    summary="Get ISO location",
    description="Return metadata for a specific ISO location id (case-insensitive match).",
)
def get_location(
    iso: str = Path(..., description="ISO code", examples={"default": {"value": "PJM"}}),
    location_id: str = Path(..., description="Location identifier", examples={"default": {"value": "AECO"}}),
    response: Response = None,  # type: ignore[assignment]
) -> IsoLocationResponse:
    request_id = str(uuid.uuid4())
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
        rec = _CACHE.get_or_set(f"iso:loc:{iso.upper()}:{location_id.upper()}", _load_loc)
    except LookupError:
        raise HTTPException(status_code=404, detail="Location not found")
    item = IsoLocationOut(**rec)
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return IsoLocationResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=item)


# --- Units metadata ---


@app.get(
    "/v1/metadata/units",
    response_model=UnitsCanonicalResponse,
    tags=["Metadata"],
    summary="List canonical currencies and units",
    description="Return distinct currencies and units derived from the units mapping file. Optionally filter by case-insensitive prefix.",
)
def list_units(prefix: Optional[str] = Query(None, description="Optional startswith filter", examples={"sample": {"value": "US"}}), response: Response = None) -> UnitsCanonicalResponse:  # type: ignore[assignment]
    request_id = str(uuid.uuid4())
    def _base():
        mapper = ref_units.UnitsMapper()
        mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
        curr = sorted({rec.currency for rec in mapping.values() if rec.currency})
        unit_list = sorted({rec.per_unit for rec in mapping.values() if rec.per_unit})
        return {"currencies": curr, "units": unit_list}

    base = _CACHE.get_or_set("units:canonical", _base)
    currencies = base["currencies"]
    units = base["units"]
    if prefix:
        pfx = prefix.lower()
        currencies = [c for c in currencies if c.lower().startswith(pfx)]
        units = [u for u in units if u.lower().startswith(pfx)]
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return UnitsCanonicalResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=UnitsCanonical(currencies=currencies, units=units))


@app.get(
    "/v1/metadata/units/mapping",
    response_model=UnitsMappingResponse,
    tags=["Metadata", "Units"],
    summary="List unit mappings",
    description="Return raw-to-canonical unit mappings. Optionally filter by case-insensitive prefix on the raw units string.",
)
def list_unit_mappings(
    prefix: Optional[str] = Query(None, description="Startswith filter on units_raw", examples={"sample": {"value": "USD/"}}),
    response: Response = None,  # type: ignore[assignment]
) -> UnitsMappingResponse:
    request_id = str(uuid.uuid4())
    def _base_list():
        mapper = ref_units.UnitsMapper()
        mapping = mapper._load_mapping(mapper._path)  # type: ignore[attr-defined]
        tuples: list[tuple[str, str | None, str | None]] = []
        for _, rec in mapping.items():
            tuples.append((rec.raw, rec.currency or None, rec.per_unit or None))
        tuples.sort(key=lambda t: t[0])
        return tuples

    base = _CACHE.get_or_set("units:mapping", _base_list)
    items: list[UnitMappingOut] = []
    if prefix:
        pfx = prefix.lower()
        for raw, cur, per in base:
            if raw.lower().startswith(pfx):
                items.append(UnitMappingOut(units_raw=raw, currency=cur, per_unit=per))
    else:
        items = [UnitMappingOut(units_raw=raw, currency=cur, per_unit=per) for raw, cur, per in base]
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return UnitsMappingResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)


# --- Calendars metadata ---


@app.get(
    "/v1/metadata/calendars",
    response_model=CalendarsResponse,
    tags=["Metadata"],
    summary="List calendars",
    description="Return available trading calendars and their timezones.",
)
def list_calendars() -> CalendarsResponse:
    request_id = str(uuid.uuid4())
    calendars = ref_cal.get_calendars()
    items = [CalendarOut(name=name, timezone=cfg.timezone) for name, cfg in calendars.items()]
    items.sort(key=lambda x: x.name)
    return CalendarsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)


@app.get(
    "/v1/metadata/calendars/{name}/blocks",
    response_model=CalendarBlocksResponse,
    tags=["Metadata"],
    summary="List calendar blocks",
    description="Return block names for a given calendar.",
)
def list_calendar_blocks(name: str = Path(..., description="Calendar name", examples={"default": {"value": "us"}})) -> CalendarBlocksResponse:
    request_id = str(uuid.uuid4())
    calendars = ref_cal.get_calendars()
    cfg = calendars.get(name.lower())
    if cfg is None:
        raise HTTPException(status_code=404, detail="Calendar not found")
    blocks = sorted(cfg.blocks.keys())
    return CalendarBlocksResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=blocks)


@app.get(
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
) -> CalendarHoursResponse:  # type: ignore[no-redef]
    request_id = str(uuid.uuid4())
    datetimes = ref_cal.hours_for_block(name, block, date)
    iso_list = [dt.isoformat() for dt in datetimes]
    return CalendarHoursResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=iso_list)


@app.get(
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
) -> CalendarHoursResponse:
    request_id = str(uuid.uuid4())
    datetimes = ref_cal.expand_block_range(name, block, start, end)
    iso_list = [dt.isoformat() for dt in datetimes]
    return CalendarHoursResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=iso_list)


# --- EIA Catalog metadata ---


@app.get(
    "/v1/metadata/eia/datasets",
    response_model=EiaDatasetsResponse,
    tags=["Metadata", "EIA"],
    summary="List EIA datasets",
    description="Return EIA datasets from the harvested catalog with optional case-insensitive prefix filter on path or name.",
)
def list_eia_datasets(
    prefix: Optional[str] = Query(None, description="Startswith filter on path or name", examples={"default": {"value": "natural-gas/"}}),
    response: Response = None,  # type: ignore[assignment]
) -> EiaDatasetsResponse:
    request_id = str(uuid.uuid4())
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
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return EiaDatasetsResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=items)


@app.get(
    "/v1/metadata/eia/datasets/{dataset_path:path}",
    response_model=EiaDatasetResponse,
    tags=["Metadata", "EIA"],
    summary="Get EIA dataset",
    description="Return dataset metadata including facets, frequencies, and data columns for the specified dataset path.",
)
def get_eia_dataset(
    dataset_path: str = Path(..., description="Dataset path (e.g., natural-gas/stor/wkly)", examples={"default": {"value": "natural-gas/stor/wkly"}}),
    response: Response = None,  # type: ignore[assignment]
) -> EiaDatasetResponse:
    request_id = str(uuid.uuid4())
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
    if isinstance(response, Response):
        response.headers["Cache-Control"] = f"public, max-age={_INMEM_TTL}"
    return EiaDatasetResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=item)


@app.get("/v1/curves/strips", response_model=CurveResponse)
def list_strips(
    asof: Optional[date] = Query(None),
    type: str = Query(..., pattern="^(CALENDAR|SEASON|QUARTER)$", description="Strip type"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
) -> CurveResponse:
    request_id = str(uuid.uuid4())
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    if cursor:
        payload = _decode_cursor(cursor)
        offset = int(payload.get("offset", 0))

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
            limit=limit + 1,
            offset=offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: str | None = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_cursor({"offset": offset + limit})

    return CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )


__all__ = ["app"]


# --- Scenario endpoints (stubbed service behavior for now) ---


@app.post("/v1/scenarios", response_model=ScenarioResponse, status_code=201)
def create_scenario(payload: CreateScenarioRequest, request: Request) -> ScenarioResponse:
    request_id = str(uuid.uuid4())
    tenant_id = _resolve_tenant(request, payload.tenant_id)
    record = ScenarioStore.create_scenario(
        tenant_id=tenant_id,
        name=payload.name,
        description=payload.description,
        assumptions=payload.assumptions,
    )
    return ScenarioResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=ScenarioData(
            scenario_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            description=record.description,
            status=record.status,
            assumptions=record.assumptions,
            created_at=record.created_at.isoformat(),
        ),
    )


@app.get("/v1/scenarios/{scenario_id}", response_model=ScenarioResponse)
def get_scenario(scenario_id: str, request: Request) -> ScenarioResponse:
    request_id = str(uuid.uuid4())
    tenant_id = _resolve_tenant_optional(request, None)
    record = ScenarioStore.get_scenario(scenario_id, tenant_id=tenant_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return ScenarioResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=ScenarioData(
            scenario_id=record.id,
            tenant_id=record.tenant_id,
            name=record.name,
            description=record.description,
            status=record.status,
            assumptions=record.assumptions,
            created_at=record.created_at.isoformat(),
        ),
    )


@app.post("/v1/scenarios/{scenario_id}/run", response_model=ScenarioRunResponse, status_code=202)
def run_scenario(
    scenario_id: str,
    request: Request,
    options: ScenarioRunOptions | None = None,
) -> ScenarioRunResponse:
    request_id = str(uuid.uuid4())
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
    )
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=ScenarioRunData(
            run_id=run.run_id,
            scenario_id=run.scenario_id,
            state=run.state,
            code_version=run.code_version,
            seed=run.seed,
            created_at=run.created_at.isoformat(),
        ),
    )


@app.get("/v1/scenarios/{scenario_id}/runs/{run_id}", response_model=ScenarioRunResponse)
def get_scenario_run(scenario_id: str, run_id: str, request: Request) -> ScenarioRunResponse:
    request_id = str(uuid.uuid4())
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.get_run_for_scenario(scenario_id, run_id, tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=ScenarioRunData(
            run_id=run.run_id,
            scenario_id=run.scenario_id,
            state=run.state,
            code_version=run.code_version,
            seed=run.seed,
            created_at=run.created_at.isoformat(),
        ),
    )


@app.post("/v1/scenarios/runs/{run_id}/state", response_model=ScenarioRunResponse)
def update_scenario_run_state(
    run_id: str,
    request: Request,
    state: str = Query(..., pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED)$"),
) -> ScenarioRunResponse:
    request_id = str(uuid.uuid4())
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.update_run_state(run_id, state=state, tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=ScenarioRunData(
            run_id=run.run_id,
            scenario_id=run.scenario_id,
            state=run.state,
            code_version=run.code_version,
            seed=run.seed,
            created_at=run.created_at.isoformat(),
        ),
    )


@app.post("/v1/ppa/valuate", response_model=PpaValuationResponse)
def valuate_ppa(payload: PpaValuationRequest) -> PpaValuationResponse:
    request_id = str(uuid.uuid4())
    # Placeholder behavior: return empty metrics list until valuation engine is wired
    return PpaValuationResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=[])


@app.get("/v1/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputResponse)
def list_scenario_outputs(
    scenario_id: str,
    request: Request,
    metric: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None),
    curve_key: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    cursor: Optional[str] = Query(None),
) -> ScenarioOutputResponse:
    if not _scenario_outputs_enabled():
        raise HTTPException(status_code=503, detail="Scenario outputs not enabled")

    request_id = str(uuid.uuid4())
    tenant_id = _resolve_tenant_optional(request, None)
    trino_cfg = TrinoConfig.from_env()
    cache_cfg = CacheConfig.from_env()

    cursor_after: Optional[dict] = None
    offset = 0
    if cursor:
        payload = _decode_cursor(cursor)
        offset, cursor_after = _normalise_cursor_input(payload)

    try:
        rows, elapsed_ms = service.query_scenario_outputs(
            trino_cfg,
            cache_cfg,
            tenant_id=tenant_id,
            scenario_id=scenario_id,
            curve_key=curve_key,
            tenor_type=tenor_type,
            metric=metric,
            limit=limit + 1,
            offset=offset,
            cursor_after=cursor_after,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor: Optional[str] = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_payload = _extract_cursor_payload(rows[-1], SCENARIO_OUTPUT_CURSOR_FIELDS)
        next_cursor = _encode_cursor(next_payload)

    return ScenarioOutputResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[ScenarioOutputPoint(**row) for row in rows],
    )
