from __future__ import annotations

"""FastAPI application exposing curve endpoints."""

import base64
import csv
import hashlib
import io
import json
import math
import os
import uuid
from pathlib import Path as FilePath
from datetime import date, datetime
import logging
import time
import httpx
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dataclasses import replace
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set

from fastapi import Depends, FastAPI, HTTPException, Response, Request
try:
    from opentelemetry import trace
except ImportError:  # pragma: no cover - optional dependency
    trace = None  # type: ignore[assignment]
from ._fastapi_compat import Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse

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
)
from .ratelimit import RateLimitConfig, RateLimitMiddleware
from .auth import AuthMiddleware, OIDCConfig
from .scenario_service import STORE as ScenarioStore
from aurum.reference import iso_locations as ref_iso
from aurum.reference import units as ref_units
from aurum.reference import calendars as ref_cal
from aurum.reference import eia_catalog as ref_eia
from aurum.telemetry import configure_telemetry
from aurum.drought.catalog import load_catalog, DroughtCatalog
from aurum.telemetry.context import get_request_id, set_request_id, reset_request_id


app = FastAPI(title="Aurum API", version="0.1.0")

configure_telemetry("aurum-api", fastapi_app=app, enable_psycopg=True)

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


def _load_admin_groups() -> Set[str]:
    raw = os.getenv("AURUM_API_ADMIN_GROUP", "aurum-admins")
    groups = {item.strip().lower() for item in raw.split(",") if item.strip()}
    if os.getenv("AURUM_API_AUTH_DISABLED", "0").lower() in {"1", "true", "yes"}:
        return set()
    return groups


ADMIN_GROUPS: Set[str] = _load_admin_groups()


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
    groups = principal.get("groups") or []
    normalized = {str(group).lower() for group in groups if group}
    return any(group in normalized for group in ADMIN_GROUPS)


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
_TILE_CACHE_CFG = CacheConfig.from_env()
_TILE_CACHE = None


def _drought_catalog() -> DroughtCatalog:
    global _DROUGHT_CATALOG
    if _DROUGHT_CATALOG is None:
        _DROUGHT_CATALOG = load_catalog(_CATALOG_PATH)
    return _DROUGHT_CATALOG


def _tile_cache():
    global _TILE_CACHE
    if _TILE_CACHE is not None:
        return _TILE_CACHE
    try:
        client = service._maybe_redis_client(_TILE_CACHE_CFG)
    except Exception:
        client = None
    _TILE_CACHE = client
    return _TILE_CACHE

# CORS (configurable via env AURUM_API_CORS_ORIGINS, comma-separated)
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
LOGGER = logging.getLogger(__name__)


@app.middleware("http")
async def _access_log_middleware(request, call_next):  # type: ignore[no-redef]
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    token = set_request_id(request_id)
    request.state.request_id = request_id
    span = trace.get_current_span() if trace else None
    if span is not None and getattr(span, "is_recording", lambda: False)():  # pragma: no branch - inexpensive check
        span.set_attribute("aurum.request_id", request_id)
        span.set_attribute("http.request_id", request_id)
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
        reset_request_id(token)
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
    reset_request_id(token)
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


def _model_dump(value: Any) -> Dict[str, Any]:
    if hasattr(value, "model_dump"):
        return value.model_dump()  # type: ignore[attr-defined]
    if hasattr(value, "dict"):
        return value.dict()  # type: ignore[attr-defined]
    if isinstance(value, dict):
        return value
    raise TypeError(f"Unsupported payload type for ETag generation: {type(value)!r}")



def _compute_etag(payload: Dict[str, Any]) -> str:
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()



def _metadata_cache_get_or_set(key_suffix: str, supplier):
    cache_cfg = replace(CacheConfig.from_env(), ttl_seconds=METADATA_CACHE_TTL)
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
        except Exception:
            pass
    return value


def _respond_with_etag(
    model,
    request: Request,
    response: Response,
    *,
    extra_headers: Optional[Dict[str, str]] = None,
):
    payload = _model_dump(model)
    etag_payload = {k: v for k, v in payload.items() if k != "meta"}
    etag = _compute_etag(etag_payload)
    incoming = request.headers.get("if-none-match")
    if incoming and incoming == etag:
        headers = {"ETag": etag}
        if extra_headers:
            headers.update(extra_headers)
        return Response(status_code=304, headers=headers)
    response.headers["ETag"] = etag
    if extra_headers:
        for key, value in extra_headers.items():
            response.headers.setdefault(key, value)
    return model


def _prepare_csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, default=str)
        except TypeError:
            return str(value)
    return str(value)


def _csv_stream(rows: Iterable[Mapping[str, Any]], fieldnames: Sequence[str]) -> Iterable[str]:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    yield buffer.getvalue()
    buffer.seek(0)
    buffer.truncate(0)

    for row in rows:
        writer.writerow({field: _prepare_csv_value(row.get(field)) for field in fieldnames})
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)


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
    stream = _csv_stream(rows, fieldnames)
    response = StreamingResponse(stream, media_type="text/csv; charset=utf-8")
    response.headers["X-Request-Id"] = request_id
    if cache_control:
        response.headers["Cache-Control"] = cache_control
    if next_cursor:
        response.headers["X-Next-Cursor"] = next_cursor
    if prev_cursor:
        response.headers["X-Prev-Cursor"] = prev_cursor
    response.headers["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response

# Prometheus metrics
_METRICS_ENABLED = os.getenv("AURUM_API_METRICS_ENABLED", "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
_METRICS_PATH = os.getenv("AURUM_API_METRICS_PATH", "/metrics").strip() or "/metrics"
if not _METRICS_PATH.startswith("/"):
    _METRICS_PATH = f"/{_METRICS_PATH}"

if _METRICS_ENABLED:
    try:  # pragma: no cover - exercised in integration
        from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
    except ImportError:  # pragma: no cover - best effort logging
        LOGGER.warning(
            "AURUM_API_METRICS_ENABLED is set but prometheus_client is not installed; disabling metrics"
        )
        _METRICS_ENABLED = False
    except Exception:  # pragma: no cover - initialization failure should not break startup
        LOGGER.exception("Failed to initialize Prometheus metrics; disabling")
        _METRICS_ENABLED = False

if _METRICS_ENABLED:
    REQUEST_COUNTER = Counter(
        "aurum_api_requests_total",
        "Total API requests",
        ["method", "path", "status"],
    )
    REQUEST_LATENCY = Histogram(
        "aurum_api_request_duration_seconds",
        "API request duration in seconds",
        ["method", "path"],
    )
    TILE_CACHE_COUNTER = Counter(
        "aurum_drought_tile_cache_total",
        "Drought tile cache lookup results",
        ["endpoint", "result"],
    )
    TILE_FETCH_LATENCY = Histogram(
        "aurum_drought_tile_fetch_seconds",
        "Downstream drought tile/info fetch latency in seconds",
        ["endpoint", "status"],
    )

    @app.middleware("http")
    async def _metrics_middleware(request, call_next):  # type: ignore[no-redef]
        request_path = request.url.path
        if request_path == _METRICS_PATH:
            return await call_next(request)

        method = request.method
        route = request.scope.get("route") if hasattr(request, "scope") else None
        path_template = getattr(route, "path", request_path)
        start_time = time.perf_counter()
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        finally:
            duration = time.perf_counter() - start_time
            REQUEST_LATENCY.labels(method=method, path=path_template).observe(duration)
            REQUEST_COUNTER.labels(method=method, path=path_template, status=status_code).inc()

    @app.get(_METRICS_PATH)
    def metrics():
        data = generate_latest()
        return Response(content=data, media_type=CONTENT_TYPE_LATEST)
else:
    REQUEST_COUNTER = None
    REQUEST_LATENCY = None
    TILE_CACHE_COUNTER = None
    TILE_FETCH_LATENCY = None


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
    "run_id",
]
SCENARIO_METRIC_CURSOR_FIELDS = ["metric", "tenor_label", "curve_key"]


METADATA_CACHE_TTL = int(os.getenv("AURUM_API_METADATA_REDIS_TTL", "300") or 300)
SCENARIO_OUTPUT_CACHE_TTL = int(os.getenv("AURUM_API_SCENARIO_OUTPUT_TTL", "60") or 60)
SCENARIO_METRIC_CACHE_TTL = int(os.getenv("AURUM_API_SCENARIO_METRICS_TTL", "60") or 60)

CURVE_MAX_LIMIT = int(os.getenv("AURUM_API_CURVE_MAX_LIMIT", "500") or 500)
SCENARIO_OUTPUT_MAX_LIMIT = int(os.getenv("AURUM_API_SCENARIO_OUTPUT_MAX_LIMIT", "500") or 500)
SCENARIO_METRIC_MAX_LIMIT = int(os.getenv("AURUM_API_SCENARIO_METRIC_MAX_LIMIT", "500") or 500)
CURVE_CACHE_TTL = int(os.getenv("AURUM_API_CURVE_TTL", "120") or 120)
CURVE_DIFF_CACHE_TTL = int(os.getenv("AURUM_API_CURVE_DIFF_TTL", "120") or 120)
CURVE_STRIP_CACHE_TTL = int(os.getenv("AURUM_API_CURVE_STRIP_TTL", "120") or 120)

EIA_SERIES_CURSOR_FIELDS = ["series_id", "period_start", "period"]
EIA_SERIES_MAX_LIMIT = int(os.getenv("AURUM_API_EIA_SERIES_MAX_LIMIT", "1000") or 1000)
EIA_SERIES_CACHE_TTL = int(os.getenv("AURUM_API_EIA_SERIES_TTL", "120") or 120)
EIA_SERIES_DIMENSIONS_CACHE_TTL = int(os.getenv("AURUM_API_EIA_SERIES_DIMENSIONS_TTL", "300") or 300)

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
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant
    if explicit:
        return explicit
    raise HTTPException(status_code=400, detail="tenant_id is required")


def _resolve_tenant_optional(request: Request, explicit: Optional[str]) -> Optional[str]:
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant
    return explicit


def _extract_cursor_payload(row: Dict[str, Any], fields: list[str]) -> Dict[str, Any]:
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


def _persist_ppa_valuation_records(
    *,
    ppa_contract_id: str,
    scenario_id: str,
    tenant_id: Optional[str],
    asof_date: date,
    metrics: List[Dict[str, Any]],
) -> None:
    if os.getenv("AURUM_API_PPA_WRITE_ENABLED", "1").lower() not in {"1", "true", "yes", "on"}:
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


@app.get("/v1/curves", response_model=CurveResponse)
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
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
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
    *,
    response: Response,
) -> CurveResponse:
    request_id = _current_request_id()
    trino_cfg = TrinoConfig.from_env()
    base_cache_cfg = CacheConfig.from_env()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=CURVE_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
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
        next_cursor = _encode_cursor(next_payload)

    prev_cursor_value = None
    if rows:
        if descending:
            if cursor_before and more:
                prev_payload = _extract_cursor_payload(rows[0], CURVE_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})
        else:
            if prev_token or forward_token or effective_offset > 0:
                prev_payload = _extract_cursor_payload(rows[0], CURVE_CURSOR_FIELDS)
                prev_cursor_value = _encode_cursor({"before": prev_payload})

    if format.lower() == "csv":
        cache_header = f"public, max-age={CURVE_CACHE_TTL}"
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
        return _csv_response(
            request_id,
            rows,
            fieldnames,
            filename="curves.csv",
            cache_control=cache_header,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        )

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
        extra_headers={"Cache-Control": f"public, max-age={CURVE_CACHE_TTL}"},
    )


@app.get("/v1/curves/diff", response_model=CurveDiffResponse)
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
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0, description="Offset for pagination (use 'cursor' for stability)"),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(
        None,
        description="Alias for 'cursor' to resume iteration from a previously returned next_cursor",
    ),
    *,
    response: Response,
) -> CurveDiffResponse:
    request_id = _current_request_id()
    trino_cfg = TrinoConfig.from_env()
    base_cache_cfg = CacheConfig.from_env()
    cache_cfg = replace(base_cache_cfg, ttl_seconds=CURVE_DIFF_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    cursor_after: Optional[dict] = None
    effective_cursor = cursor or since_cursor
    if effective_cursor:
        payload = _decode_cursor(effective_cursor)
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
        extra_headers={"Cache-Control": f"public, max-age={CURVE_DIFF_CACHE_TTL}"},
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
    *,
    request: Request,
    response: Response,
) -> DimensionsResponse:
    request_id = _current_request_id()
    trino_cfg = TrinoConfig.from_env()
    base_cache = CacheConfig.from_env()
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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


# --- Units metadata ---


@app.get(
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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


@app.get(
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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


# --- Calendars metadata ---


@app.get(
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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


@app.get(
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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


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
        extra_headers={"Cache-Control": f"public, max-age={METADATA_CACHE_TTL}"},
    )


# --- EIA Catalog metadata ---


@app.get(
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
    limit: int = Query(500, ge=1, le=2000),
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
    )
    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@app.get(
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
    limit: int = Query(500, ge=1, le=2000),
    *,
    request: Request,
    response: Response,
) -> IsoLmpAggregateResponse:
    request_id = _current_request_id()
    rows, elapsed = service.query_iso_lmp_hourly(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        limit=limit,
    )
    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@app.get(
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
    limit: int = Query(500, ge=1, le=2000),
    *,
    request: Request,
    response: Response,
) -> IsoLmpAggregateResponse:
    request_id = _current_request_id()
    rows, elapsed = service.query_iso_lmp_daily(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        limit=limit,
    )
    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)


@app.get(
    "/v1/iso/lmp/negative",
    response_model=IsoLmpResponse,
    tags=["ISO"],
    summary="Recent negative ISO LMP events",
    description="Return the most negative ISO LMP observations across the last seven days.",
)
def iso_lmp_negative(
    iso_code: Optional[str] = Query(None, description="Filter by ISO code"),
    market: Optional[str] = Query(None, description="Filter by market"),
    limit: int = Query(200, ge=1, le=1000),
    *,
    request: Request,
    response: Response,
) -> IsoLmpResponse:
    request_id = _current_request_id()
    rows, elapsed = service.query_iso_lmp_negative(
        iso_code=iso_code,
        market=market,
        limit=limit,
    )
    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed)), data=data)
    return _respond_with_etag(model, request, response)



@app.get(
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
    return _respond_with_etag(model, request, response, extra_headers={"Cache-Control": f"public, max-age={_INMEM_TTL}"})


@app.get(
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
    return _respond_with_etag(model, request, response, extra_headers={"Cache-Control": f"public, max-age={_INMEM_TTL}"})


# --- EIA Series data ---


@app.get(
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
    limit: int = Query(200, ge=1, le=2000),
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
    trino_cfg = TrinoConfig.from_env()
    base_cache_cfg = CacheConfig.from_env()
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
    headers = {"Cache-Control": f"public, max-age={EIA_SERIES_CACHE_TTL}"}
    return _respond_with_etag(model, request, response, extra_headers=headers)


@app.get(
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
    trino_cfg = TrinoConfig.from_env()
    base_cache_cfg = CacheConfig.from_env()
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
    headers = {"Cache-Control": f"public, max-age={EIA_SERIES_DIMENSIONS_CACHE_TTL}"}
    return _respond_with_etag(model, request, response, extra_headers=headers)


@app.get("/v1/curves/strips", response_model=CurveResponse)
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
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(
        None,
        description="Alias for 'cursor' to resume iteration from a previously returned next_cursor",
    ),
    *,
    response: Response,
) -> CurveResponse:
    request_id = _current_request_id()
    trino_cfg = TrinoConfig.from_env()
    base_cache_cfg = CacheConfig.from_env()
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
        extra_headers={"Cache-Control": f"public, max-age={CURVE_STRIP_CACHE_TTL}"},
    )


__all__ = ["app"]


# --- Scenario endpoints (stubbed service behavior for now) ---


def _scenario_record_to_data(record) -> ScenarioData:
    return ScenarioData(
        scenario_id=record.id,
        tenant_id=record.tenant_id,
        name=record.name,
        description=record.description,
        status=record.status,
        assumptions=record.assumptions,
        created_at=record.created_at.isoformat() if record.created_at else None,
    )


def _scenario_run_to_data(run) -> ScenarioRunData:
    created_at = None
    if isinstance(run.created_at, datetime):
        created_at = run.created_at.isoformat()
    return ScenarioRunData(
        run_id=run.run_id,
        scenario_id=run.scenario_id,
        state=run.state,
        code_version=run.code_version,
        seed=run.seed,
        created_at=created_at,
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


@app.get("/v1/scenarios", response_model=ScenarioListResponse)
def list_scenarios(
    request: Request,
    response: Response,
    status: Optional[str] = Query(None, pattern="^[A-Z_]+$", description="Optional scenario status filter"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
) -> ScenarioListResponse:
    tenant_id = _resolve_tenant_optional(request, None)
    request_id = _current_request_id()
    start = time.perf_counter()
    effective_offset = offset
    cursor_token = cursor or since_cursor
    if cursor_token:
        payload = _decode_cursor(cursor_token)
        effective_offset, _cursor_after = _normalise_cursor_input(payload)

    records = ScenarioStore.list_scenarios(
        tenant_id=tenant_id,
        status=status,
        limit=limit,
        offset=effective_offset,
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


@app.post("/v1/scenarios", response_model=ScenarioResponse, status_code=201)
def create_scenario(payload: CreateScenarioRequest, request: Request) -> ScenarioResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, payload.tenant_id)
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


@app.get("/v1/scenarios/{scenario_id}", response_model=ScenarioResponse)
def get_scenario(scenario_id: str, request: Request, response: Response) -> ScenarioResponse:
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


@app.delete("/v1/scenarios/{scenario_id}", status_code=204)
def delete_scenario(scenario_id: str, request: Request) -> Response:
    tenant_id = _resolve_tenant(request, None)
    deleted = ScenarioStore.delete_scenario(scenario_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Scenario not found")
    return Response(status_code=204)


@app.get("/v1/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
def list_scenario_runs(
    scenario_id: str,
    request: Request,
    response: Response,
    state: Optional[str] = Query(None, pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)$"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
) -> ScenarioRunListResponse:
    tenant_id = _resolve_tenant_optional(request, None)
    request_id = _current_request_id()
    start = time.perf_counter()
    effective_offset = offset
    cursor_token = cursor or since_cursor
    if cursor_token:
        payload = _decode_cursor(cursor_token)
        effective_offset, _cursor_after = _normalise_cursor_input(payload)

    runs = ScenarioStore.list_runs(
        tenant_id=tenant_id,
        scenario_id=scenario_id,
        state=state,
        limit=limit,
        offset=effective_offset,
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


@app.post("/v1/scenarios/{scenario_id}/run", response_model=ScenarioRunResponse, status_code=202)
def run_scenario(
    scenario_id: str,
    request: Request,
    options: ScenarioRunOptions | None = None,
) -> ScenarioRunResponse:
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
    )
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@app.get("/v1/scenarios/{scenario_id}/runs/{run_id}", response_model=ScenarioRunResponse)
def get_scenario_run(scenario_id: str, run_id: str, request: Request, response: Response) -> ScenarioRunResponse:
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


@app.post("/v1/scenarios/runs/{run_id}/state", response_model=ScenarioRunResponse)
def update_scenario_run_state(
    run_id: str,
    request: Request,
    state: str = Query(..., pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)$"),
) -> ScenarioRunResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.update_run_state(run_id, state=state, tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@app.post("/v1/scenarios/runs/{run_id}/cancel", response_model=ScenarioRunResponse)
def cancel_scenario_run(run_id: str, request: Request) -> ScenarioRunResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant_optional(request, None)
    run = ScenarioStore.update_run_state(run_id, state="CANCELLED", tenant_id=tenant_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Scenario run not found")
    return ScenarioRunResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_scenario_run_to_data(run),
    )


@app.post(
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
    service.invalidate_scenario_outputs_cache(CacheConfig.from_env(), effective_tenant, scenario_id)
    return Response(status_code=204)


@app.get("/v1/ppa/contracts", response_model=PpaContractListResponse)
def list_ppa_contracts(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
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


@app.post("/v1/ppa/contracts", response_model=PpaContractResponse, status_code=201)
def create_ppa_contract(
    payload: PpaContractCreate,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> PpaContractResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, None)
    _require_admin(principal)
    record = ScenarioStore.create_ppa_contract(
        tenant_id=tenant_id,
        instrument_id=payload.instrument_id,
        terms=payload.terms,
    )
    return PpaContractResponse(
        meta=Meta(request_id=request_id, query_time_ms=0),
        data=_ppa_contract_to_model(record),
    )


@app.get("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
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


@app.patch("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
def update_ppa_contract(
    contract_id: str,
    payload: PpaContractUpdate,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> PpaContractResponse:
    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, None)
    _require_admin(principal)
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


@app.delete("/v1/ppa/contracts/{contract_id}", status_code=204)
def delete_ppa_contract(
    contract_id: str,
    request: Request,
    principal: Dict[str, Any] | None = Depends(_get_principal),
) -> Response:
    tenant_id = _resolve_tenant(request, None)
    _require_admin(principal)
    deleted = ScenarioStore.delete_ppa_contract(contract_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    return Response(status_code=204)


@app.get("/v1/ppa/contracts/{contract_id}/valuations", response_model=PpaValuationListResponse)
def list_ppa_contract_valuations(
    contract_id: str,
    request: Request,
    response: Response,
    scenario_id: Optional[str] = Query(None, description="Filter by scenario identifier"),
    metric: Optional[str] = Query(None, description="Filter by valuation metric name"),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
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
    trino_cfg = TrinoConfig.from_env()
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


@app.post("/v1/ppa/valuate", response_model=PpaValuationResponse)
def valuate_ppa(payload: PpaValuationRequest, request: Request) -> PpaValuationResponse:
    request_id = _current_request_id()
    if not payload.scenario_id:
        raise HTTPException(status_code=400, detail="scenario_id is required for valuation")
    tenant_id = _resolve_tenant(request, None)
    contract = ScenarioStore.get_ppa_contract(payload.ppa_contract_id, tenant_id=tenant_id)
    if contract is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    trino_cfg = TrinoConfig.from_env()

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


@app.get("/v1/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputResponse)
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
    limit: int = Query(200, ge=1, le=1000),
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
    if not _scenario_outputs_enabled():
        raise HTTPException(status_code=503, detail="Scenario outputs not enabled")

    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, tenant_id_param)
    trino_cfg = TrinoConfig.from_env()
    base_cache = CacheConfig.from_env()
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
        extra_headers={"Cache-Control": f"public, max-age={SCENARIO_OUTPUT_CACHE_TTL}"},
    )


@app.get("/v1/scenarios/{scenario_id}/metrics/latest", response_model=ScenarioMetricLatestResponse)
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
    limit: int = Query(200, ge=1, le=1000),
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
    if not _scenario_outputs_enabled():
        raise HTTPException(status_code=503, detail="Scenario outputs not enabled")

    request_id = _current_request_id()
    tenant_id = _resolve_tenant(request, tenant_id_param)
    trino_cfg = TrinoConfig.from_env()
    base_cache = CacheConfig.from_env()
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
        extra_headers={"Cache-Control": f"public, max-age={SCENARIO_METRIC_CACHE_TTL}"},
    )


@app.get("/v1/drought/dimensions", response_model=DroughtDimensionsResponse)
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


@app.get("/v1/drought/indices", response_model=DroughtIndexResponse)
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
    limit: int = Query(250, ge=1, le=5000),
) -> DroughtIndexResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = TrinoConfig.from_env()
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


@app.get("/v1/drought/usdm", response_model=DroughtUsdmResponse)
def get_drought_usdm(
    region: str | None = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: str | None = Query(None),
    region_id: str | None = Query(None),
    start: date | None = Query(None),
    end: date | None = Query(None),
    limit: int = Query(250, ge=1, le=5000),
) -> DroughtUsdmResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = TrinoConfig.from_env()
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


@app.get("/v1/drought/layers", response_model=DroughtVectorResponse)
def get_drought_layers(
    layer: str | None = Query(None, description="Layer key (qpf, ahps_flood, aqi, wildfire)."),
    region: str | None = Query(None, description="Region specifier REGION_TYPE:REGION_ID."),
    region_type: str | None = Query(None),
    region_id: str | None = Query(None),
    start: datetime | None = Query(None),
    end: datetime | None = Query(None),
    limit: int = Query(250, ge=1, le=5000),
) -> DroughtVectorResponse:
    parsed_region_type = region_type
    parsed_region_id = region_id
    if region:
        parsed_region_type, parsed_region_id = _parse_region_param(region)
    trino_cfg = TrinoConfig.from_env()
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


@app.get(
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
        with httpx.Client(timeout=20.0) as client:
            response = client.get(tile_url)
    except httpx.HTTPError as exc:
        _observe_tile_fetch("tile", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=502, detail=f"tile_fetch_failed: {exc}") from exc

    duration = time.perf_counter() - start_fetch
    if response.status_code != 200:
        _observe_tile_fetch("tile", "error", duration)
        raise HTTPException(status_code=response.status_code, detail="tile_fetch_failed")

    _observe_tile_fetch("tile", "success", duration)

    content = response.content
    if cache_client is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, content)
        except Exception:
            pass
    media_type = response.headers.get("Content-Type", "image/png")
    return Response(content=content, media_type=media_type)


@app.get("/v1/drought/info/{dataset}/{index}/{timescale}", response_model=DroughtInfoResponse)
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
        with httpx.Client(timeout=15.0) as client:
            response = client.get(info_url)
    except httpx.HTTPError as exc:
        _observe_tile_fetch("info", "error", time.perf_counter() - start_fetch)
        raise HTTPException(status_code=502, detail=f"info_fetch_failed: {exc}") from exc

    duration = time.perf_counter() - start_fetch
    if response.status_code != 200:
        _observe_tile_fetch("info", "error", duration)
        raise HTTPException(status_code=response.status_code, detail="info_fetch_failed")

    _observe_tile_fetch("info", "success", duration)

    payload = response.json()
    if cache_client is not None:
        try:
            cache_client.setex(cache_key, _TILE_CACHE_CFG.ttl_seconds, response.text)
        except Exception:
            pass

    meta = Meta(request_id=_current_request_id(), query_time_ms=0)
    return DroughtInfoResponse(meta=meta, data=payload)
