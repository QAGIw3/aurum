from __future__ import annotations

"""Shared utilities and configuration helpers for legacy Aurum API routers."""

import hashlib
import json
import logging
import threading
import time
import uuid
from pathlib import Path as FilePath
from typing import Any, Dict, Optional, Sequence, Set

from fastapi import HTTPException, Request, Response

from aurum.core import AurumSettings
from aurum.telemetry.context import get_request_id

from . import service
from .cache.cache import CacheManager
from .config import CacheConfig, TrinoConfig
from .http import respond_with_etag as http_respond_with_etag
from .state import get_settings
from aurum.observability import metrics as observability_metrics
from aurum.observability.metrics import (
    METRICS_MIDDLEWARE,
    METRICS_PATH,
    PROMETHEUS_AVAILABLE,
    TILE_CACHE_COUNTER,
    TILE_FETCH_LATENCY,
)

try:  # pragma: no cover - optional dependency
    from aurum.drought.catalog import load_catalog, DroughtCatalog
except ModuleNotFoundError:  # pragma: no cover - drought features optional
    load_catalog = None  # type: ignore[assignment]
    DroughtCatalog = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


# --- Global configuration state ------------------------------------------------

_CATALOG_PATH = FilePath(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"

_DROUGHT_CATALOG: DroughtCatalog | None = None
_TILE_CACHE_CFG: CacheConfig | None = None
_TILE_CACHE: Any | None = None

_CACHE_MANAGER: CacheManager | None = None
_METADATA_FALLBACK_CACHE: dict[str, tuple[float, Any]] = {}
_METADATA_FALLBACK_LOCK = threading.Lock()
_INMEM_TTL = 60

ADMIN_GROUPS: Set[str] = set()

METADATA_CACHE_TTL = 300
CURVE_MAX_LIMIT = 500
CURVE_CACHE_TTL = 120
CURVE_DIFF_CACHE_TTL = 120
CURVE_STRIP_CACHE_TTL = 120
EIA_SERIES_MAX_LIMIT = 1000
EIA_SERIES_CACHE_TTL = 120
EIA_SERIES_DIMENSIONS_CACHE_TTL = 300


# --- Settings helpers ---------------------------------------------------------

def _settings() -> AurumSettings:
    """Return the globally configured settings instance."""
    return get_settings()


def _trino_config() -> TrinoConfig:
    """Build a Trino configuration from current settings."""
    return TrinoConfig.from_settings(_settings())


def _cache_config(*, ttl_override: int | None = None) -> CacheConfig:
    """Build a cache configuration from current settings."""
    return CacheConfig.from_settings(_settings(), ttl_override=ttl_override)


# --- Route configuration ------------------------------------------------------

def _load_admin_groups(settings: AurumSettings) -> Set[str]:
    auth_cfg = getattr(settings, "auth", None)
    if auth_cfg is None:
        return set()
    if getattr(auth_cfg, "disabled", False):
        return set()
    raw_groups = getattr(auth_cfg, "admin_groups", None)
    if not raw_groups:
        return set()
    return {str(item).strip().lower() for item in raw_groups if str(item).strip()}


def configure_routes(settings: AurumSettings) -> None:
    """Initialise shared route configuration from settings."""

    global ADMIN_GROUPS, _TILE_CACHE_CFG, _TILE_CACHE, _INMEM_TTL
    global METADATA_CACHE_TTL
    global CURVE_MAX_LIMIT, CURVE_CACHE_TTL, CURVE_DIFF_CACHE_TTL, CURVE_STRIP_CACHE_TTL
    global EIA_SERIES_CACHE_TTL, EIA_SERIES_DIMENSIONS_CACHE_TTL, EIA_SERIES_MAX_LIMIT

    ADMIN_GROUPS = _load_admin_groups(settings)
    _TILE_CACHE_CFG = CacheConfig.from_settings(settings)
    _TILE_CACHE = None
    api_cfg = getattr(settings, "api", None)
    cache_cfg = getattr(api_cfg, "cache", None) if api_cfg is not None else None
    pagination_cfg = getattr(settings, "pagination", None)

    if cache_cfg is not None:
        _INMEM_TTL = getattr(cache_cfg, "in_memory_ttl", _INMEM_TTL)
        METADATA_CACHE_TTL = getattr(cache_cfg, "metadata_ttl", METADATA_CACHE_TTL)
        CURVE_CACHE_TTL = getattr(cache_cfg, "curve_ttl", CURVE_CACHE_TTL)
        CURVE_DIFF_CACHE_TTL = getattr(cache_cfg, "curve_diff_ttl", CURVE_DIFF_CACHE_TTL)
        CURVE_STRIP_CACHE_TTL = getattr(cache_cfg, "curve_strip_ttl", CURVE_STRIP_CACHE_TTL)
        EIA_SERIES_CACHE_TTL = getattr(cache_cfg, "eia_series_ttl", EIA_SERIES_CACHE_TTL)
        EIA_SERIES_DIMENSIONS_CACHE_TTL = getattr(
            cache_cfg,
            "eia_series_dimensions_ttl",
            EIA_SERIES_DIMENSIONS_CACHE_TTL,
        )

    if pagination_cfg is not None:
        CURVE_MAX_LIMIT = getattr(pagination_cfg, "curves_max_limit", CURVE_MAX_LIMIT)
        EIA_SERIES_MAX_LIMIT = getattr(
            pagination_cfg,
            "eia_series_max_limit",
            EIA_SERIES_MAX_LIMIT,
        )

    metrics_cfg = getattr(api_cfg, "metrics", None) if api_cfg is not None else None
    if (
        metrics_cfg is not None
        and getattr(metrics_cfg, "enabled", False)
        and PROMETHEUS_AVAILABLE
        and METRICS_MIDDLEWARE is not None
    ):
        path = getattr(metrics_cfg, "path", "/metrics") or "/metrics"
        if not path.startswith("/"):
            path = f"/{path}"
        observability_metrics.METRICS_PATH = path
    else:
        observability_metrics.METRICS_PATH = METRICS_PATH

    with _METADATA_FALLBACK_LOCK:
        _METADATA_FALLBACK_CACHE.clear()


# --- Principal / admin helpers ------------------------------------------------

def _get_principal(request: Request) -> Dict[str, Any] | None:
    """Extract the principal dictionary from the request state if available."""
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
    if any(group in normalized for group in ADMIN_GROUPS):
        return True

    claims = principal.get("claims") or {}
    scopes = claims.get("scope", "")
    if isinstance(scopes, str):
        scope_list = scopes.split()
    elif isinstance(scopes, list):
        scope_list = [str(scope) for scope in scopes]
    else:
        scope_list = []

    admin_scopes = {
        "admin",
        "admin:read",
        "admin:write",
        "aurum:admin",
        "admin:feature_flags",
        "admin:rate_limits",
        "admin:trino",
    }
    return any(scope in scope_list for scope in admin_scopes)


def _require_admin(principal: Dict[str, Any] | None) -> None:
    if not _is_admin(principal):
        raise HTTPException(status_code=403, detail="admin_required")


def _parse_region_param(region: str) -> tuple[str, str]:
    region = region.strip()
    if not region or ":" not in region:
        raise HTTPException(status_code=400, detail="invalid_region")
    region_type, region_id = region.split(":", 1)
    region_type = region_type.strip().upper()
    region_id = region_id.strip()
    if not region_type or not region_id:
        raise HTTPException(status_code=400, detail="invalid_region")
    return region_type, region_id


# --- Tenant helpers -----------------------------------------------------------

def _resolve_tenant(request: Request, explicit: Optional[str]) -> str:
    if tenant := getattr(request.state, "tenant", None):
        return tenant
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
    if tenant := getattr(request.state, "tenant", None):
        return tenant
    principal = getattr(request.state, "principal", {}) or {}
    if tenant := principal.get("tenant"):
        return tenant
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant
    return explicit


# --- Tile helpers -------------------------------------------------------------

def _drought_catalog() -> DroughtCatalog:
    if load_catalog is None or DroughtCatalog is None:  # pragma: no cover - optional dependency
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
    except Exception:  # pragma: no cover - cache is optional
        client = None
    _TILE_CACHE = client
    return _TILE_CACHE


def _record_tile_cache_metric(endpoint: str, result: str) -> None:
    if TILE_CACHE_COUNTER is None:  # pragma: no cover - metrics disabled
        return
    try:
        TILE_CACHE_COUNTER.labels(endpoint=endpoint, result=result).inc()
    except Exception:  # pragma: no cover - defensive guard
        LOGGER.debug("Failed to record tile cache metric", exc_info=True)


def _observe_tile_fetch(endpoint: str, status: str, duration: float) -> None:
    if TILE_FETCH_LATENCY is None:  # pragma: no cover - metrics disabled
        return
    try:
        TILE_FETCH_LATENCY.labels(endpoint=endpoint, status=status).observe(duration)
    except Exception:  # pragma: no cover - defensive guard
        LOGGER.debug("Failed to record tile fetch latency", exc_info=True)


# --- Metadata cache helpers ---------------------------------------------------

def set_cache_manager(manager: CacheManager | None) -> None:
    """Register the active CacheManager for synchronous helpers."""
    global _CACHE_MANAGER
    _CACHE_MANAGER = manager


def _metadata_fallback_get(key: str) -> Any | None:
    now = time.time()
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
        _METADATA_FALLBACK_CACHE[key] = (time.time() + ttl, value)


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
            keys = [key for key in list(_METADATA_FALLBACK_CACHE) if key.startswith(prefix)]
            for key in keys:
                _METADATA_FALLBACK_CACHE.pop(key, None)
            removed += len(keys)
    return removed


# --- ETag helpers -------------------------------------------------------------

def _current_request_id() -> str:
    existing = get_request_id()
    if existing:
        return existing
    return str(uuid.uuid4())


def _generate_etag(data: Dict[str, Any]) -> str:
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


__all__ = [
    "configure_routes",
    "METADATA_CACHE_TTL",
    "CURVE_MAX_LIMIT",
    "CURVE_CACHE_TTL",
    "CURVE_DIFF_CACHE_TTL",
    "CURVE_STRIP_CACHE_TTL",
    "EIA_SERIES_CACHE_TTL",
    "EIA_SERIES_DIMENSIONS_CACHE_TTL",
    "EIA_SERIES_MAX_LIMIT",
    "_settings",
    "_trino_config",
    "_cache_config",
    "_current_request_id",
    "_get_principal",
    "_require_admin",
    "_parse_region_param",
    "_resolve_tenant",
    "_resolve_tenant_optional",
    "_drought_catalog",
    "_tile_cache",
    "_record_tile_cache_metric",
    "_observe_tile_fetch",
    "_metadata_cache_get_or_set",
    "_metadata_cache_purge",
    "_generate_etag",
    "_respond_with_etag",
    "set_cache_manager",
    "METRICS_MIDDLEWARE",
]
