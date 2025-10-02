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

from aurum.api.services import admin_service as _admin_service
from aurum.api.services import metadata_service as _metadata_service
from aurum.api.services import iso_service as _iso_service

from .cache.cache import CacheManager
from .cache.consolidated_manager import get_unified_cache_manager
from .config import CacheConfig, TrinoConfig
from .http import respond_with_etag as http_respond_with_etag
from aurum.core.settings import get_settings as _core_get_settings
from aurum.observability import metrics as observability_metrics
try:
    from aurum.observability.metrics import (
        METRICS_MIDDLEWARE,
        METRICS_PATH,
        PROMETHEUS_AVAILABLE,
        TILE_CACHE_COUNTER,
        TILE_FETCH_LATENCY,
    )
except Exception:  # pragma: no cover - optional metrics symbols
    METRICS_MIDDLEWARE = None  # type: ignore[assignment]
    METRICS_PATH = "/metrics"  # type: ignore[assignment]
    PROMETHEUS_AVAILABLE = False  # type: ignore[assignment]
    TILE_CACHE_COUNTER = None  # type: ignore[assignment]
    TILE_FETCH_LATENCY = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from aurum.drought.catalog import load_catalog, DroughtCatalog
except ModuleNotFoundError:  # pragma: no cover - drought features optional
    load_catalog = None  # type: ignore[assignment]
    DroughtCatalog = None  # type: ignore[assignment]


LOGGER = logging.getLogger(__name__)


# --- Global configuration state (being phased out in favor of dependency injection) ---

_CATALOG_PATH = FilePath(__file__).resolve().parents[2] / "config" / "droughtgov_catalog.json"

_DROUGHT_CATALOG: DroughtCatalog | None = None
_TILE_CACHE_CFG: CacheConfig | None = None
_TILE_CACHE: Any | None = None

_CACHE_MANAGER: CacheManager | None = None
_METADATA_CACHE_LOCK = threading.Lock()
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
    return _core_get_settings()


def _trino_config() -> TrinoConfig:
    """Build a Trino configuration from current settings."""
    return TrinoConfig.from_settings(_settings())


def _cache_config(*, ttl_override: int | None = None) -> CacheConfig:
    """Build a cache configuration from current settings."""
    return CacheConfig.from_settings(_settings(), ttl_override=ttl_override)


# --- Route configuration ------------------------------------------------------

def _load_admin_groups(settings: AurumSettings) -> Set[str]:
    """Load admin groups from settings (deprecated - use ApplicationContext instead)."""
    from .container import get_app_context
    app_context = get_app_context()
    app_context.settings = settings
    return app_context.get_admin_groups()


def configure_routes(settings: AurumSettings) -> None:
    """Initialise shared route configuration from settings."""

    # Use application context instead of global variables
    from .container import get_app_context
    app_context = get_app_context()
    app_context.settings = settings

    # Legacy global variables (being phased out)
    global ADMIN_GROUPS, _TILE_CACHE_CFG, _TILE_CACHE, _INMEM_TTL
    global METADATA_CACHE_TTL
    global CURVE_MAX_LIMIT, CURVE_CACHE_TTL, CURVE_DIFF_CACHE_TTL, CURVE_STRIP_CACHE_TTL
    global EIA_SERIES_CACHE_TTL, EIA_SERIES_DIMENSIONS_CACHE_TTL, EIA_SERIES_MAX_LIMIT

    ADMIN_GROUPS = app_context.get_admin_groups()
    # Reset context-managed tile cache so it will be re-initialized using new settings
    try:
        app_context.tile_cache = None
    except Exception:
        pass
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

    cache_manager = get_metadata_cache()
    if cache_manager is not None:
        try:
            cache_manager.clear()
        except Exception:
            LOGGER.warning("metadata_cache_clear_failed", exc_info=True)


# --- Principal / admin helpers ------------------------------------------------

def _get_principal(request: Request) -> Dict[str, Any] | None:
    """Extract the principal dictionary from the request state if available."""
    principal = getattr(request.state, "principal", None)
    if isinstance(principal, dict):
        return principal
    return None


def _is_admin(principal: Dict[str, Any] | None) -> bool:
    if not principal:
        return False

    if not ADMIN_GROUPS:
        return False

    claims = principal.get("claims") or {}

    candidate_groups: Set[str] = set()

    if "groups" in claims and isinstance(claims["groups"], list):
        candidate_groups.update(str(group).lower() for group in claims["groups"] if group)

    if "roles" in claims and isinstance(claims["roles"], list):
        candidate_groups.update(str(role).lower() for role in claims["roles"] if role)

    groups = principal.get("groups") or []
    if isinstance(groups, list):
        candidate_groups.update(str(group).lower() for group in groups if group)
    elif isinstance(groups, str):
        candidate_groups.add(groups.lower())

    if candidate_groups & ADMIN_GROUPS:
        return True

    scopes_claim = claims.get("scope")
    scope_tokens: Set[str] = set()
    if isinstance(scopes_claim, str):
        scope_tokens.update(token.strip().lower() for token in scopes_claim.split() if token.strip())
    elif isinstance(scopes_claim, list):
        scope_tokens.update(str(token).lower() for token in scopes_claim if token)

    admin_scopes = {
        "admin",
        "admin:read",
        "admin:write",
        "aurum:admin",
        "admin:feature_flags",
        "admin:rate_limits",
        "admin:trino",
    }
    return bool(scope_tokens & admin_scopes)


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
    # Use application context instead of module-global cache
    try:
        from .container import get_app_context
        app_context = get_app_context()
    except Exception:
        app_context = None

    if app_context is not None and getattr(app_context, "drought_catalog", None) is not None:
        return app_context.drought_catalog  # type: ignore[return-value]

    catalog = load_catalog(_CATALOG_PATH)
    if app_context is not None:
        try:
            app_context.drought_catalog = catalog
        except Exception:
            pass
    return catalog


def _tile_cache():
    # Use application context instead of module-global cache/config
    try:
        from .container import get_app_context
        app_context = get_app_context()
    except Exception:
        app_context = None

    if app_context is not None and getattr(app_context, "tile_cache", None) is not None:
        return app_context.tile_cache

    cfg = _cache_config()
    try:
        client = service._maybe_redis_client(cfg)
    except Exception:  # pragma: no cover - cache is optional
        client = None

    if app_context is not None:
        try:
            app_context.tile_cache = client
        except Exception:
            pass
    return client


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


def get_metadata_cache() -> CacheManager | None:
    """Return the metadata cache manager for legacy routes."""

    from .container import get_app_context

    app_context = get_app_context()
    cache = getattr(app_context, "metadata_cache", None)
    if cache is not None:
        return cache

    try:
        cache = get_unified_cache_manager()
    except Exception:
        LOGGER.warning("metadata_cache_manager_unavailable", exc_info=True)
        cache = None

    if cache is not None:
        setattr(app_context, "metadata_cache", cache)
    return cache


def invalidate_metadata_cache(prefixes: Sequence[str]) -> int:
    """Invalidate metadata cache entries with the specified prefixes."""

    cache_manager = get_metadata_cache()
    if cache_manager is None:
        return 0

    removed = 0
    for prefix in prefixes:
        try:
            removed += cache_manager.invalidate_pattern(prefix)
        except Exception:
            LOGGER.warning("metadata_cache_invalidate_failed", extra={"prefix": prefix}, exc_info=True)
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
    "_generate_etag",
    "_respond_with_etag",
    "METRICS_MIDDLEWARE",
]
