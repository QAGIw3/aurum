"""FastAPI dependency helpers for settings, cache, and principal context.

These helpers avoid direct globals and provide a consistent way for
route handlers and services to access runtime state.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import HTTPException, Request

from aurum.api.telemetry.context import (
    TenantIdValidationError,
    normalize_tenant_id,
)

from aurum.api.cache.cache import CacheManager
from aurum.api.cache.unified_cache_manager import UnifiedCacheManager, get_unified_cache_manager
from aurum.core import AurumSettings
from aurum.core.settings import get_settings as _core_get_settings


def get_settings(request: Request) -> AurumSettings:
    """Return `AurumSettings` from the app state with fallback to API state.

    Preference order:
    1) request.app.state.settings (if configured by the app factory)
    2) aurum.core.settings.get_settings() (configured during startup)
    """
    settings_state = getattr(getattr(request, "app", None), "state", None)
    if settings_state is not None:
        maybe = getattr(settings_state, "settings", None)
        if isinstance(maybe, AurumSettings):
            return maybe
    return _core_get_settings()


def get_cache_manager(request: Request) -> Optional[CacheManager]:
    """Return the active CacheManager from app state, if present."""
    return getattr(getattr(request, "app", None), "state", None) and getattr(  # type: ignore[return-value]
        request.app.state, "cache_manager", None
    )


def get_unified_cache_manager_dep(request: Request) -> Optional[UnifiedCacheManager]:
    """Return the active UnifiedCacheManager from app state or global instance."""
    # First try to get from app state
    app_state = getattr(getattr(request, "app", None), "state", None)
    if app_state:
        unified_manager = getattr(app_state, "unified_cache_manager", None)
        if unified_manager:
            return unified_manager
    
    # Fall back to global instance
    return get_unified_cache_manager()


def get_principal(request: Request) -> Optional[dict[str, Any]]:
    """Return the authenticated principal from request.state, if any."""
    principal = getattr(getattr(request, "state", None), "principal", None)
    return principal if isinstance(principal, dict) else None


def get_tenant_id(request: Request) -> Optional[str]:
    """Return the normalized tenant identifier when available."""

    # Prefer values injected by middleware or dependencies
    tenant = getattr(getattr(request, "state", None), "tenant", None)
    if tenant is None:
        tenant = getattr(getattr(request, "state", None), "tenant_id", None)

    # Fall back to explicit request hints
    if tenant is None and hasattr(request, "query_params"):
        tenant = request.query_params.get("tenant_id")

    if tenant is None:
        tenant = request.headers.get("x-aurum-tenant")

    try:
        normalized = normalize_tenant_id(tenant)
    except TenantIdValidationError as exc:  # pragma: no cover - validation paths exercised in tests
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if normalized:
        # Ensure downstream code can rely on state for quick access
        request.state.tenant = normalized  # type: ignore[attr-defined]
        request.state.tenant_id = normalized  # type: ignore[attr-defined]

    return normalized


def require_tenant_id(request: Request) -> str:
    """Require a tenant identifier, raising when absent."""

    tenant = get_tenant_id(request)
    if tenant:
        return tenant

    raise HTTPException(status_code=400, detail="Missing tenant context")


__all__ = [
    "get_settings",
    "get_cache_manager",
    "get_unified_cache_manager_dep",
    "get_principal",
    "get_tenant_id",
    "require_tenant_id",
]
