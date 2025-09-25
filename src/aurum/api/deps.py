"""FastAPI dependency helpers for settings, cache, and principal context.

These helpers avoid direct globals and provide a consistent way for
route handlers and services to access runtime state.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import Request

from aurum.api.cache.cache import CacheManager
from aurum.core import AurumSettings
from aurum.api import state as _state


def get_settings(request: Request) -> AurumSettings:
    """Return `AurumSettings` from the app state with fallback to API state.

    Preference order:
    1) request.app.state.settings (if configured by the app factory)
    2) aurum.api.state.get_settings() (configured during startup)
    """
    settings = getattr(getattr(request, "app", None), "state", None)
    if settings is not None:
        maybe = getattr(settings, "settings", None)
        if isinstance(maybe, AurumSettings):
            return maybe
    return _state.get_settings()


def get_cache_manager(request: Request) -> Optional[CacheManager]:
    """Return the active CacheManager from app state, if present."""
    return getattr(getattr(request, "app", None), "state", None) and getattr(  # type: ignore[return-value]
        request.app.state, "cache_manager", None
    )


def get_principal(request: Request) -> Optional[dict[str, Any]]:
    """Return the authenticated principal from request.state, if any."""
    principal = getattr(getattr(request, "state", None), "principal", None)
    return principal if isinstance(principal, dict) else None


def get_tenant_id(request: Request) -> Optional[str]:
    """Return tenant id propagated by auth middleware, if available."""
    tenant = getattr(getattr(request, "state", None), "tenant", None)
    return str(tenant) if tenant is not None else None


__all__ = [
    "get_settings",
    "get_cache_manager",
    "get_principal",
    "get_tenant_id",
]

