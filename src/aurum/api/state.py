"""Runtime state holders for the Aurum API.

The API historically maintained its own global settings accessor. This module
now proxies to ``aurum.core.settings`` to ensure there is a single source of
truth for configuration while retaining backwards-compatible helpers.
"""

from __future__ import annotations

import warnings

from fastapi import Request

from aurum.core.settings import AurumSettings, configure_settings, get_settings as _core_get_settings


def configure(settings: AurumSettings) -> None:
    """Configure global settings via the core settings module."""

    configure_settings(settings)


def get_settings() -> AurumSettings:
    """Proxy to the core ``get_settings`` helper (deprecated)."""

    warnings.warn(
        "aurum.api.state.get_settings is deprecated; import get_settings from "
        "aurum.core.settings instead",
        DeprecationWarning,
        stacklevel=2,
    )
    return _core_get_settings()


def request_settings(request: Request) -> AurumSettings:
    """FastAPI dependency returning settings stored on the app state."""

    settings = getattr(request.app.state, "settings", None)
    if isinstance(settings, AurumSettings):
        return settings
    return _core_get_settings()


__all__ = ["configure", "get_settings", "request_settings"]
