"""Runtime state holders for the Aurum API."""
from __future__ import annotations

from typing import Callable

from fastapi import Request

from aurum.core import AurumSettings

_SETTINGS: AurumSettings | None = None


def configure(settings: AurumSettings) -> None:
    """Set the active settings instance for the API."""
    global _SETTINGS
    _SETTINGS = settings


def get_settings() -> AurumSettings:
    """Return the configured settings, raising if unavailable."""
    if _SETTINGS is None:  # pragma: no cover - defensive guard
        raise RuntimeError("AurumSettings have not been configured")
    return _SETTINGS


def request_settings(request: Request) -> AurumSettings:
    """FastAPI dependency returning settings stored on the app state."""
    settings = getattr(request.app.state, "settings", None)
    if isinstance(settings, AurumSettings):
        return settings
    return get_settings()


__all__ = ["configure", "get_settings", "request_settings"]
