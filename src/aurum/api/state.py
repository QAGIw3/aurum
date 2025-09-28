"""Deprecated global state accessors.

This module previously exposed global singletons used across the API. As part
of the "Eliminate Global State" refactor, all consumers must migrate to
dependency-injected access via FastAPI dependencies.

What to use instead:
- Import `AurumSettings` via FastAPI dependency using `aurum.api.deps.get_settings`
- Access cache via FastAPI dependency or via container service providers
- Avoid module-level initialization side effects
"""

from __future__ import annotations

import warnings

from fastapi import Request

from aurum.core.settings import AurumSettings
from aurum.api.deps import get_settings as request_settings  # FastAPI dependency


def configure(_settings: AurumSettings) -> None:  # pragma: no cover - maintained for backward imports
    """No-op placeholder retained for import-compatibility.

    Use application factory to attach settings to `app.state.settings` instead.
    """
    return None


def get_settings() -> AurumSettings:
    """Deprecated. Use `aurum.api.deps.get_settings` FastAPI dependency instead."""
    raise RuntimeError(
        "aurum.api.state.get_settings is removed. Inject settings via FastAPI dependency 'aurum.api.deps.get_settings'."
    )


def request_settings_dependency(request: Request) -> AurumSettings:
    """Alias to the canonical FastAPI settings dependency for transitional imports."""
    from aurum.api.deps import get_settings as _dep
    return _dep(request)


__all__ = ["configure", "get_settings", "request_settings_dependency"]
