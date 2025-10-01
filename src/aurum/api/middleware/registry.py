"""Legacy middleware registry shim.

The enhanced registry now owns middleware orchestration.  This module keeps the
historical ``apply_middleware_stack`` helper alive for callers that have not yet
migrated.
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from .enhanced_registry import get_enhanced_middleware_registry

if TYPE_CHECKING:  # pragma: no cover - import-time helpers for type checkers
    from fastapi import FastAPI
    from starlette.types import ASGIApp
    from aurum.core import AurumSettings


def apply_middleware_stack(app: "FastAPI", settings: "AurumSettings") -> "ASGIApp":
    """Delegate to the enhanced middleware registry."""
    warnings.warn(
        "aurum.api.middleware.registry.apply_middleware_stack is deprecated; "
        "use aurum.api.middleware.enhanced_registry instead",
        DeprecationWarning,
        stacklevel=2,
    )
    registry = get_enhanced_middleware_registry()
    return registry.apply_middleware_stack(app, settings)


__all__ = ["apply_middleware_stack"]
