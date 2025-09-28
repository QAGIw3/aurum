"""Aurum API v2 - Enhanced version with improved features.

This module contains the v2 API implementations with:
- Cursor-only pagination
- RFC 7807 compliant error responses
- Enhanced ETag support
- Improved observability
- Better error handling
- Consistent response formats
- Tenant context enforcement
- Link headers for navigation
- Rate limiting headers
"""

from __future__ import annotations

import os as _os

from fastapi import APIRouter, Depends
from importlib import import_module as _import_module

from ..deps import require_tenant_id

# Explicit API surface for v2 submodules; lazily imported to avoid heavy deps
__all__ = [
    "scenarios",
    "curves",
    "metadata",
    "iso",
    "eia",
    "ppa",
    "drought",
    "admin",
    "forecasting",
    "auto_reforecast",
    "renewables",
    "model_registry",
    "explainability",
    "signals",
    "bidding",
    "carbon_rec",
    "risk_engine",
    "stress_testing",
    "regulatory_tracker",
    "plugin_system",
    "developer_workspace",
    "performance_monitoring",
    "plugin_marketplace",
    "dbt_management",
]

def _attach_tenant_dependency(router: APIRouter) -> None:
    if getattr(router, "_aurum_v2_tenant_dependency", False):
        return
    dependency = Depends(require_tenant_id)
    router.dependencies = list(router.dependencies or []) + [dependency]
    setattr(router, "_aurum_v2_tenant_dependency", True)

def __getattr__(name: str):
    if name in __all__:
        module = _import_module(f"{__name__}.{name}")
        router = getattr(module, "router", None)
        if isinstance(router, APIRouter):
            _attach_tenant_dependency(router)
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
