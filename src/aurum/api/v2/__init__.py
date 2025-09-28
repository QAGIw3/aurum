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

if _os.getenv("AURUM_API_V2_LIGHT_INIT", "0") == "1":
    __all__: list[str] = []
else:
    from fastapi import APIRouter, Depends

    from ..deps import require_tenant_id

    from . import (  # noqa: WPS319 - intentional grouped imports for registration side-effects
        admin,
        auto_reforecast,
        bidding,
        carbon_rec,
        curves,
        dbt_management,
        developer_workspace,
        drought,
        eia,
        explainability,
        forecasting,
        iso,
        metadata,
        model_registry,
        performance_monitoring,
        plugin_marketplace,
        plugin_system,
        ppa,
        regulatory_tracker,
        renewables,
        risk_engine,
        scenarios,
        signals,
        stress_testing,
    )

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

    for _module in (
        admin,
        auto_reforecast,
        bidding,
        carbon_rec,
        curves,
        dbt_management,
        developer_workspace,
        drought,
        eia,
        explainability,
        forecasting,
        iso,
        metadata,
        model_registry,
        performance_monitoring,
        plugin_marketplace,
        plugin_system,
        ppa,
        regulatory_tracker,
        renewables,
        risk_engine,
        scenarios,
        signals,
        stress_testing,
    ):
        router = getattr(_module, "router", None)
        if isinstance(router, APIRouter):
            _attach_tenant_dependency(router)
