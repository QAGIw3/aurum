"""Database functionality for the Aurum API.

This package lazily exposes routers and helpers to avoid circular imports
during test collection. Modules are imported only on attribute access.
"""

from __future__ import annotations

import importlib
from typing import Any

_LAZY_ATTRS = {
    # Routers
    "performance_router": (".performance", "router"),
    "query_analysis_router": (".query_analysis", "router"),
    "optimization_router": (".optimization", "router"),
    "connections_router": (".connections", "router"),
    "health_router": (".health", "router"),
    "trino_admin_router": (".trino_admin", "router"),
    # Trino client
    "get_trino_client": (".trino_client", "get_trino_client"),
    "get_trino_client_by_catalog": (".trino_client", "get_trino_client_by_catalog"),
    "get_trino_catalog_config": (".trino_client", "get_trino_catalog_config"),
    "configure_trino_catalogs": (".trino_client", "configure_trino_catalogs"),
    "TrinoClientManager": (".trino_client", "TrinoClientManager"),
    # Config
    "TrinoCatalogType": (".config", "TrinoCatalogType"),
    "TrinoAccessLevel": (".config", "TrinoAccessLevel"),
    "TrinoCatalogConfig": (".config", "TrinoCatalogConfig"),
    # Auto reforecast
    "get_auto_reforecast_repository": (".auto_reforecast", "get_auto_reforecast_repository"),
    "get_auto_reforecast_job_repository": (".auto_reforecast", "get_auto_reforecast_job_repository"),
    "AutoReforecastRepository": (".auto_reforecast", "AutoReforecastRepository"),
    "AutoReforecastJobRepository": (".auto_reforecast", "AutoReforecastJobRepository"),
    "AutoReforecastScheduler": (".auto_reforecast_scheduler", "AutoReforecastScheduler"),
    "get_auto_reforecast_scheduler": (".auto_reforecast_scheduler", "get_auto_reforecast_scheduler"),
    # Database monitor
    "get_database_monitor": (".database_monitor", "get_database_monitor"),
    "initialize_database_monitoring": (".database_monitor", "initialize_database_monitoring"),
    "DatabaseMonitor": (".database_monitor", "DatabaseMonitor"),
    "QueryMetrics": (".database_monitor", "QueryMetrics"),
    "QueryPattern": (".database_monitor", "QueryPattern"),
    "OptimizationSuggestion": (".database_monitor", "OptimizationSuggestion"),
    "QueryPerformanceLevel": (".database_monitor", "QueryPerformanceLevel"),
    "OptimizationType": (".database_monitor", "OptimizationType"),
}


def __getattr__(name: str) -> Any:  # pragma: no cover - import indirection
    spec = _LAZY_ATTRS.get(name)
    if not spec:
        raise AttributeError(name)
    module_name, attr = spec
    module = importlib.import_module(module_name, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value


def initialize_trino_catalogs(settings) -> None:
    """Initialize Trino catalogs with proper separation and access control."""
    # Create catalog configurations
    raw_catalog_config = TrinoCatalogConfig.from_settings(
        settings=settings,
        catalog_type=TrinoCatalogType.RAW,
        access_level=TrinoAccessLevel.READ_ONLY,
        lineage_tags=["source=raw", "environment=production", "data_type=external"]
    )

    market_catalog_config = TrinoCatalogConfig.from_settings(
        settings=settings,
        catalog_type=TrinoCatalogType.MARKET,
        access_level=TrinoAccessLevel.READ_WRITE,
        lineage_tags=["source=processed", "environment=production", "data_type=market"]
    )

    # Configure the client manager with both catalogs
    from .trino_client import configure_trino_catalogs
    configure_trino_catalogs([raw_catalog_config, market_catalog_config])

    # Log the configuration
    from aurum.telemetry.context import log_structured
    log_structured(
        "info",
        "trino_catalogs_initialized",
        raw_catalog=TrinoCatalogType.RAW.value,
        market_catalog=TrinoCatalogType.MARKET.value,
        raw_access_level=raw_catalog_config.access_level.value,
        market_access_level=market_catalog_config.access_level.value,
    )
 # database_monitor is exposed lazily via __getattr__

__all__ = [
    "performance_router",
    "query_analysis_router",
    "optimization_router",
    "connections_router",
    "health_router",
    "trino_admin_router",
    # Trino catalog types
    "TrinoCatalogType",
    "TrinoAccessLevel",
    "TrinoCatalogConfig",
    # Trino initialization
    "initialize_trino_catalogs",
    # Trino client functions
    "get_trino_client",
    "get_trino_client_by_catalog",
    "get_trino_catalog_config",
    "configure_trino_catalogs",
    "TrinoClientManager",
    # Database monitor functions
    "get_database_monitor",
    "initialize_database_monitoring",
    "DatabaseMonitor",
    "QueryMetrics",
    "QueryPattern",
    "OptimizationSuggestion",
    "QueryPerformanceLevel",
    "OptimizationType",
    "AutoReforecastRepository",
    "AutoReforecastJobRepository",
    "get_auto_reforecast_repository",
    "get_auto_reforecast_job_repository",
    "AutoReforecastScheduler",
    "get_auto_reforecast_scheduler",
]
