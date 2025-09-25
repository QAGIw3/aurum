"""Database functionality for the Aurum API."""

from .performance import router as performance_router
from .query_analysis import router as query_analysis_router
from .optimization import router as optimization_router
from .connections import router as connections_router
from .health import router as health_router
from .trino_admin import router as trino_admin_router
from .trino_client import (
    get_trino_client,
    get_trino_client_by_catalog,
    get_trino_catalog_config,
    configure_trino_catalogs,
    TrinoClientManager,
)
from .config import TrinoCatalogType, TrinoAccessLevel, TrinoCatalogConfig


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
from .database_monitor import (
    get_database_monitor,
    initialize_database_monitoring,
    DatabaseMonitor,
    QueryMetrics,
    QueryPattern,
    OptimizationSuggestion,
    QueryPerformanceLevel,
    OptimizationType,
)

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
]
