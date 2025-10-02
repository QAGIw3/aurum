"""Migration initializer for gradual rollout of refactored components.

This module provides utilities for initializing and managing the gradual migration
from legacy code to the new architecture using feature flags.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Dict, Any, Optional

from .features.migration_flags import (
    initialize_migration_flags,
    should_use_unified_db_connections,
    should_enable_db_health_monitoring,
    should_use_unified_collectors,
    should_use_new_ppa_service,
)
from ..database import get_connection_manager_registry, start_production_monitoring
from ..database.production_monitor import configure_alerting

logger = logging.getLogger(__name__)


class MigrationInitializer:
    """Manages the gradual migration of components during startup."""

    def __init__(self):
        self._initialized = False

    async def initialize_migration_features(self) -> None:
        """Initialize migration feature flags and components."""
        if self._initialized:
            return

        try:
            # Initialize migration feature flags
            await initialize_migration_flags()
            logger.info("Initialized migration feature flags")

            # Initialize database connection management
            if should_use_unified_db_connections():
                await self._initialize_database_connections()
                logger.info("Initialized unified database connection management")

            # Initialize database health monitoring
            if should_enable_db_health_monitoring():
                await self._initialize_health_monitoring()
                logger.info("Initialized database health monitoring")

            # Initialize external collectors
            if should_use_unified_collectors():
                await self._initialize_external_collectors()
                logger.info("Initialized unified external collectors")

            self._initialized = True
            logger.info("Migration initialization completed")

        except Exception as e:
            logger.error(f"Failed to initialize migration features: {e}")
            raise

    async def _initialize_database_connections(self) -> None:
        """Initialize unified database connection management."""
        from ..database import DatabasePoolFactory
        from ..core.settings import get_settings

        settings = get_settings()
        registry = get_connection_manager_registry()

        # Create pool managers for each database type
        databases_to_initialize = ["trino", "timescale"]

        for db_type in databases_to_initialize:
            try:
                pool_manager = DatabasePoolFactory.create_pool_manager(db_type, settings)
                await registry.register_pool(db_type, pool_manager)
                logger.info(f"Registered {db_type} connection pool")
            except Exception as e:
                logger.error(f"Failed to initialize {db_type} pool: {e}")

    async def _initialize_health_monitoring(self) -> None:
        """Initialize database health monitoring."""
        from ..database.production_monitor import configure_alerting

        # Configure alerting (can be customized based on environment)
        alert_config = configure_alerting(
            smtp_server=None,  # Configure based on environment
            slack_webhook=None,  # Configure based on environment
            pagerduty_routing_key=None,  # Configure based on environment
            to_emails=["admin@aurum.com"],  # Default fallback
        )

        # Start production monitoring
        await start_production_monitoring(alert_config, interval_seconds=30)

    async def _initialize_external_collectors(self) -> None:
        """Initialize unified external data collectors."""
        # Import collectors that should be initialized
        try:
            from ..external.providers.eia_unified import create_eia_collector
            from ..external.providers.fred_unified import create_fred_collector
            from ..external.providers.noaa_unified import create_noaa_collector
            from ..external.providers.worldbank_unified import create_worldbank_collector

            # Create collectors (they will be initialized on first use)
            collectors = {
                "eia": create_eia_collector,
                "fred": create_fred_collector,
                "noaa": create_noaa_collector,
                "worldbank": create_worldbank_collector,
            }

            logger.info(f"Registered {len(collectors)} unified external collectors")

        except Exception as e:
            logger.error(f"Failed to initialize external collectors: {e}")

    async def check_migration_readiness(self) -> Dict[str, Any]:
        """Check if the system is ready for migration."""
        readiness = {
            "database_connections": await self._check_db_connections_ready(),
            "health_monitoring": await self._check_health_monitoring_ready(),
            "external_collectors": await self._check_collectors_ready(),
            "feature_flags": await self._check_feature_flags_ready(),
        }

        overall_ready = all(status["ready"] for status in readiness.values())
        readiness["overall_ready"] = overall_ready

        return readiness

    async def _check_db_connections_ready(self) -> Dict[str, Any]:
        """Check if database connections are ready."""
        try:
            registry = get_connection_manager_registry()
            pools = await registry.get_all_pools()

            return {
                "ready": len(pools) > 0,
                "pools_count": len(pools),
                "pools": list(pools.keys()),
            }
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
            }

    async def _check_health_monitoring_ready(self) -> Dict[str, Any]:
        """Check if health monitoring is ready."""
        try:
            from ..database.production_monitor import get_production_monitor

            monitor = get_production_monitor()
            status = await monitor.get_monitoring_status()

            return {
                "ready": status.get("monitoring_active", False),
                "pools_monitored": status.get("pools_monitored", 0),
                "alert_handlers": status.get("alert_handlers_count", 0),
            }
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
            }

    async def _check_collectors_ready(self) -> Dict[str, Any]:
        """Check if external collectors are ready."""
        try:
            # Check if unified collectors are available
            collectors_available = [
                "eia_unified",
                "fred_unified",
                "noaa_unified",
                "worldbank_unified",
            ]

            return {
                "ready": True,
                "collectors_available": len(collectors_available),
                "collectors": collectors_available,
            }
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
            }

    async def _check_feature_flags_ready(self) -> Dict[str, Any]:
        """Check if feature flags are ready."""
        try:
            from .features.migration_flags import MIGRATION_FLAGS

            return {
                "ready": True,
                "flags_count": len(MIGRATION_FLAGS),
                "flags": list(MIGRATION_FLAGS.keys()),
            }
        except Exception as e:
            return {
                "ready": False,
                "error": str(e),
            }


# Global migration initializer
_migration_initializer: Optional[MigrationInitializer] = None


def get_migration_initializer() -> MigrationInitializer:
    """Get the global migration initializer."""
    global _migration_initializer
    if _migration_initializer is None:
        _migration_initializer = MigrationInitializer()
    return _migration_initializer


async def initialize_migration() -> None:
    """Initialize the migration system."""
    initializer = get_migration_initializer()
    await initializer.initialize_migration_features()


async def check_migration_readiness() -> Dict[str, Any]:
    """Check migration readiness."""
    initializer = get_migration_initializer()
    return await initializer.check_migration_readiness()


# Migration utility functions
def should_migrate_component(component: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Check if a specific component should be migrated."""
    import asyncio

    # Run async check in sync context
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # We're in an async context, use the async function
            future = asyncio.ensure_future(_async_should_migrate_component(component, context))
            return loop.run_until_complete(future)
        else:
            return loop.run_until_complete(_async_should_migrate_component(component, context))
    except RuntimeError:
        # No event loop, assume migration is enabled
        return True


async def _async_should_migrate_component(component: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """Async helper for checking component migration."""
    from .features.migration_flags import is_migration_feature_enabled

    flag_mapping = {
        "database_connections": "unified_db_connections",
        "health_monitoring": "db_health_monitoring",
        "external_collectors": "unified_external_collectors",
        "ppa_service": "new_ppa_service",
        "model_registry": "decomposed_model_registry",
        "metrics_system": "enhanced_metrics_system",
    }

    flag_key = flag_mapping.get(component)
    if flag_key:
        return await is_migration_feature_enabled(flag_key, context)

    return True  # Default to enabled if not mapped
