"""Database connection pool factory for creating standardized pool managers."""

from __future__ import annotations

import logging
from typing import Dict, Any, Optional

from aurum.core import AurumSettings

from .connection_manager import (
    ConnectionConfig,
    ConnectionPoolManager,
    PoolConfig,
    create_connection_config_from_settings,
    create_pool_config_from_settings,
)
from .trino_pool_manager import TrinoPoolManager
from .timescale_pool_manager import TimescalePoolManager

logger = logging.getLogger(__name__)


class DatabasePoolFactory:
    """Factory for creating database connection pool managers."""

    _pool_managers = {
        "trino": TrinoPoolManager,
        "timescale": TimescalePoolManager,
        # TODO: Add ClickHouse and PostgreSQL pool managers
    }

    @classmethod
    def create_pool_manager(
        cls,
        database_type: str,
        settings: AurumSettings,
        config_overrides: Optional[Dict[str, Any]] = None,
        pool_overrides: Optional[Dict[str, Any]] = None,
    ) -> ConnectionPoolManager:
        """Create a connection pool manager for the specified database type."""

        if database_type not in cls._pool_managers:
            raise ValueError(f"Unsupported database type: {database_type}")

        # Create connection configuration
        connection_config = create_connection_config_from_settings(settings, database_type)

        # Create pool configuration
        pool_config = create_pool_config_from_settings(settings, database_type, pool_overrides)

        # Apply any connection config overrides
        if config_overrides:
            for key, value in config_overrides.items():
                if hasattr(connection_config, key):
                    setattr(connection_config, key, value)

        # Create the pool manager
        pool_manager_class = cls._pool_managers[database_type]
        pool_manager = pool_manager_class(connection_config, pool_config)

        logger.info(f"Created {database_type} pool manager: {connection_config.host}:{connection_config.port}")
        return pool_manager

    @classmethod
    def register_pool_manager(
        cls,
        database_type: str,
        pool_manager_class: type[ConnectionPoolManager]
    ) -> None:
        """Register a custom pool manager for a database type."""
        cls._pool_managers[database_type] = pool_manager_class
        logger.info(f"Registered pool manager for {database_type}")

    @classmethod
    def get_supported_databases(cls) -> list[str]:
        """Get list of supported database types."""
        return list(cls._pool_managers.keys())
