"""Adapter layer for using pluggable data backends with existing service layer."""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime

from . import DataBackend, ConnectionConfig, get_backend, close_all_backends
from aurum.performance.connection_pool import PoolConfig
from aurum.core import AurumSettings
# Legacy services have been removed - use new service layer instead


class BackendAdapter:
    """Adapter that provides a unified interface to different data backends."""

    def __init__(self, settings: AurumSettings):
        self.settings = settings
        self.backend_type = settings.data_backend.backend_type.value
        self._backend = None

    async def get_backend(self) -> DataBackend:
        """Get or create the configured backend."""
        if self._backend is None:
            # Create connection config based on backend type
            config = self._create_connection_config()
            # Create pool config based on settings
            pool_config = self._create_pool_config()
            self._backend = get_backend(self.backend_type, config, pool_config)
        return self._backend

    def _create_connection_config(self) -> ConnectionConfig:
        """Create connection configuration based on backend type and settings."""
        backend_settings = self.settings.data_backend

        if self.backend_type == "trino":
            return ConnectionConfig(
                host=backend_settings.trino_host,
                port=backend_settings.trino_port,
                database=f"{backend_settings.trino_catalog}.{backend_settings.trino_database_schema}",
                username=backend_settings.trino_user,
                password=backend_settings.trino_password or "",
                ssl=False,  # Trino uses HTTP scheme instead
                timeout=30
            )
        elif self.backend_type == "clickhouse":
            return ConnectionConfig(
                host=backend_settings.clickhouse_host,
                port=backend_settings.clickhouse_port,
                database=backend_settings.clickhouse_database,
                username=backend_settings.clickhouse_user,
                password=backend_settings.clickhouse_password or "",
                ssl=False,
                timeout=30
            )
        elif self.backend_type == "timescale":
            return ConnectionConfig(
                host=backend_settings.timescale_host,
                port=backend_settings.timescale_port,
                database=backend_settings.timescale_database,
                username=backend_settings.timescale_user,
                password=backend_settings.timescale_password or "",
                ssl=False,
                timeout=30
            )
        else:
            raise ValueError(f"Unsupported backend type: {self.backend_type}")

    def _create_pool_config(self) -> PoolConfig:
        """Create pool configuration based on settings."""
        backend_settings = self.settings.data_backend
        return PoolConfig(
            min_size=backend_settings.connection_pool_min_size,
            max_size=backend_settings.connection_pool_max_size,
            max_idle_time=backend_settings.connection_pool_max_idle_time,
            connection_timeout=backend_settings.connection_pool_timeout_seconds,
            acquire_timeout=backend_settings.connection_pool_acquire_timeout_seconds
        )

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query and return results."""
        backend = await self.get_backend()
        result = await backend.execute_query(query, params)

        # Convert QueryResult to the format expected by the service layer
        return {
            'columns': result.columns,
            'rows': result.rows,
            'metadata': result.metadata
        }

    async def close(self):
        """Close the backend connection."""
        if self._backend:
            await self._backend.close()
            self._backend = None


# Legacy adapter classes removed - use new service layer (src/aurum/services/core/curves.py) instead
        self.backend_type = settings.data_backend.backend_type.value

    async def _execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query using the configured backend."""
        return await self.backend_adapter.execute_query(query, params)

    async def close(self):
        """Close the service and backend."""
        await super().close()
        await self.backend_adapter.close()

    @property
    def backend_name(self) -> str:
        """Return the backend name."""
        return self.backend_adapter.get_backend().name


# Legacy ScenarioServiceAdapter removed - use new service layer instead
