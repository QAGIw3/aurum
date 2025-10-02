"""Unified database connection pool manager.

This module provides a standardized interface for managing database connection pools
across all database types (Trino, TimescaleDB, ClickHouse, PostgreSQL).
"""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Protocol, Union
from contextlib import asynccontextmanager

from aurum.core import AurumSettings

logger = logging.getLogger(__name__)


class PoolMetrics:
    """Standardized connection pool metrics."""

    def __init__(
        self,
        active_connections: int = 0,
        idle_connections: int = 0,
        total_connections: int = 0,
        max_connections: int = 0,
        pool_utilization: float = 0.0,
        acquire_timeout_seconds: float = 0.0,
        query_timeout_seconds: float = 0.0,
    ):
        self.active_connections = active_connections
        self.idle_connections = idle_connections
        self.total_connections = total_connections
        self.max_connections = max_connections
        self.pool_utilization = pool_utilization
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self.query_timeout_seconds = query_timeout_seconds
        self.timestamp = time.time()

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for serialization."""
        return {
            "active_connections": self.active_connections,
            "idle_connections": self.idle_connections,
            "total_connections": self.total_connections,
            "max_connections": self.max_connections,
            "pool_utilization": self.pool_utilization,
            "acquire_timeout_seconds": self.acquire_timeout_seconds,
            "query_timeout_seconds": self.query_timeout_seconds,
            "timestamp": self.timestamp,
        }


class ConnectionConfig:
    """Base configuration for database connections."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: Optional[str] = None,
        ssl: bool = False,
        timeout_seconds: float = 30.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.ssl = ssl
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds


class PoolConfig:
    """Configuration for connection pool behavior."""

    def __init__(
        self,
        min_size: int = 1,
        max_size: int = 10,
        max_idle: int = 5,
        acquire_timeout_seconds: float = 10.0,
        query_timeout_seconds: float = 30.0,
        idle_timeout_seconds: float = 300.0,  # 5 minutes
        health_check_interval_seconds: float = 60.0,
        max_retries: int = 3,
        retry_backoff_seconds: float = 1.0,
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle = max_idle
        self.acquire_timeout_seconds = acquire_timeout_seconds
        self.query_timeout_seconds = query_timeout_seconds
        self.idle_timeout_seconds = idle_timeout_seconds
        self.health_check_interval_seconds = health_check_interval_seconds
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds


class DatabaseConnection(Protocol):
    """Protocol for database connections."""

    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query."""
        ...

    async def close(self) -> None:
        """Close the connection."""
        ...

    @property
    def is_closed(self) -> bool:
        """Check if connection is closed."""
        ...


class ConnectionPoolManager(ABC):
    """Abstract base class for database connection pool managers."""

    def __init__(self, config: ConnectionConfig, pool_config: PoolConfig):
        self.config = config
        self.pool_config = pool_config
        self._is_initialized = False
        self._is_closed = False
        self._lock = asyncio.Lock()

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize the connection pool."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close all connections and cleanup resources."""
        pass

    @abstractmethod
    async def acquire_connection(self) -> DatabaseConnection:
        """Acquire a connection from the pool."""
        pass

    @abstractmethod
    async def release_connection(self, connection: DatabaseConnection) -> None:
        """Release a connection back to the pool."""
        pass

    @abstractmethod
    async def get_pool_metrics(self) -> PoolMetrics:
        """Get current pool metrics."""
        pass

    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the connection pool is healthy."""
        pass

    @asynccontextmanager
    async def get_connection(self):
        """Context manager for acquiring and releasing connections."""
        connection = None
        try:
            connection = await self.acquire_connection()
            yield connection
        finally:
            if connection:
                await self.release_connection(connection)


class ConnectionManagerRegistry:
    """Registry for managing multiple database connection pools."""

    def __init__(self):
        self._pools: Dict[str, ConnectionPoolManager] = {}
        self._lock = asyncio.Lock()

    async def register_pool(
        self,
        name: str,
        pool_manager: ConnectionPoolManager
    ) -> None:
        """Register a connection pool manager."""
        async with self._lock:
            if name in self._pools:
                await self._pools[name].close()

            self._pools[name] = pool_manager
            logger.info(f"Registered connection pool: {name}")

    async def get_pool(self, name: str) -> Optional[ConnectionPoolManager]:
        """Get a connection pool manager by name."""
        return self._pools.get(name)

    async def get_all_pools(self) -> Dict[str, ConnectionPoolManager]:
        """Get all registered pools."""
        return self._pools.copy()

    async def close_all_pools(self) -> None:
        """Close all registered pools."""
        async with self._lock:
            for name, pool in self._pools.items():
                try:
                    await pool.close()
                    logger.info(f"Closed connection pool: {name}")
                except Exception as e:
                    logger.error(f"Error closing pool {name}: {e}")

            self._pools.clear()

    async def get_all_metrics(self) -> Dict[str, PoolMetrics]:
        """Get metrics for all pools."""
        metrics = {}
        for name, pool in self._pools.items():
            try:
                metrics[name] = await pool.get_pool_metrics()
            except Exception as e:
                logger.error(f"Error getting metrics for pool {name}: {e}")
                metrics[name] = PoolMetrics()
        return metrics

    async def health_check_all(self) -> Dict[str, bool]:
        """Health check all pools."""
        health = {}
        for name, pool in self._pools.items():
            try:
                health[name] = await pool.health_check()
            except Exception as e:
                logger.error(f"Error health checking pool {name}: {e}")
                health[name] = False
        return health


# Global registry instance
_registry: Optional[ConnectionManagerRegistry] = None


def get_connection_manager_registry() -> ConnectionManagerRegistry:
    """Get the global connection manager registry."""
    global _registry
    if _registry is None:
        _registry = ConnectionManagerRegistry()
    return _registry


def create_connection_config_from_settings(
    settings: AurumSettings,
    database_type: str
) -> ConnectionConfig:
    """Create connection config from application settings."""

    # Get database-specific settings
    if database_type == "trino":
        backend = settings.data_backend
        return ConnectionConfig(
            host=backend.trino_host,
            port=backend.trino_port,
            database=backend.trino_catalog,
            user=backend.trino_user,
            password=backend.trino_password,
            ssl=backend.trino_use_ssl,
            timeout_seconds=30.0,
        )
    elif database_type == "timescale":
        backend = settings.data_backend
        return ConnectionConfig(
            host=backend.timescale_host,
            port=backend.timescale_port,
            database=backend.timescale_database,
            user=backend.timescale_user,
            password=backend.timescale_password,
            ssl=backend.timescale_use_ssl,
            timeout_seconds=30.0,
        )
    elif database_type == "clickhouse":
        backend = settings.data_backend
        return ConnectionConfig(
            host=backend.clickhouse_host,
            port=backend.clickhouse_port,
            database=backend.clickhouse_database,
            user=backend.clickhouse_user,
            password=backend.clickhouse_password,
            ssl=backend.clickhouse_use_ssl,
            timeout_seconds=30.0,
        )
    elif database_type == "postgres":
        backend = settings.data_backend
        return ConnectionConfig(
            host=backend.postgres_host,
            port=backend.postgres_port,
            database=backend.postgres_database,
            user=backend.postgres_user,
            password=backend.postgres_password,
            ssl=backend.postgres_use_ssl,
            timeout_seconds=30.0,
        )
    else:
        raise ValueError(f"Unsupported database type: {database_type}")


def create_pool_config_from_settings(
    settings: AurumSettings,
    database_type: str,
    overrides: Optional[Dict[str, Any]] = None
) -> PoolConfig:
    """Create pool config from application settings."""

    # Default pool configuration
    pool_config = PoolConfig(
        min_size=1,
        max_size=10,
        max_idle=5,
        acquire_timeout_seconds=10.0,
        query_timeout_seconds=30.0,
        idle_timeout_seconds=300.0,
        health_check_interval_seconds=60.0,
        max_retries=3,
        retry_backoff_seconds=1.0,
    )

    # Apply database-specific overrides from settings if available
    backend = settings.data_backend
    if hasattr(backend, f"{database_type}_pool_min_size"):
        pool_config.min_size = getattr(backend, f"{database_type}_pool_min_size", 1)
    if hasattr(backend, f"{database_type}_pool_max_size"):
        pool_config.max_size = getattr(backend, f"{database_type}_pool_max_size", 10)

    # Apply any provided overrides
    if overrides:
        for key, value in overrides.items():
            if hasattr(pool_config, key):
                setattr(pool_config, key, value)

    return pool_config
