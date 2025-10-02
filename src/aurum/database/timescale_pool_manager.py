"""TimescaleDB connection pool manager implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

try:
    import psycopg
except ImportError:
    psycopg = None

from .connection_manager import (
    ConnectionConfig,
    ConnectionPoolManager,
    DatabaseConnection,
    PoolConfig,
    PoolMetrics,
)

logger = logging.getLogger(__name__)


class TimescaleConnection:
    """TimescaleDB database connection wrapper."""

    def __init__(self, connection: Any, config: ConnectionConfig):
        self._connection = connection
        self._config = config
        self._closed = False
        self._created_at = time.time()

    async def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a query."""
        if self._closed:
            raise RuntimeError("Connection is closed")

        try:
            # Use psycopg async API
            async with self._connection.cursor() as cursor:
                if params:
                    await cursor.execute(query, params)
                else:
                    await cursor.execute(query)
                return await cursor.fetchall()
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    async def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            try:
                await self._connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._closed = True

    @property
    def is_closed(self) -> bool:
        """Check if connection is closed."""
        return self._closed

    @property
    def age_seconds(self) -> float:
        """Get connection age in seconds."""
        return time.time() - self._created_at


class TimescalePoolManager(ConnectionPoolManager):
    """TimescaleDB connection pool manager."""

    def __init__(self, config: ConnectionConfig, pool_config: PoolConfig):
        super().__init__(config, pool_config)
        self._pool: Optional[Any] = None

    async def initialize(self) -> None:
        """Initialize the TimescaleDB connection pool."""
        if self._is_initialized:
            return

        async with self._lock:
            if self._is_initialized:
                return

            if psycopg is None:
                raise RuntimeError("psycopg package not installed")

            # Create connection string
            conninfo = (
                f"host={self.config.host} "
                f"port={self.config.port} "
                f"dbname={self.config.database} "
                f"user={self.config.user}"
            )

            if self.config.password:
                conninfo += f" password={self.config.password}"

            if self.config.ssl:
                conninfo += " sslmode=require"
            else:
                conninfo += " sslmode=disable"

            # Create async connection pool
            self._pool = psycopg.AsyncConnectionPool(
                conninfo=conninfo,
                min_size=self.pool_config.min_size,
                max_size=self.pool_config.max_size,
                timeout=self.pool_config.acquire_timeout_seconds,
                check=psycopg.AsyncConnectionPool.check_connection,
            )

            self._is_initialized = True
            logger.info(f"Initialized TimescaleDB pool: {self.config.host}:{self.config.port}")

    async def close(self) -> None:
        """Close all connections and cleanup resources."""
        if self._is_closed:
            return

        async with self._lock:
            if self._is_closed:
                return

            if self._pool:
                await self._pool.close()
                self._pool = None

            self._is_closed = True
            logger.info("TimescaleDB pool closed")

    async def acquire_connection(self) -> TimescaleConnection:
        """Acquire a connection from the pool."""
        if not self._pool:
            await self.initialize()

        try:
            connection = await asyncio.wait_for(
                self._pool.acquire(),  # type: ignore
                timeout=self.pool_config.acquire_timeout_seconds
            )
            return TimescaleConnection(connection, self.config)
        except asyncio.TimeoutError:
            raise RuntimeError("Connection acquisition timed out")
        except Exception as e:
            logger.error(f"Failed to acquire connection: {e}")
            raise

    async def release_connection(self, connection: TimescaleConnection) -> None:
        """Release a connection back to the pool."""
        if self._pool and not connection.is_closed:
            try:
                self._pool.put(connection._connection)  # type: ignore
            except Exception as e:
                logger.warning(f"Error releasing connection: {e}")
                await connection.close()

    async def get_pool_metrics(self) -> PoolMetrics:
        """Get current pool metrics."""
        if not self._pool:
            return PoolMetrics()

        try:
            # Get pool statistics
            stats = self._pool.get_stats()  # type: ignore

            return PoolMetrics(
                active_connections=getattr(stats, 'active', 0),
                idle_connections=getattr(stats, 'idle', 0),
                total_connections=getattr(stats, 'total', 0),
                max_connections=self.pool_config.max_size,
                pool_utilization=getattr(stats, 'utilization', 0.0),
                acquire_timeout_seconds=self.pool_config.acquire_timeout_seconds,
                query_timeout_seconds=self.pool_config.query_timeout_seconds,
            )
        except Exception as e:
            logger.warning(f"Error getting pool metrics: {e}")
            return PoolMetrics()

    async def health_check(self) -> bool:
        """Check if the connection pool is healthy."""
        try:
            # Try to acquire and release a connection
            async with self.get_connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"TimescaleDB pool health check failed: {e}")
            return False
