"""Trino connection pool manager implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

try:
    import trino
except ImportError:
    trino = None

from .connection_manager import (
    ConnectionConfig,
    ConnectionPoolManager,
    DatabaseConnection,
    PoolConfig,
    PoolMetrics,
)

logger = logging.getLogger(__name__)


class TrinoConnection:
    """Trino database connection wrapper."""

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
            # Trino uses synchronous API, run in thread pool
            loop = asyncio.get_event_loop()
            cursor = await loop.run_in_executor(None, self._connection.cursor)
            await loop.run_in_executor(None, cursor.execute, query, params)
            return await loop.run_in_executor(None, cursor.fetchall)
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    async def close(self) -> None:
        """Close the connection."""
        if not self._closed:
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._connection.close)
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


class TrinoPoolManager(ConnectionPoolManager):
    """Trino connection pool manager."""

    def __init__(self, config: ConnectionConfig, pool_config: PoolConfig):
        super().__init__(config, pool_config)
        self._connections: List[TrinoConnection] = []
        self._active_connections = 0
        self._total_created = 0
        self._executor = None

    async def initialize(self) -> None:
        """Initialize the Trino connection pool."""
        if self._is_initialized:
            return

        async with self._lock:
            if self._is_initialized:
                return

            if trino is None:
                raise RuntimeError("trino package not installed")

            # Create thread pool executor for sync Trino operations
            import concurrent.futures
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.pool_config.max_size,
                thread_name_prefix="trino"
            )

            # Initialize minimum connections
            for _ in range(self.pool_config.min_size):
                connection = await self._create_connection()
                self._connections.append(connection)

            self._total_created = self.pool_config.min_size
            self._is_initialized = True
            logger.info(f"Initialized Trino pool with {self.pool_config.min_size} connections")

    async def close(self) -> None:
        """Close all connections and cleanup resources."""
        if self._is_closed:
            return

        async with self._lock:
            if self._is_closed:
                return

            # Close all connections
            for connection in self._connections:
                try:
                    await connection.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")

            self._connections.clear()
            self._active_connections = 0

            # Shutdown executor
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None

            self._is_closed = True
            logger.info("Trino pool closed")

    async def acquire_connection(self) -> TrinoConnection:
        """Acquire a connection from the pool."""
        async with self._lock:
            if self._is_closed:
                raise RuntimeError("Pool is closed")

            # Try to get existing idle connection
            if self._connections:
                connection = self._connections.pop()
                self._active_connections += 1
                return connection

            # Create new connection if under limit
            if self._total_created < self.pool_config.max_size:
                connection = await self._create_connection()
                self._total_created += 1
                self._active_connections += 1
                return connection

            # Pool exhausted
            raise RuntimeError("Connection pool exhausted")

    async def release_connection(self, connection: TrinoConnection) -> None:
        """Release a connection back to the pool."""
        async with self._lock:
            if self._is_closed:
                await connection.close()
                return

            # Check if connection is still valid
            if connection.is_closed:
                logger.warning("Released closed connection")
                return

            # Check if connection is too old
            if connection.age_seconds > self.pool_config.idle_timeout_seconds:
                await connection.close()
                logger.debug("Closed stale connection")
                return

            # Return to pool if under max idle limit
            if len(self._connections) < self.pool_config.max_idle:
                self._connections.append(connection)
            else:
                await connection.close()

            self._active_connections = max(0, self._active_connections - 1)

    async def get_pool_metrics(self) -> PoolMetrics:
        """Get current pool metrics."""
        async with self._lock:
            active = self._active_connections
            idle = len(self._connections)
            total = active + idle
            max_conn = self.pool_config.max_size
            utilization = active / max(max_conn, 1) if max_conn > 0 else 0.0

            return PoolMetrics(
                active_connections=active,
                idle_connections=idle,
                total_connections=total,
                max_connections=max_conn,
                pool_utilization=utilization,
                acquire_timeout_seconds=self.pool_config.acquire_timeout_seconds,
                query_timeout_seconds=self.pool_config.query_timeout_seconds,
            )

    async def health_check(self) -> bool:
        """Check if the connection pool is healthy."""
        try:
            # Try to acquire and release a connection
            async with self.get_connection() as conn:
                await conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"Trino pool health check failed: {e}")
            return False

    async def _create_connection(self) -> TrinoConnection:
        """Create a new Trino connection."""
        try:
            # Create Trino connection synchronously in thread pool
            loop = asyncio.get_event_loop()

            def _connect():
                if trino is None:
                    raise RuntimeError("trino package not available")

                return trino.dbapi.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    catalog=self.config.database,
                    http_scheme="https" if self.config.ssl else "http",
                )

            connection = await loop.run_in_executor(self._executor, _connect)
            return TrinoConnection(connection, self.config)

        except Exception as e:
            logger.error(f"Failed to create Trino connection: {e}")
            raise
