"""Connection pooling and resource optimization utilities."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, TypeVar, Generic

try:  # optional dependency
    from opentelemetry import trace  # type: ignore
except Exception:  # pragma: no cover - fallback noop tracer
    import types
    from contextlib import nullcontext

    class _NoopTracer:
        def start_as_current_span(self, name: str):  # noqa: D401 - simple noop
            return nullcontext()

    trace = types.SimpleNamespace(get_tracer=lambda name: _NoopTracer())  # type: ignore

from ..telemetry.context import get_request_id

T = TypeVar('T')

LOGGER = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass
class PoolConfig:
    """Configuration for connection pools."""
    min_size: int = 5
    max_size: int = 20
    max_idle_time: int = 300  # 5 minutes
    connection_timeout: float = 10.0
    acquire_timeout: float = 30.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class PoolMetrics:
    """Metrics for connection pool performance."""
    connections_created: int = 0
    connections_destroyed: int = 0
    connections_acquired: int = 0
    connections_released: int = 0
    connection_wait_time: float = 0.0
    pool_exhaustion_count: int = 0
    average_pool_size: float = 0.0


class ConnectionPool(ABC, Generic[T]):
    """Abstract base class for connection pools."""

    def __init__(self, config: PoolConfig, factory_func):
        self.config = config
        self.factory_func = factory_func
        self._pool: asyncio.Queue[T] = asyncio.Queue(maxsize=config.max_size)
        self._in_use: set[T] = set()
        self._metrics = PoolMetrics()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

    async def acquire(self) -> T:
        """Acquire a connection from the pool."""
        with trace.get_tracer(__name__).start_as_current_span("connection_pool.acquire") as span:

            try:
                # Try to get existing connection first
                conn = self._pool.get_nowait()
                self._in_use.add(conn)
                self._metrics.connections_acquired += 1
                span.set_attribute("pool.result", "existing")
                return conn

            except asyncio.QueueEmpty:
                # Pool is empty, try to create new connection
                if len(self._in_use) < self.config.max_size:
                    try:
                        conn = await asyncio.wait_for(
                            self._create_connection(),
                            timeout=self.config.acquire_timeout
                        )
                        self._in_use.add(conn)
                        self._metrics.connections_created += 1
                        self._metrics.connections_acquired += 1
                        span.set_attribute("pool.result", "created")
                        return conn
                    except asyncio.TimeoutError:
                        self._metrics.pool_exhaustion_count += 1
                        span.set_attribute("pool.result", "timeout")
                        raise
                else:
                    # Wait for available connection
                    self._metrics.pool_exhaustion_count += 1
                    span.set_attribute("pool.result", "wait")
                    raise Exception("Connection pool exhausted")

    async def release(self, conn: T) -> None:
        """Release a connection back to the pool."""
        with trace.get_tracer(__name__).start_as_current_span("connection_pool.release") as span:

            if conn in self._in_use:
                self._in_use.remove(conn)
                self._metrics.connections_released += 1

                # Check if connection is still healthy
                if await self._is_connection_healthy(conn):
                    try:
                        if self._pool.qsize() >= self.max_idle:
                            await self._destroy_connection(conn)
                            self._metrics.connections_destroyed += 1
                            span.set_attribute("pool.result", "idle_trim")
                        else:
                            self._pool.put_nowait(conn)
                            span.set_attribute("pool.result", "returned")
                    except asyncio.QueueFull:
                        # Pool is full, destroy the connection
                        await self._destroy_connection(conn)
                        self._metrics.connections_destroyed += 1
                        span.set_attribute("pool.result", "destroyed")
                else:
                    # Connection is unhealthy, destroy it
                    await self._destroy_connection(conn)
                    self._metrics.connections_destroyed += 1
                    span.set_attribute("pool.result", "unhealthy")

    async def get_metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        metrics = self._metrics
        metrics.average_pool_size = len(self._in_use) + self._pool.qsize()
        return metrics

    async def start(self) -> None:
        """Start the connection pool."""
        # Initialize with minimum connections
        for _ in range(self.config.min_size):
            conn = await self._create_connection()
            await self._pool.put(conn)

        # Start cleanup task
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

    async def stop(self) -> None:
        """Stop the connection pool."""
        self._shutdown_event.set()

        if self._cleanup_task:
            self._cleanup_task.cancel()

        # Clean up all connections
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                await self._destroy_connection(conn)
            except asyncio.QueueEmpty:
                break

        # Clean up in-use connections
        for conn in list(self._in_use):
            await self._destroy_connection(conn)

    @abstractmethod
    async def _create_connection(self) -> T:
        """Create a new connection."""
        pass

    @abstractmethod
    async def _destroy_connection(self, conn: T) -> None:
        """Destroy a connection."""
        pass

    @abstractmethod
    async def _is_connection_healthy(self, conn: T) -> bool:
        """Check if a connection is healthy."""
        pass

    async def _cleanup_loop(self) -> None:
        """Periodically clean up idle connections."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(60)  # Cleanup every minute

                # Check for idle connections (simplified)
                # In a real implementation, you'd track connection timestamps

            except asyncio.CancelledError:
                break
            except Exception as exc:
                LOGGER.error(f"Connection pool cleanup failed: {exc}")


class DatabaseConnectionPool(ConnectionPool[T]):
    """Connection pool for database connections."""

    def __init__(self, config: PoolConfig, connection_string: str, **connection_kwargs):
        self.connection_string = connection_string
        self.connection_kwargs = connection_kwargs
        super().__init__(config, self._create_db_connection)

    async def _create_connection(self) -> T:
        """Create a database connection."""
        # This would use actual database connection creation
        # For now, we'll create a mock connection
        return await self._create_db_connection()

    async def _create_db_connection(self) -> Any:
        """Create actual database connection."""
        try:
            # Use psycopg for PostgreSQL/TimescaleDB
            import psycopg

            conn = await psycopg.AsyncConnection.connect(
                self.connection_string,
                **self.connection_kwargs
            )
            return conn

        except ImportError:
            # Fallback for testing
            class MockConnection:
                def __init__(self, conn_str: str):
                    self.connection_string = conn_str
                    self.is_closed = False

                async def close(self):
                    self.is_closed = True

            return MockConnection(self.connection_string)

    async def _destroy_connection(self, conn: T) -> None:
        """Destroy database connection."""
        if hasattr(conn, 'close'):
            await conn.close()

    async def _is_connection_healthy(self, conn: T) -> bool:
        """Check if database connection is healthy."""
        try:
            # Simple ping query
            if hasattr(conn, 'execute'):
                await conn.execute("SELECT 1")
                return True
            return not getattr(conn, 'is_closed', True)
        except Exception:
            return False


class TrinoConnectionPool(ConnectionPool[T]):
    """Connection pool for Trino connections."""

    def __init__(self, config: PoolConfig, host: str, port: int, **connection_kwargs):
        self.host = host
        self.port = port
        self.connection_kwargs = connection_kwargs
        super().__init__(config, self._create_trino_connection)

    async def _create_connection(self) -> T:
        """Create a Trino connection."""
        return await self._create_trino_connection()

    async def _create_trino_connection(self) -> Any:
        """Create actual Trino connection."""
        try:
            from trino.dbapi import connect

            conn = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: connect(
                    host=self.host,
                    port=self.port,
                    **self.connection_kwargs
                )
            )
            return conn

        except ImportError:
            # Fallback for testing
            class MockConnection:
                def __init__(self, host: str, port: int):
                    self.host = host
                    self.port = port
                    self.is_closed = False

                def close(self):
                    self.is_closed = True

            return MockConnection(self.host, self.port)

    async def _destroy_connection(self, conn: T) -> None:
        """Destroy Trino connection."""
        if hasattr(conn, 'close'):
            conn.close()

    async def _is_connection_healthy(self, conn: T) -> bool:
        """Check if Trino connection is healthy."""
        try:
            # Simple query to test connection
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception:
            return False


class RedisConnectionPool:
    """Connection pool for Redis connections."""

    def __init__(self, config: PoolConfig, host: str, port: int, **connection_kwargs):
        self.config = config
        self.host = host
        self.port = port
        self.connection_kwargs = connection_kwargs
        self._pool: Optional[Any] = None

    async def start(self) -> None:
        """Start the Redis connection pool."""
        try:
            import redis.asyncio as redis

            self._pool = redis.ConnectionPool(
                host=self.host,
                port=self.port,
                max_connections=self.config.max_size,
                retry_on_timeout=True,
                **self.connection_kwargs
            )
        except ImportError:
            LOGGER.warning("Redis not available, using mock connection pool")

    async def stop(self) -> None:
        """Stop the Redis connection pool."""
        if self._pool:
            await self._pool.disconnect()

    @asynccontextmanager
    async def get_connection(self):
        """Get a Redis connection from the pool."""
        if self._pool:
            connection = await self._pool.acquire()
            try:
                yield connection
            finally:
                await self._pool.release(connection)
        else:
            # Mock connection for testing
            class MockConnection:
                pass
            yield MockConnection()


# Global connection pool instances
_db_pool: Optional[DatabaseConnectionPool] = None
_trino_pool: Optional[TrinoConnectionPool] = None
_redis_pool: Optional[RedisConnectionPool] = None


def get_database_pool() -> Optional[DatabaseConnectionPool]:
    """Get the global database connection pool."""
    return _db_pool


def get_trino_pool() -> Optional[TrinoConnectionPool]:
    """Get the global Trino connection pool."""
    return _trino_pool


def get_redis_pool() -> Optional[RedisConnectionPool]:
    """Get the global Redis connection pool."""
    return _redis_pool


def initialize_connection_pools(
    db_config: Optional[PoolConfig] = None,
    trino_config: Optional[PoolConfig] = None,
    redis_config: Optional[PoolConfig] = None
) -> None:
    """Initialize global connection pools."""
    global _db_pool, _trino_pool, _redis_pool

    # Default configurations
    if db_config is None:
        db_config = PoolConfig(min_size=5, max_size=20)

    if trino_config is None:
        trino_config = PoolConfig(min_size=3, max_size=10)

    if redis_config is None:
        redis_config = PoolConfig(min_size=5, max_size=20)

    # Initialize pools with mock connections for now
    # In production, these would use actual connection strings
    _db_pool = DatabaseConnectionPool(db_config, "postgresql://localhost")
    _trino_pool = TrinoConnectionPool(trino_config, "localhost", 8080)
    _redis_pool = RedisConnectionPool(redis_config, "localhost", 6379)

    LOGGER.info("Connection pools initialized")
