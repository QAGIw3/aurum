"""Pluggable data backend interfaces for the Aurum API.

Provides a small abstraction over multiple query backends with pooled
connections and a unified `QueryResult`:
- Trino (federated queries: Iceberg, ClickHouse, Postgres, Timescale)
- ClickHouse (OLAP)
- Timescale/Postgres (operational timeseries)

Use `BackendFactory.create_backend()` or `get_backend()` to obtain a cached
backend instance. Close all pooled connections with `close_all_backends()`.

See also:
- docs/observability/observability-guide.md (query metrics)
- docs/pagination.md (cursor semantics)
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple
from datetime import date, datetime
from dataclasses import dataclass

from aurum.performance.connection_pool import ConnectionPool, PoolConfig, PoolMetrics


@dataclass
class QueryResult:
    """Standardized query result format."""
    columns: List[str]
    rows: List[Tuple[Any, ...]]
    metadata: Dict[str, Any] = None


@dataclass
class ConnectionConfig:
    """Configuration for database connections."""
    host: str
    port: int
    database: str
    username: str
    password: str
    ssl: bool = True
    timeout: int = 30


class DataBackend(ABC):
    """Abstract base class for data backends."""

    @abstractmethod
    async def connect(self) -> Any:
        """Create a connection to the backend."""
        pass

    @abstractmethod
    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute a query and return results in standardized format."""
        pass

    @abstractmethod
    async def close(self):
        """Close the connection."""
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the backend name."""
        pass

    @property
    @abstractmethod
    def supports_cursor_pagination(self) -> bool:
        """Whether the backend supports cursor-based pagination."""
        pass


class BackendFactory:
    """Factory for creating data backends."""

    @staticmethod
    def create_backend(backend_type: str, config: ConnectionConfig, pool_config: Optional[PoolConfig] = None) -> DataBackend:
        """Create a backend instance based on type."""
        backends = {
            'trino': TrinoBackend,
            'clickhouse': ClickHouseBackend,
            'timescale': TimescaleBackend,
        }

        backend_class = backends.get(backend_type.lower())
        if not backend_class:
            raise ValueError(f"Unsupported backend type: {backend_type}")

        return backend_class(config, pool_config)

    @staticmethod
    def get_available_backends() -> List[str]:
        """Get list of available backend types."""
        return ['trino', 'clickhouse', 'timescale']


class TrinoBackend(DataBackend):
    """Trino data backend implementation with connection pooling."""

    def __init__(self, config: ConnectionConfig, pool_config: Optional[PoolConfig] = None):
        self.config = config
        self.pool_config = pool_config or PoolConfig()
        self._pool: Optional[ConnectionPool] = None
        self._pool_metrics = PoolMetrics()

    def _create_connection(self):
        """Create a new Trino connection."""
        try:
            from trino.dbapi import connect
            connection = connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                catalog=self.config.database.split('.')[0] if '.' in self.config.database else self.config.database,
                schema=self.config.database.split('.')[1] if '.' in self.config.database else 'default',
            )
            self._pool_metrics.connections_created += 1
            return connection
        except ImportError:
            raise RuntimeError("Trino dependencies not installed")

    async def connect(self):
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = ConnectionPool(self.pool_config, self._create_connection)

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute Trino query using connection pool."""
        if not self._pool:
            await self.connect()

        connection = await self._pool.acquire()
        try:
            cursor = connection.cursor()
            try:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)

                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []

                return QueryResult(
                    columns=columns,
                    rows=rows,
                    metadata={
                        'backend': 'trino',
                        'query_id': getattr(cursor, 'query_id', None),
                        'pool_size': len(self._pool._in_use) + self._pool._pool.qsize(),
                        'pool_metrics': self._pool_metrics.__dict__
                    }
                )
            finally:
                cursor.close()
        finally:
            await self._pool.release(connection)

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def name(self) -> str:
        return "trino"

    @property
    def supports_cursor_pagination(self) -> bool:
        return True

    @property
    def pool_metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        return self._pool_metrics


class ClickHouseBackend(DataBackend):
    """ClickHouse data backend implementation with connection pooling."""

    def __init__(self, config: ConnectionConfig, pool_config: Optional[PoolConfig] = None):
        self.config = config
        self.pool_config = pool_config or PoolConfig()
        self._pool: Optional[ConnectionPool] = None
        self._pool_metrics = PoolMetrics()

    def _create_connection(self):
        """Create a new ClickHouse connection."""
        try:
            import clickhouse_driver
            connection = clickhouse_driver.Client(
                host=self.config.host,
                port=self.config.port,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
            )
            self._pool_metrics.connections_created += 1
            return connection
        except ImportError:
            raise RuntimeError("ClickHouse dependencies not installed")

    async def connect(self):
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = ConnectionPool(self.pool_config, self._create_connection)

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute ClickHouse query using connection pool."""
        if not self._pool:
            await self.connect()

        connection = await self._pool.acquire()
        try:
            if params:
                result = connection.execute(query, params)
            else:
                result = connection.execute(query)

            # ClickHouse returns tuples, convert to list of tuples
            rows = list(result) if result else []
            columns = []  # ClickHouse doesn't provide column names in same way

            return QueryResult(
                columns=columns,
                rows=rows,
                metadata={
                    'backend': 'clickhouse',
                    'rows_affected': len(rows),
                    'pool_size': len(self._pool._in_use) + self._pool._pool.qsize(),
                    'pool_metrics': self._pool_metrics.__dict__
                }
            )
        except Exception as e:
            raise RuntimeError(f"ClickHouse query failed: {str(e)}")
        finally:
            await self._pool.release(connection)

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def name(self) -> str:
        return "clickhouse"

    @property
    def supports_cursor_pagination(self) -> bool:
        return True

    @property
    def pool_metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        return self._pool_metrics


class TimescaleBackend(DataBackend):
    """Timescale (PostgreSQL) data backend implementation with connection pooling."""

    def __init__(self, config: ConnectionConfig, pool_config: Optional[PoolConfig] = None):
        self.config = config
        self.pool_config = pool_config or PoolConfig()
        self._pool: Optional[ConnectionPool] = None
        self._pool_metrics = PoolMetrics()

    async def _create_connection(self):
        """Create a new Timescale connection."""
        try:
            import asyncpg
            connection = await asyncpg.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
            )
            self._pool_metrics.connections_created += 1
            return connection
        except ImportError:
            raise RuntimeError("asyncpg dependencies not installed")

    async def connect(self):
        """Initialize the connection pool."""
        if self._pool is None:
            self._pool = ConnectionPool(self.pool_config, self._create_connection)

    async def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> QueryResult:
        """Execute Timescale query using connection pool."""
        if not self._pool:
            await self.connect()

        connection = await self._pool.acquire()
        try:
            if params:
                result = await connection.fetch(query, *params.values())
            else:
                result = await connection.fetch(query)

            rows = [tuple(record.values()) for record in result]
            columns = list(result[0].keys()) if result else []

            return QueryResult(
                columns=columns,
                rows=rows,
                metadata={
                    'backend': 'timescale',
                    'rows_affected': len(rows),
                    'pool_size': len(self._pool._in_use) + self._pool._pool.qsize(),
                    'pool_metrics': self._pool_metrics.__dict__
                }
            )
        except Exception as e:
            raise RuntimeError(f"Timescale query failed: {str(e)}")
        finally:
            await self._pool.release(connection)

    async def close(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    @property
    def name(self) -> str:
        return "timescale"

    @property
    def supports_cursor_pagination(self) -> bool:
        return True

    @property
    def pool_metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        return self._pool_metrics


# Global backend registry
_backend_registry: Dict[str, DataBackend] = {}


def get_backend(backend_type: str, config: ConnectionConfig, pool_config: Optional[PoolConfig] = None) -> DataBackend:
    """Get or create a backend instance."""
    registry_key = f"{backend_type}:{id(pool_config) if pool_config else 'default'}"
    if registry_key not in _backend_registry:
        _backend_registry[registry_key] = BackendFactory.create_backend(backend_type, config, pool_config)
    return _backend_registry[registry_key]


def clear_backend_cache():
    """Clear the backend cache."""
    global _backend_registry
    _backend_registry.clear()


async def close_all_backends():
    """Close all cached backends."""
    for backend in _backend_registry.values():
        await backend.close()
    clear_backend_cache()
