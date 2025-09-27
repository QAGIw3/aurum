"""Base DAO interface and common functionality."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, Optional, TypeVar, Union
from datetime import datetime

from pydantic import BaseModel

from ..telemetry.context import log_structured

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)
ID = TypeVar('ID')


class BaseDAO(ABC, Generic[T, ID]):
    """Base class for all Data Access Objects.

    Provides common functionality for database operations including:
    - Connection management
    - Query execution
    - Error handling and logging
    - Metrics collection
    """

    def __init__(self, connection_string: Optional[str] = None):
        """Initialize the DAO with database connection info.

        Args:
            connection_string: Database connection string (optional, can use env vars)
        """
        self.connection_string = connection_string
        self._connection = None
        self._is_connected = False

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()

    async def connect(self) -> None:
        """Establish database connection."""
        if self._is_connected:
            return

        try:
            await self._connect()
            self._is_connected = True
            log_structured("info", "dao_connected", dao_type=self.__class__.__name__)
        except Exception as e:
            log_structured("error", "dao_connection_failed",
                         dao_type=self.__class__.__name__, error=str(e))
            raise

    async def disconnect(self) -> None:
        """Close database connection."""
        if not self._is_connected:
            return

        try:
            await self._disconnect()
            self._is_connected = False
            log_structured("info", "dao_disconnected", dao_type=self.__class__.__name__)
        except Exception as e:
            log_structured("warning", "dao_disconnect_failed",
                         dao_type=self.__class__.__name__, error=str(e))

    @abstractmethod
    async def _connect(self) -> None:
        """Internal connection establishment - implement in subclasses."""
        pass

    @abstractmethod
    async def _disconnect(self) -> None:
        """Internal connection cleanup - implement in subclasses."""
        pass

    @abstractmethod
    async def create(self, entity: T) -> T:
        """Create a new entity.

        Args:
            entity: Entity to create

        Returns:
            Created entity with generated ID
        """
        pass

    @abstractmethod
    async def get_by_id(self, id: ID) -> Optional[T]:
        """Get entity by ID.

        Args:
            id: Entity ID

        Returns:
            Entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def update(self, id: ID, entity: T) -> Optional[T]:
        """Update existing entity.

        Args:
            id: Entity ID
            entity: Updated entity data

        Returns:
            Updated entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete(self, id: ID) -> bool:
        """Delete entity by ID.

        Args:
            id: Entity ID

        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def list(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> List[T]:
        """List entities with optional filtering and pagination.

        Args:
            limit: Maximum number of entities to return
            offset: Number of entities to skip
            filters: Optional filters to apply
            order_by: Field to order by
            order_desc: Order descending if True

        Returns:
            List of entities
        """
        pass

    async def count(self, filters: Optional[Dict[str, Any]] = None) -> int:
        """Count entities matching filters.

        Args:
            filters: Optional filters to apply

        Returns:
            Number of matching entities
        """
        return len(await self.list(limit=1000000, filters=filters))

    async def exists(self, id: ID) -> bool:
        """Check if entity exists.

        Args:
            id: Entity ID

        Returns:
            True if exists, False otherwise
        """
        return await self.get_by_id(id) is not None


class TrinoDAO(BaseDAO[T, ID]):
    """Base DAO for Trino-based data access.

    Provides Trino-specific functionality including:
    - Query execution with proper parameter binding
    - Result set processing
    - Query performance tracking
    """

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize Trino DAO.

        Args:
            trino_config: Trino configuration dictionary
        """
        self.trino_config = trino_config or {}
        super().__init__()

    async def _connect(self) -> None:
        """Connect to Trino."""
        # Implementation would use trino python client
        # For now, just log the connection
        pass

    async def _disconnect(self) -> None:
        """Disconnect from Trino."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    async def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a Trino query.

        Args:
            query: SQL query string
            parameters: Query parameters

        Returns:
            Query results as list of dictionaries
        """
        start_time = datetime.utcnow()

        try:
            # Implementation would execute actual Trino query
            log_structured("debug", "executing_trino_query",
                         query=query[:100] + "..." if len(query) > 100 else query)

            # Mock result for now
            result = await self._execute_trino_query(query, parameters or {})

            duration = (datetime.utcnow() - start_time).total_seconds()
            log_structured("info", "trino_query_completed",
                         query_duration=duration, result_count=len(result))

            return result

        except Exception as e:
            duration = (datetime.utcnow() - start_time).total_seconds()
            log_structured("error", "trino_query_failed",
                         query_duration=duration, error=str(e))
            raise

    @abstractmethod
    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Internal Trino query execution - implement in subclasses."""
        pass


class CacheDAO(BaseDAO[T, ID]):
    """Base DAO for cache-based data access.

    Provides caching functionality with TTL and invalidation support.
    """

    def __init__(self, cache_config: Optional[Dict[str, Any]] = None):
        """Initialize Cache DAO.

        Args:
            cache_config: Cache configuration dictionary
        """
        self.cache_config = cache_config or {}
        self._cache = {}
        super().__init__()

    async def _connect(self) -> None:
        """Connect to cache backend."""
        # Implementation would connect to Redis or other cache
        pass

    async def _disconnect(self) -> None:
        """Disconnect from cache backend."""
        self._cache.clear()

    async def get_cached(
        self,
        key: str,
        fetcher: callable = None,
        ttl_seconds: Optional[int] = None
    ) -> Optional[Any]:
        """Get value from cache, fetching if not present.

        Args:
            key: Cache key
            fetcher: Function to fetch value if not cached
            ttl_seconds: TTL for cached value

        Returns:
            Cached or fetched value
        """
        if key in self._cache:
            value, expiry = self._cache[key]
            if expiry is None or datetime.utcnow() < expiry:
                return value

        if fetcher:
            value = await fetcher()
            if value is not None:
                expiry = None
                if ttl_seconds:
                    expiry = datetime.utcnow().timestamp() + ttl_seconds
                self._cache[key] = (value, expiry)
            return value

        return None

    async def set_cached(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: TTL for cached value
        """
        expiry = None
        if ttl_seconds:
            expiry = datetime.utcnow().timestamp() + ttl_seconds
        self._cache[key] = (value, expiry)

    async def invalidate(self, key: str) -> bool:
        """Invalidate cache entry.

        Args:
            key: Cache key to invalidate

        Returns:
            True if key existed and was invalidated
        """
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern.

        Args:
            pattern: Pattern to match keys

        Returns:
            Number of keys invalidated
        """
        keys_to_delete = []
        for key in self._cache.keys():
            if pattern in key:
                keys_to_delete.append(key)

        for key in keys_to_delete:
            del self._cache[key]

        return len(keys_to_delete)
