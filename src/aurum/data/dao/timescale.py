"""TimescaleDB DAO for time-series data operations.

Provides async access to TimescaleDB for time-series data storage
and efficient temporal queries using the unified connection management system.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from aurum.database import get_connection_manager_registry
from .base import BaseAsyncDAO, ConnectionError, QueryError

logger = logging.getLogger(__name__)


class TimescaleDAO(BaseAsyncDAO):
    """Async DAO for TimescaleDB operations.

    TimescaleDB is used for:
    - High-frequency time-series data (ISO metrics, prices)
    - Real-time data ingestion
    - Time-based aggregations and rollups

    Uses the unified connection management system for standardized pooling.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = get_connection_manager_registry()
        self._pool_name = "timescale"

    async def initialize(self) -> None:
        """Initialize TimescaleDB connection pool using unified manager."""
        if self._is_initialized:
            return

        try:
            from aurum.database import DatabasePoolFactory
            from aurum.core.settings import get_settings

            settings = get_settings()

            # Create and register TimescaleDB pool manager
            pool_manager = DatabasePoolFactory.create_pool_manager("timescale", settings)
            await self._registry.register_pool(self._pool_name, pool_manager)

            self._is_initialized = True
            logger.info("Initialized TimescaleDB DAO with unified connection manager")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize TimescaleDB connection pool: {e}")

    async def close(self) -> None:
        """Close TimescaleDB connection pool."""
        if not self._is_initialized:
            return

        try:
            pool = await self._registry.get_pool(self._pool_name)
            if pool:
                await pool.close()

            self._is_initialized = False
            logger.info("TimescaleDB DAO closed")

        except Exception as e:
            logger.error(f"Error closing TimescaleDB DAO: {e}")
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute a TimescaleDB query and return all results."""
        if not self._is_initialized:
            await self.initialize()

        self._log_query(query, params)

        try:
            # Use unified connection management
            async with self._registry.get_pool(self._pool_name) as pool:
                async with pool.get_connection() as conn:
                    # Execute query using the connection
                    result = await conn.execute(query, params)
                    # Convert result to expected format
                    if isinstance(result, list):
                        return result
                    return []

        except Exception as e:
            raise self._handle_error(e, query, params)
    
    async def execute_query_single(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute query and return single result."""
        if not self._is_initialized:
            await self.initialize()

        self._log_query(query, params)

        try:
            # Use unified connection management
            async with self._registry.get_pool(self._pool_name) as pool:
                async with pool.get_connection() as conn:
                    result = await conn.execute(query, params)
                    # Return first result or None
                    if isinstance(result, list) and result:
                        return result[0] if isinstance(result[0], dict) else None
                    return None

        except Exception as e:
            raise self._handle_error(e, query, params)

    async def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """Execute query with multiple parameter sets."""
        if not self._is_initialized:
            await self.initialize()

        total_affected = 0

        try:
            # Use unified connection management for batch operations
            async with self._registry.get_pool(self._pool_name) as pool:
                async with pool.get_connection() as conn:
                    # Execute each query in the batch
                    for params in params_list:
                        result = await conn.execute(query, params)
                        # Count affected rows (simplified - actual implementation may vary)
                        if result:
                            total_affected += 1

            return total_affected

        except Exception as e:
            raise self._handle_error(e, query, {"batch_size": batch_size, "total_rows": len(params_list)})

    async def stream_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ):
        """Stream query results in chunks."""
        if not self._is_initialized:
            await self.initialize()

        self._log_query(query, params)

        try:
            # Use unified connection management for streaming
            async with self._registry.get_pool(self._pool_name) as pool:
                async with pool.get_connection() as conn:
                    # Execute query and get results for streaming
                    result = await conn.execute(query, params)

                    # For now, return all results in chunks (simplified streaming)
                    if result:
                        for i in range(0, len(result), chunk_size):
                            chunk = result[i:i + chunk_size]
                            yield chunk

        except Exception as e:
            raise self._handle_error(e, query, params)

