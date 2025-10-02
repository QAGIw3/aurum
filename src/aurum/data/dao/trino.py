"""Trino DAO for federated SQL queries.

Provides async access to Trino for querying Iceberg tables and
other federated data sources using the unified connection management system.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from aurum.database import get_connection_manager_registry
from .base import BaseAsyncDAO, ConnectionError, QueryError

logger = logging.getLogger(__name__)


class TrinoDAO(BaseAsyncDAO):
    """Async DAO for Trino database operations.

    Trino is used for:
    - Querying Iceberg tables (market data, curves, scenarios)
    - Federated queries across multiple data sources
    - OLAP analytics queries

    Uses the unified connection management system for standardized pooling.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = get_connection_manager_registry()
        self._pool_name = "trino"

    async def initialize(self) -> None:
        """Initialize Trino connection pool using unified manager."""
        if self._is_initialized:
            return

        try:
            from aurum.database import DatabasePoolFactory
            from aurum.core.settings import get_settings

            settings = get_settings()

            # Create and register Trino pool manager
            pool_manager = DatabasePoolFactory.create_pool_manager("trino", settings)
            await self._registry.register_pool(self._pool_name, pool_manager)

            self._is_initialized = True
            logger.info("Initialized Trino DAO with unified connection manager")

        except Exception as e:
            raise ConnectionError(f"Failed to initialize Trino connection pool: {e}")

    async def close(self) -> None:
        """Close Trino connection pool."""
        if not self._is_initialized:
            return

        try:
            pool = await self._registry.get_pool(self._pool_name)
            if pool:
                await pool.close()

            self._is_initialized = False
            logger.info("Trino DAO closed")

        except Exception as e:
            logger.error(f"Error closing Trino DAO: {e}")
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute a Trino query and return all results."""
        if not self._is_initialized:
            await self.initialize()

        self._log_query(query, params)

        try:
            # Use unified connection management
            async with self._registry.get_pool(self._pool_name) as pool:
                async with pool.get_connection() as conn:
                    # Execute query using the connection
                    result = await conn.execute(query, params)
                    # Convert result format if needed
                    if isinstance(result, list) and result and isinstance(result[0], (list, tuple)):
                        # Convert rows to dict format if columns available
                        return result
                    return result

        except Exception as e:
            raise self._handle_error(e, query, params)
    
    async def execute_query_single(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute query and return single result."""
        results = await self.execute_query(query, params, timeout)

        if not results:
            return None

        if len(results) > 1:
            raise QueryError(f"Expected single result, got {len(results)}", query=query, params=params)

        return results[0] if results else None

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

        # Process in batches using unified connection management
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i:i + batch_size]

            try:
                async with self._registry.get_pool(self._pool_name) as pool:
                    async with pool.get_connection() as conn:
                        # Execute each query in the batch
                        for params in batch:
                            result = await conn.execute(query, params)
                            # Count affected rows (simplified - actual implementation may vary)
                            if result:
                                total_affected += 1

            except Exception as e:
                logger.error(f"Batch execution failed: {e}")
                raise QueryError(f"Batch execution failed: {e}", query=query, params=params_list[i] if i < len(params_list) else None)

        return total_affected

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
                    # Execute query and get cursor for streaming
                    # Note: This is a simplified implementation - actual streaming
                    # would depend on the specific connection type
                    result = await conn.execute(query, params)

                    # For now, return all results in chunks
                    if result:
                        for i in range(0, len(result), chunk_size):
                            chunk = result[i:i + chunk_size]
                            yield chunk

        except Exception as e:
            logger.error(f"Stream query failed: {e}")
            raise QueryError(f"Stream query failed: {e}", query=query, params=params)

