"""ClickHouse DAO for OLAP operations.

Provides async access to ClickHouse for analytics, logging,
and high-performance aggregation queries.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BaseAsyncDAO, ConnectionError, QueryError

logger = logging.getLogger(__name__)


class ClickHouseDAO(BaseAsyncDAO):
    """Async DAO for ClickHouse operations.
    
    ClickHouse is used for:
    - Application and system logs
    - Real-time analytics and aggregations
    - High-cardinality data queries
    - Observability metrics
    
    Uses asyncio-compatible ClickHouse client.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._client = None
    
    async def initialize(self) -> None:
        """Initialize ClickHouse client."""
        if self._is_initialized:
            return
        
        try:
            # Lazy import to avoid hard dependency
            from clickhouse_driver import Client as SyncClient
            
            # Get ClickHouse configuration from settings
            backend_settings = self.settings.data_backend
            
            # Create synchronous client (will be used with asyncio.to_thread)
            self._client = SyncClient(
                host=backend_settings.clickhouse_host,
                port=backend_settings.clickhouse_port or 9000,
                user=backend_settings.clickhouse_user,
                password=backend_settings.clickhouse_password,
                database=backend_settings.clickhouse_database or 'default',
                settings={
                    'max_execution_time': backend_settings.clickhouse_query_timeout or 300,
                    'max_query_size': backend_settings.clickhouse_max_query_size or 262144,
                },
                compression=backend_settings.clickhouse_compression or False,
                secure=backend_settings.clickhouse_secure or False,
            )
            
            self._is_initialized = True
            logger.info(f"Initialized ClickHouse DAO: {backend_settings.clickhouse_host}:{backend_settings.clickhouse_port}")
            
        except ImportError:
            raise ConnectionError("clickhouse-driver package not installed. Install with: pip install clickhouse-driver")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize ClickHouse connection: {e}")
    
    async def close(self) -> None:
        """Close ClickHouse client."""
        if not self._is_initialized:
            return
        
        try:
            if self._client:
                self._client.disconnect()
                self._client = None
            
            self._is_initialized = False
            logger.info("Closed ClickHouse DAO")
            
        except Exception as e:
            logger.warning(f"Error closing ClickHouse connection: {e}")
    
    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute a ClickHouse query and return all results."""
        if not self._is_initialized:
            await self.initialize()
        
        self._log_query(query, params)
        
        try:
            import asyncio
            
            # Execute in thread pool since clickhouse-driver is synchronous
            result = await asyncio.to_thread(
                self._client.execute,
                query,
                params or {},
                with_column_types=True
            )
            
            # Convert to list of dicts
            rows, columns_with_types = result
            column_names = [col[0] for col in columns_with_types]
            
            return [dict(zip(column_names, row)) for row in rows]
            
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
        
        return results[0]
    
    async def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """Execute query with multiple parameter sets."""
        if not self._is_initialized:
            await self.initialize()
        
        import asyncio
        
        total_affected = 0
        
        try:
            # ClickHouse has efficient batch inserts
            # Process in batches for better performance
            for i in range(0, len(params_list), batch_size):
                batch = params_list[i:i + batch_size]
                
                # For INSERT queries, use execute_iter for best performance
                if query.strip().upper().startswith('INSERT'):
                    # Convert batch to rows
                    # This assumes the query is an INSERT and batch has consistent keys
                    if batch:
                        result = await asyncio.to_thread(
                            self._client.execute,
                            query,
                            batch,
                            types_check=True
                        )
                        total_affected += len(batch)
                else:
                    # For other queries, execute individually
                    for params in batch:
                        await asyncio.to_thread(
                            self._client.execute,
                            query,
                            params
                        )
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
        
        import asyncio
        
        try:
            # Use execute_iter for streaming
            rows_iter = await asyncio.to_thread(
                self._client.execute_iter,
                query,
                params or {},
                with_column_types=True
            )
            
            # Get column names from first result
            column_names = None
            chunk = []
            
            for row in rows_iter:
                if column_names is None:
                    # First iteration includes column types
                    if isinstance(row, tuple) and len(row) == 2:
                        _, columns_with_types = row
                        column_names = [col[0] for col in columns_with_types]
                        continue
                
                chunk.append(dict(zip(column_names, row)))
                
                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []
            
            # Yield remaining rows
            if chunk:
                yield chunk
                
        except Exception as e:
            raise self._handle_error(e, query, params)

