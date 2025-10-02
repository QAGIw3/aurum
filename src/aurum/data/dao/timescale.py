"""TimescaleDB DAO for time-series data operations.

Provides async access to TimescaleDB for time-series data storage
and efficient temporal queries.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import BaseAsyncDAO, ConnectionError, QueryError

logger = logging.getLogger(__name__)


class TimescaleDAO(BaseAsyncDAO):
    """Async DAO for TimescaleDB operations.
    
    TimescaleDB is used for:
    - High-frequency time-series data (ISO metrics, prices)
    - Real-time data ingestion
    - Time-based aggregations and rollups
    
    Uses asyncpg for native async PostgreSQL/TimescaleDB access.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._connection_pool = None
    
    async def initialize(self) -> None:
        """Initialize TimescaleDB connection pool."""
        if self._is_initialized:
            return
        
        try:
            import asyncpg
            
            # Get TimescaleDB configuration from settings
            backend_settings = self.settings.data_backend
            
            # Create connection pool
            self._connection_pool = await asyncpg.create_pool(
                host=backend_settings.timescale_host,
                port=backend_settings.timescale_port or 5432,
                user=backend_settings.timescale_user,
                password=backend_settings.timescale_password,
                database=backend_settings.timescale_database,
                min_size=backend_settings.timescale_pool_min_size or 2,
                max_size=backend_settings.timescale_pool_max_size or 20,
                command_timeout=60,
                server_settings={
                    'application_name': 'aurum_api',
                    'search_path': backend_settings.timescale_schema or 'public'
                }
            )
            
            self._is_initialized = True
            logger.info(f"Initialized TimescaleDB DAO: {backend_settings.timescale_host}:{backend_settings.timescale_port}")
            
        except ImportError:
            raise ConnectionError("asyncpg package not installed. Install with: pip install asyncpg")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize TimescaleDB connection: {e}")
    
    async def close(self) -> None:
        """Close TimescaleDB connection pool."""
        if not self._is_initialized:
            return
        
        try:
            if self._connection_pool:
                await self._connection_pool.close()
                self._connection_pool = None
            
            self._is_initialized = False
            logger.info("Closed TimescaleDB DAO")
            
        except Exception as e:
            logger.warning(f"Error closing TimescaleDB connection: {e}")
    
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
            async with self._connection_pool.acquire() as conn:
                # asyncpg uses $1, $2 for positional parameters
                # Convert dict params to positional if needed
                if params:
                    query_params = list(params.values())
                    # Replace named params with $1, $2, etc.
                    for i, key in enumerate(params.keys(), 1):
                        query = query.replace(f":{key}", f"${i}")
                    rows = await conn.fetch(query, *query_params, timeout=timeout)
                else:
                    rows = await conn.fetch(query, timeout=timeout)
                
                # Convert asyncpg.Record to dict
                return [dict(row) for row in rows]
                
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
            async with self._connection_pool.acquire() as conn:
                if params:
                    query_params = list(params.values())
                    for i, key in enumerate(params.keys(), 1):
                        query = query.replace(f":{key}", f"${i}")
                    row = await conn.fetchrow(query, *query_params, timeout=timeout)
                else:
                    row = await conn.fetchrow(query, timeout=timeout)
                
                return dict(row) if row else None
                
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
            async with self._connection_pool.acquire() as conn:
                # Process in batches
                for i in range(0, len(params_list), batch_size):
                    batch = params_list[i:i + batch_size]
                    
                    # Convert to asyncpg format
                    values_list = [list(params.values()) for params in batch]
                    
                    # Use executemany for better performance
                    result = await conn.executemany(query, values_list)
                    
                    # Parse result (e.g., "INSERT 0 100")
                    if result:
                        parts = result.split()
                        if len(parts) >= 2 and parts[-1].isdigit():
                            total_affected += int(parts[-1])
            
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
            async with self._connection_pool.acquire() as conn:
                # Prepare query
                if params:
                    query_params = list(params.values())
                    for i, key in enumerate(params.keys(), 1):
                        query = query.replace(f":{key}", f"${i}")
                else:
                    query_params = []
                
                # Use cursor for streaming
                async with conn.transaction():
                    cursor = await conn.cursor(query, *query_params)
                    
                    while True:
                        rows = await cursor.fetch(chunk_size)
                        if not rows:
                            break
                        
                        yield [dict(row) for row in rows]
                        
        except Exception as e:
            raise self._handle_error(e, query, params)

