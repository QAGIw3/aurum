"""Trino DAO for federated SQL queries.

Provides async access to Trino for querying Iceberg tables and
other federated data sources.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from .base import BaseAsyncDAO, ConnectionError, QueryError

logger = logging.getLogger(__name__)


class TrinoDAO(BaseAsyncDAO):
    """Async DAO for Trino database operations.
    
    Trino is used for:
    - Querying Iceberg tables (market data, curves, scenarios)
    - Federated queries across multiple data sources
    - OLAP analytics queries
    
    Uses a thread pool executor since python-trino is synchronous.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._executor: Optional[ThreadPoolExecutor] = None
        self._trino_connection = None
    
    async def initialize(self) -> None:
        """Initialize Trino connection pool."""
        if self._is_initialized:
            return
        
        try:
            # Lazy import to avoid hard dependency
            import trino
            from trino import auth as trino_auth
            
            # Get Trino configuration from settings
            backend_settings = self.settings.data_backend
            
            # Create thread pool for sync Trino client
            self._executor = ThreadPoolExecutor(
                max_workers=backend_settings.trino_pool_size or 10,
                thread_name_prefix="trino"
            )
            
            # Configure authentication
            auth = None
            if backend_settings.trino_auth_type == "basic":
                auth = trino_auth.BasicAuthentication(
                    backend_settings.trino_user,
                    backend_settings.trino_password or ""
                )
            elif backend_settings.trino_auth_type == "jwt":
                auth = trino_auth.JWTAuthentication(backend_settings.trino_jwt_token)
            elif backend_settings.trino_auth_type == "kerberos":
                auth = trino_auth.KerberosAuthentication()
            
            # Create Trino connection
            self._trino_connection = trino.dbapi.connect(
                host=backend_settings.trino_host,
                port=backend_settings.trino_port,
                user=backend_settings.trino_user,
                catalog=backend_settings.trino_catalog,
                schema=backend_settings.trino_database_schema,
                auth=auth,
                http_scheme="https" if backend_settings.trino_use_ssl else "http",
                verify=backend_settings.trino_verify_ssl if backend_settings.trino_use_ssl else False,
            )
            
            self._is_initialized = True
            logger.info(f"Initialized Trino DAO: {backend_settings.trino_host}:{backend_settings.trino_port}")
            
        except ImportError:
            raise ConnectionError("trino package not installed. Install with: pip install trino")
        except Exception as e:
            raise ConnectionError(f"Failed to initialize Trino connection: {e}")
    
    async def close(self) -> None:
        """Close Trino connection and thread pool."""
        if not self._is_initialized:
            return
        
        try:
            if self._trino_connection:
                await asyncio.get_event_loop().run_in_executor(
                    self._executor, self._trino_connection.close
                )
                self._trino_connection = None
            
            if self._executor:
                self._executor.shutdown(wait=True)
                self._executor = None
            
            self._is_initialized = False
            logger.info("Closed Trino DAO")
            
        except Exception as e:
            logger.warning(f"Error closing Trino connection: {e}")
    
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
            # Execute query in thread pool
            result = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._execute_sync,
                query,
                params
            )
            return result
            
        except Exception as e:
            raise self._handle_error(e, query, params)
    
    def _execute_sync(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute query synchronously (called in thread pool)."""
        cursor = self._trino_connection.cursor()
        try:
            # Bind parameters if provided
            if params:
                # Trino uses :param syntax for named parameters
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            # Fetch all results
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Convert to list of dicts
            return [dict(zip(columns, row)) for row in rows]
            
        finally:
            cursor.close()
    
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
        
        total_affected = 0
        
        # Process in batches
        for i in range(0, len(params_list), batch_size):
            batch = params_list[i:i + batch_size]
            
            # Execute batch in thread pool
            affected = await asyncio.get_event_loop().run_in_executor(
                self._executor,
                self._execute_many_sync,
                query,
                batch
            )
            total_affected += affected
        
        return total_affected
    
    def _execute_many_sync(self, query: str, params_list: List[Dict[str, Any]]) -> int:
        """Execute many synchronously (called in thread pool)."""
        cursor = self._trino_connection.cursor()
        try:
            # Trino doesn't have native executemany, so execute individually
            affected = 0
            for params in params_list:
                cursor.execute(query, params)
                affected += cursor.rowcount if hasattr(cursor, 'rowcount') else 0
            return affected
        finally:
            cursor.close()
    
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
        
        # For streaming, we need to fetch in the executor but yield chunks
        cursor = self._trino_connection.cursor()
        try:
            # Execute query in thread pool
            await asyncio.get_event_loop().run_in_executor(
                self._executor,
                lambda: cursor.execute(query, params) if params else cursor.execute(query)
            )
            
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch and yield chunks
            while True:
                rows = await asyncio.get_event_loop().run_in_executor(
                    self._executor,
                    lambda: cursor.fetchmany(chunk_size)
                )
                
                if not rows:
                    break
                
                yield [dict(zip(columns, row)) for row in rows]
                
        finally:
            cursor.close()

