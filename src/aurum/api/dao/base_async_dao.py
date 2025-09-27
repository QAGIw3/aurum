"""Base async DAO class providing common database operations with connection pooling."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from aurum.data import DataBackend, get_backend, ConnectionConfig, PoolConfig
from aurum.data.backend_adapter import BackendAdapter

LOGGER = logging.getLogger(__name__)


class BaseAsyncDao(ABC):
    """Abstract base class for async Data Access Objects with connection pooling."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize the async DAO with settings and backend adapter."""
        from ..state import get_settings
        
        self._settings = settings or get_settings()
        self._backend_adapter = BackendAdapter(self._settings)
        self._backend: Optional[DataBackend] = None
    
    async def _get_backend(self) -> DataBackend:
        """Get or create the database backend with connection pooling."""
        if self._backend is None:
            self._backend = await self._backend_adapter.get_backend()
        return self._backend
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool = True
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            use_cache: Whether to use query caching (if available)
            
        Returns:
            List of dictionaries representing query results
        """
        try:
            backend = await self._get_backend()
            result = await backend.execute_query(query, params)
            
            # Convert QueryResult to list of dicts
            if not result.rows:
                return []
            
            return [
                dict(zip(result.columns, row)) 
                for row in result.rows
            ]
            
        except Exception as e:
            LOGGER.error(
                "Query execution failed",
                extra={
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "params": params,
                    "error": str(e)
                }
            )
            raise
    
    async def execute_query_raw(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[List[str], List[Tuple[Any, ...]]]:
        """Execute a query and return raw columns and rows.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            Tuple of (columns, rows)
        """
        try:
            backend = await self._get_backend()
            result = await backend.execute_query(query, params)
            return result.columns, result.rows
            
        except Exception as e:
            LOGGER.error(
                "Raw query execution failed",
                extra={
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "params": params,
                    "error": str(e)
                }
            )
            raise
    
    async def execute_count_query(
        self, 
        base_query: str, 
        params: Optional[Dict[str, Any]] = None
    ) -> int:
        """Execute a count query for pagination support.
        
        Args:
            base_query: Base query to wrap with COUNT(*)
            params: Query parameters
            
        Returns:
            Total count of records
        """
        count_query = f"SELECT COUNT(*) as total FROM ({base_query}) AS count_subquery"
        
        try:
            results = await self.execute_query(count_query, params)
            return results[0]["total"] if results else 0
            
        except Exception as e:
            LOGGER.error(
                "Count query execution failed",
                extra={
                    "base_query": base_query[:200] + "..." if len(base_query) > 200 else base_query,
                    "params": params,
                    "error": str(e)
                }
            )
            raise
    
    async def close(self):
        """Close the backend connection."""
        if self._backend_adapter:
            await self._backend_adapter.close()
        self._backend = None
    
    @property
    def backend_name(self) -> Optional[str]:
        """Get the name of the current backend."""
        return self._backend.name if self._backend else None
    
    @property
    @abstractmethod
    def dao_name(self) -> str:
        """Return the name of this DAO for logging purposes."""
        pass