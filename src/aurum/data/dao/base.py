"""Base async DAO with connection pooling and common functionality.

This module provides the foundation for all database access objects,
implementing connection management, query execution, and error handling
following SOLID principles.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, AsyncContextManager
from contextlib import asynccontextmanager

from aurum.core import AurumSettings

logger = logging.getLogger(__name__)


class DAOError(Exception):
    """Base exception for DAO errors."""
    pass


class ConnectionError(DAOError):
    """Database connection error."""
    pass


class QueryError(DAOError):
    """Query execution error."""
    
    def __init__(self, message: str, query: Optional[str] = None, params: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.query = query
        self.params = params


class BaseAsyncDAO(ABC):
    """Abstract base class for async Data Access Objects.
    
    Provides:
    - Connection pooling and lifecycle management
    - Query execution with parameter binding
    - Error handling and logging
    - Metrics collection hooks
    - Transaction support (where applicable)
    
    Following SOLID principles:
    - Single Responsibility: Database operations only
    - Open/Closed: Extensible via abstract methods
    - Liskov Substitution: All implementations are interchangeable
    - Interface Segregation: Minimal interface, specific methods in subclasses
    - Dependency Inversion: Depends on AurumSettings abstraction
    """
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        """Initialize the DAO with settings.
        
        Args:
            settings: Application settings. If None, loads from environment.
        """
        self.settings = settings or self._load_settings()
        self._connection_pool: Optional[Any] = None
        self._is_initialized = False
        
    def _load_settings(self) -> AurumSettings:
        """Load settings from environment."""
        from aurum.core.settings import get_settings
        return get_settings()
    
    async def __aenter__(self) -> BaseAsyncDAO:
        """Async context manager entry."""
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit with cleanup."""
        await self.close()
    
    @abstractmethod
    async def initialize(self) -> None:
        """Initialize connection pool and resources.
        
        Must be called before executing queries. Idempotent.
        """
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close connection pool and release resources.
        
        Safe to call multiple times. Idempotent.
        """
        pass
    
    @abstractmethod
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return all results.
        
        Args:
            query: SQL query to execute
            params: Query parameters for binding
            timeout: Query timeout in seconds
            
        Returns:
            List of rows as dictionaries
            
        Raises:
            QueryError: If query execution fails
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    async def execute_query_single(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None
    ) -> Optional[Dict[str, Any]]:
        """Execute a query and return single result.
        
        Args:
            query: SQL query to execute
            params: Query parameters for binding
            timeout: Query timeout in seconds
            
        Returns:
            Single row as dictionary, or None if no results
            
        Raises:
            QueryError: If query execution fails or multiple results returned
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    async def execute_many(
        self,
        query: str,
        params_list: List[Dict[str, Any]],
        batch_size: int = 1000
    ) -> int:
        """Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query to execute
            params_list: List of parameter dictionaries
            batch_size: Number of queries to batch together
            
        Returns:
            Total number of affected rows
            
        Raises:
            QueryError: If query execution fails
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    async def stream_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        chunk_size: int = 1000
    ):
        """Stream query results in chunks.
        
        Args:
            query: SQL query to execute
            params: Query parameters for binding
            chunk_size: Number of rows per chunk
            
        Yields:
            Chunks of rows as lists of dictionaries
            
        Raises:
            QueryError: If query execution fails
            ConnectionError: If connection fails
        """
        pass
    
    async def health_check(self) -> bool:
        """Check if database connection is healthy.
        
        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            await self.execute_query("SELECT 1")
            return True
        except Exception as e:
            logger.warning(f"Health check failed: {e}")
            return False
    
    def _log_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> None:
        """Log query for debugging and auditing."""
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(f"Executing query: {query[:200]}..." if len(query) > 200 else f"Executing query: {query}")
            if params:
                logger.debug(f"Query params: {params}")
    
    def _handle_error(self, error: Exception, query: str, params: Optional[Dict[str, Any]] = None) -> QueryError:
        """Convert database errors to DAO errors with context."""
        error_msg = f"Query failed: {str(error)}"
        logger.error(error_msg, exc_info=True)
        return QueryError(error_msg, query=query, params=params)

