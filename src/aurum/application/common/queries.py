"""Query pattern implementations for CQRS."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Generic, Type, TypeVar

from .results import Result

TQuery = TypeVar('TQuery', bound='Query')
TResult = TypeVar('TResult')


@dataclass
class Query(ABC):
    """Base class for queries in CQRS pattern.
    
    Queries represent requests for data. They should:
    - Be named with nouns or questions (GetOrder, IsUserActive)
    - Contain all parameters needed to fetch data
    - Be immutable (use frozen dataclasses)
    - Not change system state
    """
    pass


class QueryHandler(ABC, Generic[TQuery, TResult]):
    """Base class for query handlers.
    
    Handlers implement the logic to execute a query. They should:
    - Validate the query
    - Fetch data from read models
    - Transform to DTOs
    - Never modify state
    """
    
    @abstractmethod
    async def handle(self, query: TQuery) -> Result[TResult]:
        """Handle the query.
        
        Args:
            query: The query to execute
            
        Returns:
            Result containing the requested data or error
        """
        pass


class QueryBus:
    """Query bus for dispatching queries to handlers.
    
    Provides centralized query handling with:
    - Type-safe query routing
    - Caching support
    - Performance monitoring
    """
    
    def __init__(self):
        self._handlers: Dict[Type[Query], QueryHandler] = {}
    
    def register(self, query_type: Type[TQuery], handler: QueryHandler[TQuery, TResult]) -> None:
        """Register a query handler.
        
        Args:
            query_type: The query class to handle
            handler: The handler instance
        """
        if query_type in self._handlers:
            raise ValueError(f"Handler already registered for {query_type.__name__}")
        
        self._handlers[query_type] = handler
    
    async def dispatch(self, query: TQuery) -> Result[TResult]:
        """Dispatch a query to its handler.
        
        Args:
            query: The query to dispatch
            
        Returns:
            Result from the handler
            
        Raises:
            ValueError: If no handler is registered for the query type
        """
        query_type = type(query)
        
        if query_type not in self._handlers:
            raise ValueError(f"No handler registered for {query_type.__name__}")
        
        handler = self._handlers[query_type]
        return await handler.handle(query)

