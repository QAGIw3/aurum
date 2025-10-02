"""Unit of Work pattern for transaction management."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, AsyncContextManager


class UnitOfWork(ABC, AsyncContextManager):
    """Abstract Unit of Work for managing transactions.
    
    The Unit of Work pattern maintains a list of objects affected by a business
    transaction and coordinates writing out changes.
    
    Usage:
        async with unit_of_work:
            # Perform operations
            await repository.save(aggregate)
            # Changes are committed when context exits
    """
    
    @abstractmethod
    async def __aenter__(self) -> UnitOfWork:
        """Enter the unit of work context."""
        pass
    
    @abstractmethod
    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the unit of work context.
        
        If no exception occurred, commits the transaction.
        If an exception occurred, rolls back the transaction.
        """
        pass
    
    @abstractmethod
    async def commit(self) -> None:
        """Commit the transaction."""
        pass
    
    @abstractmethod
    async def rollback(self) -> None:
        """Rollback the transaction."""
        pass

