"""Repository interfaces for domain layer.

Repositories provide collection-like interfaces for retrieving and storing
aggregate roots. They are defined in the domain but implemented in infrastructure.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, List, Optional, TypeVar

from .entities import AggregateRoot
from .value_objects import EntityId

T = TypeVar('T', bound=AggregateRoot)


class Repository(ABC, Generic[T]):
    """Base interface for repositories.
    
    Repositories abstract the persistence layer and provide a collection-like
    interface for working with aggregate roots.
    """
    
    @abstractmethod
    async def get_by_id(self, id: EntityId) -> Optional[T]:
        """Retrieve an aggregate by its ID.
        
        Args:
            id: The entity identifier
            
        Returns:
            The aggregate if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def save(self, aggregate: T) -> None:
        """Save an aggregate.
        
        This method should be idempotent - calling it multiple times with
        the same aggregate should have the same effect as calling it once.
        
        Args:
            aggregate: The aggregate to save
        """
        pass
    
    @abstractmethod
    async def delete(self, id: EntityId) -> None:
        """Delete an aggregate by its ID.
        
        Args:
            id: The entity identifier
        """
        pass
    
    @abstractmethod
    async def exists(self, id: EntityId) -> bool:
        """Check if an aggregate exists.
        
        Args:
            id: The entity identifier
            
        Returns:
            True if the aggregate exists, False otherwise
        """
        pass


class ReadOnlyRepository(ABC, Generic[T]):
    """Base interface for read-only repositories.
    
    Used in CQRS patterns where read models are separate from write models.
    """
    
    @abstractmethod
    async def get_by_id(self, id: EntityId) -> Optional[T]:
        """Retrieve an entity by its ID."""
        pass
    
    @abstractmethod
    async def find_all(self, limit: int = 100, offset: int = 0) -> List[T]:
        """Find all entities with pagination."""
        pass

