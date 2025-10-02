"""Base entity classes for domain models."""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from .value_objects import EntityId, TenantId


@dataclass
class Entity(ABC):
    """Base class for all domain entities.
    
    Entities have identity that persists over time and through changes.
    Two entities are equal if they have the same identity.
    """
    
    id: EntityId
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Entity):
            return False
        return self.id == other.id
    
    def __hash__(self) -> int:
        return hash(self.id)


@dataclass
class AggregateRoot(Entity):
    """Base class for aggregate roots.
    
    Aggregate roots are the entry points to aggregates and enforce
    consistency boundaries. They are responsible for:
    - Maintaining aggregate invariants
    - Publishing domain events
    - Coordinating changes to entities within the aggregate
    """
    
    version: int = 0
    _domain_events: List[DomainEvent] = field(default_factory=list, init=False, repr=False)
    
    def record_event(self, event: DomainEvent) -> None:
        """Record a domain event that occurred."""
        self._domain_events.append(event)
    
    def clear_events(self) -> List[DomainEvent]:
        """Clear and return recorded events."""
        events = self._domain_events.copy()
        self._domain_events.clear()
        return events
    
    @property
    def domain_events(self) -> List[DomainEvent]:
        """Get recorded domain events."""
        return self._domain_events.copy()


@dataclass
class TenantEntity(Entity):
    """Base class for entities that belong to a tenant."""
    
    tenant_id: TenantId


@dataclass
class DomainEvent(ABC):
    """Base class for domain events.
    
    Domain events represent something that happened in the domain that
    domain experts care about.
    """
    
    event_id: EntityId = field(default_factory=EntityId.generate)
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    aggregate_id: EntityId = field(default=None)
    
    @property
    def event_type(self) -> str:
        """Get the event type name."""
        return self.__class__.__name__

