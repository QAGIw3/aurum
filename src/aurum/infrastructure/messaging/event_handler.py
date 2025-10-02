"""Event handler interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from ...domain.shared_kernel.entities import DomainEvent


class EventHandler(ABC):
    """Base class for domain event handlers.
    
    Event handlers react to domain events and perform side effects like:
    - Updating read models
    - Sending notifications
    - Triggering workflows
    - Integrating with external systems
    """
    
    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """Handle a domain event.
        
        Args:
            event: The domain event to handle
        """
        pass

