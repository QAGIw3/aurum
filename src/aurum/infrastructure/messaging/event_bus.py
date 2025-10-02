"""Event bus implementation for publishing domain events."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, List, Type

from ...domain.shared_kernel.entities import DomainEvent
from .event_handler import EventHandler

logger = logging.getLogger(__name__)


class EventBus(ABC):
    """Abstract event bus for publishing domain events."""
    
    @abstractmethod
    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event.
        
        Args:
            event: The domain event to publish
        """
        pass
    
    @abstractmethod
    def subscribe(self, event_type: Type[DomainEvent], handler: EventHandler) -> None:
        """Subscribe a handler to an event type.
        
        Args:
            event_type: The event class to subscribe to
            handler: The handler to invoke when event is published
        """
        pass


class InMemoryEventBus(EventBus):
    """In-memory event bus implementation.
    
    Suitable for development and testing. For production, consider
    using a message broker like Kafka or RabbitMQ.
    """
    
    def __init__(self):
        """Initialize the event bus."""
        self._handlers: Dict[Type[DomainEvent], List[EventHandler]] = {}
    
    async def publish(self, event: DomainEvent) -> None:
        """Publish a domain event to all subscribed handlers.
        
        Args:
            event: The domain event to publish
        """
        event_type = type(event)
        handlers = self._handlers.get(event_type, [])
        
        if not handlers:
            logger.debug(f"No handlers registered for event type {event_type.__name__}")
            return
        
        logger.info(f"Publishing event {event_type.__name__} to {len(handlers)} handlers")
        
        for handler in handlers:
            try:
                await handler.handle(event)
            except Exception as e:
                logger.error(
                    f"Error in handler {handler.__class__.__name__} "
                    f"for event {event_type.__name__}: {e}",
                    exc_info=True
                )
                # Continue with other handlers even if one fails
    
    def subscribe(self, event_type: Type[DomainEvent], handler: EventHandler) -> None:
        """Subscribe a handler to an event type.
        
        Args:
            event_type: The event class to subscribe to
            handler: The handler to invoke when event is published
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        
        self._handlers[event_type].append(handler)
        logger.info(f"Subscribed {handler.__class__.__name__} to {event_type.__name__}")
    
    def clear_subscriptions(self) -> None:
        """Clear all event subscriptions.
        
        Useful for testing.
        """
        self._handlers.clear()
    
    def get_handler_count(self, event_type: Type[DomainEvent]) -> int:
        """Get the number of handlers for an event type.
        
        Args:
            event_type: The event class
            
        Returns:
            Number of subscribed handlers
        """
        return len(self._handlers.get(event_type, []))

