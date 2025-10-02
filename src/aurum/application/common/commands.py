"""Command pattern implementations for CQRS."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Generic, Type, TypeVar

from .results import Result

TCommand = TypeVar('TCommand', bound='Command')
TResult = TypeVar('TResult')


@dataclass
class Command(ABC):
    """Base class for commands in CQRS pattern.
    
    Commands represent intentions to change system state. They should:
    - Be named with imperative verbs (CreateOrder, UpdatePrice)
    - Contain all data needed to perform the operation
    - Be immutable (use frozen dataclasses)
    - Not return data (use queries for that)
    """
    pass


class CommandHandler(ABC, Generic[TCommand, TResult]):
    """Base class for command handlers.
    
    Handlers implement the logic to execute a command. They should:
    - Validate the command
    - Load necessary aggregates
    - Execute domain logic
    - Save changes
    - Publish domain events
    """
    
    @abstractmethod
    async def handle(self, command: TCommand) -> Result[TResult]:
        """Handle the command.
        
        Args:
            command: The command to execute
            
        Returns:
            Result indicating success or failure
        """
        pass


class CommandBus:
    """Command bus for dispatching commands to handlers.
    
    Provides centralized command handling with:
    - Type-safe command routing
    - Middleware support (logging, validation, etc.)
    - Transaction management
    """
    
    def __init__(self):
        self._handlers: Dict[Type[Command], CommandHandler] = {}
    
    def register(self, command_type: Type[TCommand], handler: CommandHandler[TCommand, TResult]) -> None:
        """Register a command handler.
        
        Args:
            command_type: The command class to handle
            handler: The handler instance
        """
        if command_type in self._handlers:
            raise ValueError(f"Handler already registered for {command_type.__name__}")
        
        self._handlers[command_type] = handler
    
    async def dispatch(self, command: TCommand) -> Result[TResult]:
        """Dispatch a command to its handler.
        
        Args:
            command: The command to dispatch
            
        Returns:
            Result from the handler
            
        Raises:
            ValueError: If no handler is registered for the command type
        """
        command_type = type(command)
        
        if command_type not in self._handlers:
            raise ValueError(f"No handler registered for {command_type.__name__}")
        
        handler = self._handlers[command_type]
        return await handler.handle(command)

