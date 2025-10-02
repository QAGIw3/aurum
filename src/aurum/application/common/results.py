"""Result type for application layer operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Generic, Optional, TypeVar, Union

T = TypeVar('T')
E = TypeVar('E')


@dataclass(frozen=True)
class Success(Generic[T]):
    """Represents a successful operation result."""
    
    value: T
    
    def is_success(self) -> bool:
        return True
    
    def is_failure(self) -> bool:
        return False


@dataclass(frozen=True)
class Failure(Generic[E]):
    """Represents a failed operation result."""
    
    error: E
    message: str
    details: Optional[dict] = None
    
    def is_success(self) -> bool:
        return False
    
    def is_failure(self) -> bool:
        return True


# Type alias for Result
Result = Union[Success[T], Failure[E]]


def success(value: T) -> Success[T]:
    """Create a successful result.
    
    Args:
        value: The success value
        
    Returns:
        Success result
    """
    return Success(value)


def failure(error: E, message: str, details: Optional[dict] = None) -> Failure[E]:
    """Create a failure result.
    
    Args:
        error: The error value
        message: Error message
        details: Optional error details
        
    Returns:
        Failure result
    """
    return Failure(error, message, details)

