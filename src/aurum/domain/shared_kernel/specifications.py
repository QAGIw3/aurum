"""Specification pattern for domain queries.

Specifications encapsulate business rules and query logic in a reusable,
composable way.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar('T')


class Specification(ABC, Generic[T]):
    """Base specification interface.
    
    Specifications allow you to encapsulate business rules that can be
    combined and reused.
    """
    
    @abstractmethod
    def is_satisfied_by(self, candidate: T) -> bool:
        """Check if a candidate satisfies this specification.
        
        Args:
            candidate: The object to check
            
        Returns:
            True if the specification is satisfied, False otherwise
        """
        pass
    
    def and_(self, other: Specification[T]) -> Specification[T]:
        """Combine two specifications with AND logic."""
        return AndSpecification(self, other)
    
    def or_(self, other: Specification[T]) -> Specification[T]:
        """Combine two specifications with OR logic."""
        return OrSpecification(self, other)
    
    def not_(self) -> Specification[T]:
        """Negate this specification."""
        return NotSpecification(self)


class AndSpecification(Specification[T]):
    """Specification that combines two specifications with AND logic."""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def is_satisfied_by(self, candidate: T) -> bool:
        return self.left.is_satisfied_by(candidate) and self.right.is_satisfied_by(candidate)


class OrSpecification(Specification[T]):
    """Specification that combines two specifications with OR logic."""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def is_satisfied_by(self, candidate: T) -> bool:
        return self.left.is_satisfied_by(candidate) or self.right.is_satisfied_by(candidate)


class NotSpecification(Specification[T]):
    """Specification that negates another specification."""
    
    def __init__(self, spec: Specification[T]):
        self.spec = spec
    
    def is_satisfied_by(self, candidate: T) -> bool:
        return not self.spec.is_satisfied_by(candidate)

