"""Domain-level exceptions."""

from __future__ import annotations

from typing import Any, Dict, Optional


class DomainException(Exception):
    """Base exception for all domain-level errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ValidationError(DomainException):
    """Raised when domain validation fails."""
    pass


class BusinessRuleViolation(DomainException):
    """Raised when a business rule is violated."""
    pass


class EntityNotFoundError(DomainException):
    """Raised when an entity cannot be found."""
    
    def __init__(self, entity_type: str, entity_id: Any):
        super().__init__(
            f"{entity_type} with id {entity_id} not found",
            {"entity_type": entity_type, "entity_id": str(entity_id)}
        )


class AggregateVersionConflict(DomainException):
    """Raised when there's a version conflict in aggregate update."""
    
    def __init__(self, expected_version: int, actual_version: int):
        super().__init__(
            f"Aggregate version conflict: expected {expected_version}, got {actual_version}",
            {"expected_version": expected_version, "actual_version": actual_version}
        )

