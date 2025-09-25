"""Scenario-specific exception re-exports for backwards compatibility."""

from ..exceptions import (
    ForbiddenException,
    NotFoundException,
    ValidationException,
)

__all__ = [
    "ForbiddenException",
    "NotFoundException",
    "ValidationException",
]
