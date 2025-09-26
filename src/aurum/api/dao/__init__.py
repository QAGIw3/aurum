"""Data Access Objects (DAO) for domain-specific data operations.

This package implements the DAO pattern to separate data access logic from business logic.
Each domain has its own DAO class that handles database queries and caching for that domain.
"""

from .eia_dao import EiaDao

__all__ = [
    "EiaDao",
]