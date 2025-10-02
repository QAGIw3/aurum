"""Domain repositories for business logic.

Repositories sit between the service layer and the data access layer (DAOs),
providing domain-specific operations while abstracting away database details.

Architecture:
- Repositories implement domain logic and coordinate multiple DAOs
- Services call repositories for business operations
- DAOs handle low-level database operations

Following the Repository Pattern from Domain-Driven Design.
"""

from .curves import CurveRepository
from .scenarios import ScenarioRepository
from .metadata import MetadataRepository
from .ppa import PpaRepository
from .drought import DroughtRepository

__all__ = [
    "CurveRepository",
    "ScenarioRepository",
    "MetadataRepository",
    "PpaRepository",
    "DroughtRepository",
]

