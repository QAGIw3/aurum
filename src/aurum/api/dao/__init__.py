"""Data Access Objects (DAO) for domain-specific data operations.

This package implements the DAO pattern to separate data access logic from business logic.
Each domain has its own DAO class that handles database queries and caching for that domain.
"""

from .curves_dao import CurvesDao
from .drought_dao import DroughtDao
from .eia_dao import EiaDao
from .iso_dao import IsoDao
from .metadata_dao import MetadataDao
from .ppa_dao import PpaDao
from .scenario_dao import ScenarioDao

__all__ = [
    "CurvesDao",
    "DroughtDao",
    "EiaDao",
    "IsoDao",
    "MetadataDao",
    "PpaDao",
    "ScenarioDao",
]