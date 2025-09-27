"""Data Access Objects (DAOs) for domain entities.

DAOs handle all database/storage operations and provide a clean interface
for services to interact with data persistence layers. They abstract away
the details of SQL queries, connection management, and data transformation.

Design Principles:
- Each DAO corresponds to a single domain entity or related set of entities
- DAOs are async-first and support both read and write operations
- Connection management and transaction handling is handled internally
- DAOs return domain models, not raw database records
- Error handling and logging is consistent across all DAOs
"""

from .base_dao import BaseDAO
from .curves_dao import CurvesDAO
from .scenarios_dao import ScenariosDAO
from .cache_dao import CacheDAO

__all__ = [
    "BaseDAO",
    "CurvesDAO",
    "ScenariosDAO",
    "CacheDAO",
]
