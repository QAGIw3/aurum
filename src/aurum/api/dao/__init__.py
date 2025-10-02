"""Data Access Objects (DAO) for domain-specific data operations.

This package implements the DAO pattern to separate data access logic from business logic.
Each domain has its own DAO class that handles database queries and caching for that domain.

Async DAO Classes:
- BaseAsyncDao: Abstract base class for async DAOs with connection pooling
- TrinoAsyncDao: Async DAO for Trino federated queries
- ClickHouseAsyncDao: Async DAO for ClickHouse OLAP operations
- TimescaleAsyncDao: Async DAO for TimescaleDB time-series operations
- EiaAsyncDao: Async DAO for EIA data operations

Note: All DAOs are now async. Legacy sync DAOs have been migrated or removed.
"""

# New async DAOs with connection pooling
from .base_async_dao import BaseAsyncDao
from .trino_async_dao import TrinoAsyncDao
from .clickhouse_async_dao import ClickHouseAsyncDao
from .timescale_async_dao import TimescaleAsyncDao
from .eia_async_dao import EiaAsyncDao

__all__ = [
    # New async DAOs with connection pooling
    "BaseAsyncDao",
    "TrinoAsyncDao",
    "ClickHouseAsyncDao",
    "TimescaleAsyncDao",
    "EiaAsyncDao",
]
