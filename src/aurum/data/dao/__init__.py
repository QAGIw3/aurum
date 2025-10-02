"""Data Access Objects (DAO) for database operations.

This package provides a unified, async-first data access layer supporting
multiple backend databases (Trino, TimescaleDB, ClickHouse, Postgres).

Architecture:
- DAOs handle low-level database operations and connection management
- Repositories (in ../repositories/) handle domain-specific logic
- Services (in ../../services/) orchestrate business logic

All DAOs are async and use connection pooling for optimal performance.
"""

from .base import BaseAsyncDAO, DAOError, ConnectionError, QueryError
from .trino import TrinoDAO
from .timescale import TimescaleDAO
from .clickhouse import ClickHouseDAO
from .postgres import PostgresDAO

__all__ = [
    # Base classes and exceptions
    "BaseAsyncDAO",
    "DAOError",
    "ConnectionError",
    "QueryError",
    
    # Backend-specific DAOs
    "TrinoDAO",
    "TimescaleDAO",
    "ClickHouseDAO",
    "PostgresDAO",
]

