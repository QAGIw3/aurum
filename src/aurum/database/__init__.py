"""Unified database connection management for Aurum platform.

This module provides standardized connection pooling and management across
all database types (Trino, TimescaleDB, ClickHouse, PostgreSQL).
"""

from .connection_manager import (
    ConnectionConfig,
    ConnectionManagerRegistry,
    ConnectionPoolManager,
    DatabaseConnection,
    PoolConfig,
    PoolMetrics,
    create_connection_config_from_settings,
    create_pool_config_from_settings,
    get_connection_manager_registry,
)
from .pool_factory import DatabasePoolFactory
from .trino_pool_manager import TrinoPoolManager
from .timescale_pool_manager import TimescalePoolManager
from .health_monitor import (
    DatabaseHealthMonitor,
    HealthCheckResult,
    PoolHealthConfig,
    get_database_health_monitor,
    run_database_health_checks,
    get_database_health_status,
    start_database_monitoring,
    stop_database_monitoring,
)

__all__ = [
    # Core interfaces
    "ConnectionConfig",
    "ConnectionManagerRegistry",
    "ConnectionPoolManager",
    "DatabaseConnection",
    "PoolConfig",
    "PoolMetrics",

    # Factory and utilities
    "DatabasePoolFactory",
    "create_connection_config_from_settings",
    "create_pool_config_from_settings",
    "get_connection_manager_registry",

    # Health monitoring
    "DatabaseHealthMonitor",
    "HealthCheckResult",
    "PoolHealthConfig",
    "get_database_health_monitor",
    "run_database_health_checks",
    "get_database_health_status",
    "start_database_monitoring",
    "stop_database_monitoring",

    # Concrete implementations
    "TrinoPoolManager",
    "TimescalePoolManager",
]
