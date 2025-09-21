"""Performance optimization module for Aurum data platform.

This module provides comprehensive performance optimization strategies including:

1. **Batching**: Efficient batch processing for data ingestion
2. **Caching**: Multi-level caching with Redis and in-memory backends
3. **Connection Pooling**: Database and service connection pooling
4. **Partitioning**: Database partitioning for query performance
5. **Orchestration**: Global performance optimization management

Example Usage:
    from aurum.performance.orchestrator import initialize_performance_orchestrator, get_performance_report

    # Initialize performance optimizations
    orchestrator = initialize_performance_orchestrator()

    # Get performance metrics
    report = get_performance_report()
    print(f"Cache hit rate: {report['metrics']['cache_hit_rate']:.2%}")
"""

from .batching import BatchConfig, BatchProcessor, DataFrameBatcher, get_batch_manager
from .caching import CacheConfig, CacheBackend, InMemoryCache, MultiLevelCache, get_memory_cache
from .connection_pool import PoolConfig, ConnectionPool, DatabaseConnectionPool, get_database_pool
from .partitioning import PartitionConfig, PartitionManager, TimescaleDBPartitionManager
from .orchestrator import PerformanceOrchestrator, get_performance_orchestrator, get_performance_report
from .config import (
    GlobalPerformanceSettings,
    OptimizedIngestionSettings,
    DatabaseOptimizationSettings,
    PerformanceMonitoringSettings,
    create_performance_config_from_env
)

__all__ = [
    # Batching
    "BatchConfig",
    "BatchProcessor",
    "DataFrameBatcher",
    "get_batch_manager",

    # Caching
    "CacheConfig",
    "CacheBackend",
    "InMemoryCache",
    "MultiLevelCache",
    "get_memory_cache",

    # Connection Pooling
    "PoolConfig",
    "ConnectionPool",
    "DatabaseConnectionPool",
    "get_database_pool",

    # Partitioning
    "PartitionConfig",
    "PartitionManager",
    "TimescaleDBPartitionManager",

    # Orchestration
    "PerformanceOrchestrator",
    "get_performance_orchestrator",
    "get_performance_report",

    # Configuration
    "GlobalPerformanceSettings",
    "OptimizedIngestionSettings",
    "DatabaseOptimizationSettings",
    "PerformanceMonitoringSettings",
    "create_performance_config_from_env",
]
