"""Database functionality for the Aurum API."""

from .performance import router as performance_router
from .query_analysis import router as query_analysis_router
from .optimization import router as optimization_router
from .connections import router as connections_router
from .health import router as health_router
from .database_monitor import (
    get_database_monitor,
    initialize_database_monitoring,
    DatabaseMonitor,
    QueryMetrics,
    QueryPattern,
    OptimizationSuggestion,
    QueryPerformanceLevel,
    OptimizationType,
)

__all__ = [
    "performance_router",
    "query_analysis_router",
    "optimization_router",
    "connections_router",
    "health_router",
    "get_database_monitor",
    "initialize_database_monitoring",
    "DatabaseMonitor",
    "QueryMetrics",
    "QueryPattern",
    "OptimizationSuggestion",
    "QueryPerformanceLevel",
    "OptimizationType",
]
