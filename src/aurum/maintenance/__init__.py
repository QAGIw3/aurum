"""Refactored maintenance system for Aurum.

This module provides a unified, extensible maintenance system that follows
established Aurum architectural patterns with proper separation of concerns,
dependency injection, and comprehensive testing support.
"""

# Core interfaces are always available
from .interfaces import (
    MaintenanceOperation,
    MaintenanceBackend, 
    MaintenanceExecutor,
    MaintenanceResult,
    MaintenanceConfig,
    MaintenanceStatus,
    OperationType,
)

# Other imports may have dependencies, so import conditionally
try:
    from .backends import (
        IcebergMaintenanceBackend,
        TimescaleMaintenanceBackend, 
        ClickHouseMaintenanceBackend,
    )
    from .operations import (
        CompactionOperation,
        RetentionOperation,
        PartitionEvolutionOperation,
        ZOrderOptimizationOperation,
    )
    from .executor import MaintenanceExecutorImpl
    from .factory import MaintenanceFactory
    
    # Service has external dependencies
    try:
        from .service import MaintenanceService
        _HAS_SERVICE = True
    except ImportError:
        _HAS_SERVICE = False
        MaintenanceService = None
    
    _HAS_IMPLEMENTATIONS = True
    
except ImportError as e:
    # Fallback for missing dependencies
    _HAS_IMPLEMENTATIONS = False
    _HAS_SERVICE = False
    IcebergMaintenanceBackend = None
    TimescaleMaintenanceBackend = None
    ClickHouseMaintenanceBackend = None
    CompactionOperation = None
    RetentionOperation = None
    PartitionEvolutionOperation = None
    ZOrderOptimizationOperation = None
    MaintenanceExecutorImpl = None
    MaintenanceFactory = None
    MaintenanceService = None

__all__ = [
    # Core interfaces (always available)
    "MaintenanceOperation",
    "MaintenanceBackend", 
    "MaintenanceExecutor",
    "MaintenanceResult",
    "MaintenanceConfig",
    "MaintenanceStatus",
    "OperationType",
    
    # Implementation classes (may be None if dependencies missing)
    "IcebergMaintenanceBackend",
    "TimescaleMaintenanceBackend",
    "ClickHouseMaintenanceBackend",
    "CompactionOperation",
    "RetentionOperation", 
    "PartitionEvolutionOperation",
    "ZOrderOptimizationOperation",
    "MaintenanceExecutorImpl",
    "MaintenanceFactory",
    "MaintenanceService",
]