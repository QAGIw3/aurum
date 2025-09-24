"""Core interfaces for the refactored maintenance system.

These interfaces define the contracts for maintenance operations, backends,
and execution engines, enabling testability and extensibility.
"""

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Protocol, Union
from enum import Enum


class MaintenanceStatus(str, Enum):
    """Status of a maintenance operation."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class OperationType(str, Enum):
    """Type of maintenance operation."""
    COMPACTION = "compaction"
    RETENTION = "retention"
    PARTITION_EVOLUTION = "partition_evolution"
    ZORDER_OPTIMIZATION = "zorder_optimization"
    VACUUM = "vacuum"
    REWRITE_MANIFESTS = "rewrite_manifests"
    EXPIRE_SNAPSHOTS = "expire_snapshots"
    REMOVE_ORPHANS = "remove_orphans"


@dataclass
class MaintenanceConfig:
    """Configuration for maintenance operations."""
    
    operation_type: OperationType
    target_table: str
    dry_run: bool = False
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: Optional[int] = None
    retry_count: int = 0
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass 
class MaintenanceResult:
    """Result of a maintenance operation."""
    
    operation_id: str
    config: MaintenanceConfig
    status: MaintenanceStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class MaintenanceOperation(Protocol):
    """Protocol for maintenance operations."""
    
    @property
    def operation_type(self) -> OperationType:
        """Get the type of this operation."""
        ...
    
    @property
    def supports_dry_run(self) -> bool:
        """Whether this operation supports dry run mode."""
        ...
    
    async def execute(
        self, 
        backend: MaintenanceBackend,
        config: MaintenanceConfig
    ) -> MaintenanceResult:
        """Execute the maintenance operation."""
        ...
    
    async def validate_config(self, config: MaintenanceConfig) -> List[str]:
        """Validate configuration and return any errors."""
        ...


class MaintenanceBackend(Protocol):
    """Protocol for maintenance backends (Iceberg, Timescale, etc.)."""
    
    @property
    def backend_type(self) -> str:
        """Get the backend type identifier."""
        ...
    
    @property
    def supported_operations(self) -> List[OperationType]:
        """Get list of supported operation types."""
        ...
    
    async def connect(self) -> None:
        """Establish connection to the backend."""
        ...
    
    async def disconnect(self) -> None:
        """Close connection to the backend."""
        ...
    
    async def health_check(self) -> bool:
        """Check if backend is healthy and accessible."""
        ...
    
    async def get_table_metadata(self, table_name: str) -> Dict[str, Any]:
        """Get metadata for a table."""
        ...


class MaintenanceExecutor(abc.ABC):
    """Abstract base class for maintenance executors."""
    
    @abc.abstractmethod
    async def execute_operation(
        self,
        operation: MaintenanceOperation,
        backend: MaintenanceBackend, 
        config: MaintenanceConfig
    ) -> MaintenanceResult:
        """Execute a maintenance operation."""
        ...
    
    @abc.abstractmethod
    async def execute_batch(
        self,
        operations: List[tuple[MaintenanceOperation, MaintenanceBackend, MaintenanceConfig]]
    ) -> List[MaintenanceResult]:
        """Execute multiple maintenance operations."""
        ...
    
    @abc.abstractmethod
    async def get_execution_status(self, operation_id: str) -> Optional[MaintenanceResult]:
        """Get status of a running or completed operation."""
        ...


# Type aliases for convenience
ConfigDict = Dict[str, Any]
MetricsDict = Dict[str, Union[int, float, str]]