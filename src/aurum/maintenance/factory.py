"""Factory for creating maintenance components."""

from __future__ import annotations

from typing import Any, Dict, Optional, Type

from .interfaces import MaintenanceBackend, MaintenanceOperation, OperationType
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


class MaintenanceFactory:
    """Factory for creating maintenance backends and operations."""
    
    # Registry of backend types to classes
    _backend_registry: Dict[str, Type[MaintenanceBackend]] = {
        "iceberg": IcebergMaintenanceBackend,
        "timescale": TimescaleMaintenanceBackend,
        "clickhouse": ClickHouseMaintenanceBackend,
    }
    
    # Registry of operation types to classes
    _operation_registry: Dict[OperationType, Type[MaintenanceOperation]] = {
        OperationType.COMPACTION: CompactionOperation,
        OperationType.RETENTION: RetentionOperation,
        OperationType.PARTITION_EVOLUTION: PartitionEvolutionOperation,
        OperationType.ZORDER_OPTIMIZATION: ZOrderOptimizationOperation,
    }
    
    @classmethod
    def create_backend(
        self, 
        backend_type: str, 
        connection_config: Dict[str, Any]
    ) -> MaintenanceBackend:
        """Create a maintenance backend of the specified type."""
        backend_class = self._backend_registry.get(backend_type)
        if backend_class is None:
            raise ValueError(f"Unknown backend type: {backend_type}")
        
        return backend_class(connection_config)
    
    @classmethod
    def create_operation(self, operation_type: OperationType) -> MaintenanceOperation:
        """Create a maintenance operation of the specified type."""
        operation_class = self._operation_registry.get(operation_type)
        if operation_class is None:
            raise ValueError(f"Unknown operation type: {operation_type}")
        
        return operation_class()
    
    @classmethod
    def register_backend(
        self, 
        backend_type: str, 
        backend_class: Type[MaintenanceBackend]
    ) -> None:
        """Register a new backend type."""
        self._backend_registry[backend_type] = backend_class
    
    @classmethod
    def register_operation(
        self,
        operation_type: OperationType,
        operation_class: Type[MaintenanceOperation]
    ) -> None:
        """Register a new operation type."""
        self._operation_registry[operation_type] = operation_class
    
    @classmethod
    def get_supported_backends(self) -> list[str]:
        """Get list of supported backend types."""
        return list(self._backend_registry.keys())
    
    @classmethod
    def get_supported_operations(self) -> list[OperationType]:
        """Get list of supported operation types."""
        return list(self._operation_registry.keys())