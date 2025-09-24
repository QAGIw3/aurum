"""High-level maintenance service following Aurum patterns."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from aurum.core import AurumSettings

from .interfaces import (
    MaintenanceConfig,
    MaintenanceResult,
    OperationType,
)
from .factory import MaintenanceFactory
from .executor import MaintenanceExecutorImpl

LOGGER = logging.getLogger(__name__)


class MaintenanceService:
    """High-level service for managing maintenance operations.
    
    This service follows the established Aurum patterns for configuration
    and dependency management, providing a clean interface for maintenance
    operations across different storage backends.
    """
    
    def __init__(
        self, 
        settings: Optional[AurumSettings] = None,
        executor: Optional[MaintenanceExecutorImpl] = None
    ) -> None:
        self._settings = settings or AurumSettings.from_env()
        self._executor = executor or MaintenanceExecutorImpl()
        self._factory = MaintenanceFactory()
        self._backends: Dict[str, Any] = {}
    
    async def execute_maintenance(
        self,
        backend_type: str,
        operation_type: OperationType,
        target_table: str,
        *,
        dry_run: bool = False,
        parameters: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
        retry_count: int = 0,
        tags: Optional[Dict[str, str]] = None
    ) -> MaintenanceResult:
        """Execute a single maintenance operation.
        
        Args:
            backend_type: Type of storage backend (iceberg, timescale, clickhouse)
            operation_type: Type of maintenance operation
            target_table: Name of the table to operate on
            dry_run: Whether to run in dry-run mode
            parameters: Operation-specific parameters
            timeout_seconds: Operation timeout
            retry_count: Number of retries on failure
            tags: Additional tags for tracking
            
        Returns:
            Result of the maintenance operation
        """
        # Create maintenance config
        config = MaintenanceConfig(
            operation_type=operation_type,
            target_table=target_table,
            dry_run=dry_run,
            parameters=parameters or {},
            timeout_seconds=timeout_seconds,
            retry_count=retry_count,
            tags=tags or {},
        )
        
        # Get or create backend
        backend = await self._get_backend(backend_type)
        
        # Create operation
        operation = self._factory.create_operation(operation_type)
        
        # Execute the operation
        result = await self._executor.execute_operation(operation, backend, config)
        
        LOGGER.info(
            "Maintenance operation completed: %s %s on %s (status: %s)",
            backend_type, operation_type.value, target_table, result.status.value
        )
        
        return result
    
    async def execute_batch_maintenance(
        self,
        operations: List[Dict[str, Any]]
    ) -> List[MaintenanceResult]:
        """Execute multiple maintenance operations.
        
        Args:
            operations: List of operation specifications, each containing:
                - backend_type: Storage backend type
                - operation_type: Maintenance operation type  
                - target_table: Table name
                - Optional: dry_run, parameters, timeout_seconds, retry_count, tags
                
        Returns:
            List of maintenance operation results
        """
        # Prepare operation tuples
        operation_tuples = []
        
        for op_spec in operations:
            # Extract required fields
            backend_type = op_spec["backend_type"]
            operation_type = OperationType(op_spec["operation_type"])
            target_table = op_spec["target_table"]
            
            # Create config
            config = MaintenanceConfig(
                operation_type=operation_type,
                target_table=target_table,
                dry_run=op_spec.get("dry_run", False),
                parameters=op_spec.get("parameters", {}),
                timeout_seconds=op_spec.get("timeout_seconds"),
                retry_count=op_spec.get("retry_count", 0),
                tags=op_spec.get("tags", {}),
            )
            
            # Get backend and operation
            backend = await self._get_backend(backend_type)
            operation = self._factory.create_operation(operation_type)
            
            operation_tuples.append((operation, backend, config))
        
        # Execute batch
        results = await self._executor.execute_batch(operation_tuples)
        
        LOGGER.info("Batch maintenance completed: %d operations", len(results))
        
        return results
    
    async def get_operation_status(self, operation_id: str) -> Optional[MaintenanceResult]:
        """Get the status of a maintenance operation."""
        return await self._executor.get_execution_status(operation_id)
    
    async def _get_backend(self, backend_type: str) -> Any:
        """Get or create a backend instance."""
        if backend_type not in self._backends:
            # Create backend with configuration from settings
            connection_config = self._get_backend_config(backend_type)
            backend = self._factory.create_backend(backend_type, connection_config)
            
            # Connect the backend
            await backend.connect()
            
            self._backends[backend_type] = backend
        
        return self._backends[backend_type]
    
    def _get_backend_config(self, backend_type: str) -> Dict[str, Any]:
        """Get backend configuration from settings."""
        if backend_type == "iceberg":
            # Use Trino connection for Iceberg
            return {
                "host": getattr(self._settings.data_backend, "trino_host", "localhost"),
                "port": getattr(self._settings.data_backend, "trino_port", 8080),
                "user": getattr(self._settings.data_backend, "trino_user", "aurum"),
                "catalog": getattr(self._settings.data_backend, "iceberg_catalog", "iceberg"),
            }
        elif backend_type == "timescale":
            return {
                "host": getattr(self._settings.data_backend, "timescale_host", "localhost"), 
                "port": getattr(self._settings.data_backend, "timescale_port", 5432),
                "user": getattr(self._settings.data_backend, "timescale_user", "timescale"),
                "password": getattr(self._settings.data_backend, "timescale_password", ""),
                "database": getattr(self._settings.data_backend, "timescale_database", "timeseries"),
            }
        elif backend_type == "clickhouse":
            return {
                "host": getattr(self._settings.data_backend, "clickhouse_host", "localhost"),
                "port": getattr(self._settings.data_backend, "clickhouse_port", 9000), 
                "user": getattr(self._settings.data_backend, "clickhouse_user", "default"),
                "password": getattr(self._settings.data_backend, "clickhouse_password", ""),
                "database": getattr(self._settings.data_backend, "clickhouse_database", "default"),
            }
        else:
            raise ValueError(f"Unknown backend type: {backend_type}")
    
    async def cleanup(self) -> None:
        """Clean up resources."""
        # Disconnect all backends
        for backend in self._backends.values():
            await backend.disconnect()
        
        # Clean up executor
        self._executor.cleanup()
        
        LOGGER.info("Maintenance service cleanup completed")