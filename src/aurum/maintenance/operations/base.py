"""Base implementation for maintenance operations."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List

from ..interfaces import (
    MaintenanceBackend,
    MaintenanceConfig, 
    MaintenanceOperation,
    MaintenanceResult,
    MaintenanceStatus,
    OperationType,
)

LOGGER = logging.getLogger(__name__)


class BaseMaintenanceOperation:
    """Base implementation for maintenance operations."""
    
    def __init__(self, operation_type: OperationType) -> None:
        self._operation_type = operation_type
    
    @property
    def operation_type(self) -> OperationType:
        """Get the type of this operation."""
        return self._operation_type
    
    @property
    def supports_dry_run(self) -> bool:
        """Whether this operation supports dry run mode."""
        return True
    
    async def execute(
        self, 
        backend: MaintenanceBackend,
        config: MaintenanceConfig
    ) -> MaintenanceResult:
        """Execute the maintenance operation."""
        operation_id = str(uuid.uuid4())
        started_at = datetime.now(timezone.utc)
        
        # Validate configuration first
        validation_errors = await self.validate_config(config)
        if validation_errors:
            return MaintenanceResult(
                operation_id=operation_id,
                config=config,
                status=MaintenanceStatus.FAILED,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                error_message=f"Configuration validation failed: {'; '.join(validation_errors)}",
            )
        
        try:
            LOGGER.info(
                "Starting %s operation for table %s (operation_id: %s)", 
                self.operation_type.value, config.target_table, operation_id
            )
            
            # Check if backend is healthy
            if not await backend.health_check():
                raise RuntimeError(f"Backend {backend.backend_type} is not healthy")
            
            # Execute the operation
            start_time = time.time()
            metrics = await self._execute_impl(backend, config)
            end_time = time.time()
            
            completed_at = datetime.now(timezone.utc)
            duration_seconds = end_time - start_time
            
            LOGGER.info(
                "Completed %s operation for table %s in %.2f seconds", 
                self.operation_type.value, config.target_table, duration_seconds
            )
            
            return MaintenanceResult(
                operation_id=operation_id,
                config=config,
                status=MaintenanceStatus.COMPLETED,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration_seconds,
                metrics=metrics,
            )
            
        except Exception as e:
            completed_at = datetime.now(timezone.utc)
            duration_seconds = (completed_at - started_at).total_seconds()
            
            LOGGER.error(
                "Failed %s operation for table %s after %.2f seconds: %s", 
                self.operation_type.value, config.target_table, duration_seconds, e
            )
            
            return MaintenanceResult(
                operation_id=operation_id,
                config=config,
                status=MaintenanceStatus.FAILED,
                started_at=started_at,
                completed_at=completed_at,
                duration_seconds=duration_seconds,
                error_message=str(e),
            )
    
    async def validate_config(self, config: MaintenanceConfig) -> List[str]:
        """Validate configuration and return any errors."""
        errors = []
        
        # Basic validation
        if not config.target_table:
            errors.append("target_table is required")
        
        if config.timeout_seconds is not None and config.timeout_seconds <= 0:
            errors.append("timeout_seconds must be positive")
        
        if config.retry_count < 0:
            errors.append("retry_count must be non-negative")
        
        # Allow subclasses to add specific validation
        specific_errors = await self._validate_config_impl(config)
        errors.extend(specific_errors)
        
        return errors
    
    async def _execute_impl(
        self, 
        backend: MaintenanceBackend,
        config: MaintenanceConfig
    ) -> Dict[str, Any]:
        """Implementation-specific execution logic.
        
        Subclasses should override this method to implement their specific logic.
        
        Returns:
            Dictionary of metrics from the operation.
        """
        raise NotImplementedError("Subclasses must implement _execute_impl")
    
    async def _validate_config_impl(self, config: MaintenanceConfig) -> List[str]:
        """Implementation-specific configuration validation.
        
        Subclasses can override this to add specific validation rules.
        
        Returns:
            List of validation error messages.
        """
        return []