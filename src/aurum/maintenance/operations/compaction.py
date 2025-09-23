"""Compaction operation implementation."""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from ..interfaces import MaintenanceBackend, MaintenanceConfig, OperationType
from .base import BaseMaintenanceOperation

LOGGER = logging.getLogger(__name__)


class CompactionOperation(BaseMaintenanceOperation):
    """Compaction operation for optimizing file sizes."""
    
    def __init__(self) -> None:
        super().__init__(OperationType.COMPACTION)
    
    async def _validate_config_impl(self, config: MaintenanceConfig) -> List[str]:
        """Validate compaction-specific configuration."""
        errors = []
        
        # Check for required parameters
        params = config.parameters
        
        # target_file_size_mb is commonly used
        if "target_file_size_mb" in params:
            target_size = params["target_file_size_mb"]
            if not isinstance(target_size, (int, float)) or target_size <= 0:
                errors.append("target_file_size_mb must be a positive number")
        
        return errors
    
    async def _execute_impl(
        self, 
        backend: MaintenanceBackend,
        config: MaintenanceConfig
    ) -> Dict[str, Any]:
        """Execute compaction operation."""
        params = config.parameters
        
        if backend.backend_type == "iceberg":
            # Use Iceberg-specific compaction
            from .backends.iceberg import IcebergMaintenanceBackend
            
            if isinstance(backend, IcebergMaintenanceBackend):
                # Map parameters to the existing function signature
                kwargs = {
                    "dry_run": config.dry_run,
                }
                
                # Map common parameters
                if "target_file_size_mb" in params:
                    kwargs["target_file_size_mb"] = params["target_file_size_mb"]
                
                # Execute using existing rewrite_data_files function
                result = await backend.execute_iceberg_operation(
                    "rewrite_data_files",
                    config.target_table,
                    **kwargs
                )
                
                # Extract metrics from the result
                metrics = {
                    "files_rewritten": result.get("rewritten_files", 0),
                    "files_added": result.get("added_files", 0), 
                    "files_deleted": result.get("deleted_files", 0),
                    "bytes_rewritten": result.get("rewritten_bytes", 0),
                    "target_file_size_mb": kwargs.get("target_file_size_mb", 0),
                    "dry_run": config.dry_run,
                }
                
                return metrics
        
        # For other backends, implement accordingly
        raise NotImplementedError(f"Compaction not implemented for backend: {backend.backend_type}")