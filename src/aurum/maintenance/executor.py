"""Maintenance executor implementation."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor

from .interfaces import (
    MaintenanceBackend,
    MaintenanceConfig,
    MaintenanceExecutor, 
    MaintenanceOperation,
    MaintenanceResult,
    MaintenanceStatus,
)

LOGGER = logging.getLogger(__name__)


class MaintenanceExecutorImpl(MaintenanceExecutor):
    """Implementation of maintenance executor with async support."""
    
    def __init__(
        self, 
        max_workers: int = 4,
        max_concurrent_operations: int = 10
    ) -> None:
        self._max_workers = max_workers
        self._max_concurrent_operations = max_concurrent_operations
        self._thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self._semaphore = asyncio.Semaphore(max_concurrent_operations)
        self._running_operations: Dict[str, MaintenanceResult] = {}
    
    async def execute_operation(
        self,
        operation: MaintenanceOperation,
        backend: MaintenanceBackend,
        config: MaintenanceConfig
    ) -> MaintenanceResult:
        """Execute a single maintenance operation."""
        async with self._semaphore:
            # Ensure backend is connected
            await backend.connect()
            
            try:
                # Execute the operation
                result = await operation.execute(backend, config)
                
                # Store result for status tracking
                self._running_operations[result.operation_id] = result
                
                return result
                
            except Exception as e:
                LOGGER.error("Operation execution failed: %s", e)
                raise
            finally:
                # Backend cleanup is handled by the backend itself
                pass
    
    async def execute_batch(
        self,
        operations: List[tuple[MaintenanceOperation, MaintenanceBackend, MaintenanceConfig]]
    ) -> List[MaintenanceResult]:
        """Execute multiple maintenance operations concurrently."""
        
        async def execute_single(op_tuple: tuple) -> MaintenanceResult:
            operation, backend, config = op_tuple
            return await self.execute_operation(operation, backend, config)
        
        # Execute all operations concurrently with semaphore limiting
        tasks = [execute_single(op_tuple) for op_tuple in operations]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Convert exceptions to failed results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Create a failed result for exceptions
                operation, backend, config = operations[i]
                failed_result = MaintenanceResult(
                    operation_id=f"failed-{i}",
                    config=config,
                    status=MaintenanceStatus.FAILED,
                    started_at=config.tags.get("started_at", "unknown"),
                    error_message=str(result),
                )
                processed_results.append(failed_result)
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def get_execution_status(self, operation_id: str) -> Optional[MaintenanceResult]:
        """Get status of a running or completed operation."""
        return self._running_operations.get(operation_id)
    
    def cleanup(self) -> None:
        """Clean up resources."""
        self._thread_pool.shutdown(wait=True)
        self._running_operations.clear()