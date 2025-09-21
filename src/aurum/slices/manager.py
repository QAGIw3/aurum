"""Slice manager for coordinating slice processing and retry logic."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Union
from enum import Enum

from .client import SliceClient, SliceConfig, SliceStatus, SliceType, SliceInfo, SliceQuery
from ..logging import create_logger, LogLevel


class RetryPolicy(str, Enum):
    """Retry policies for failed slices."""

    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIXED_DELAY = "fixed_delay"
    IMMEDIATE = "immediate"


class SliceProcessingError(Exception):
    """Exception raised during slice processing."""
    pass


@dataclass
class SliceProcessingResult:
    """Result of slice processing operation."""

    slice_id: str
    success: bool
    records_processed: int = 0
    processing_time_seconds: float = 0.0
    error_message: Optional[str] = None
    retryable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class SliceManager:
    """Manager for coordinating slice processing and retry logic."""

    def __init__(self, client: SliceClient):
        """Initialize slice manager.

        Args:
            client: Slice client instance
        """
        self.client = client
        self.logger = create_logger(
            source_name="slice_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.slices.management",
            dataset="slice_management"
        )

        # Worker management
        self.workers: Dict[str, Dict[str, Any]] = {}
        self.worker_tasks: Dict[str, asyncio.Task] = {}

        # Processing configuration
        self.max_concurrent_workers = 5
        self.worker_heartbeat_interval = 30  # seconds
        self.processing_callbacks: List[Callable] = []

    async def start_worker(
        self,
        worker_id: str,
        source_name: Optional[str] = None,
        slice_types: Optional[List[SliceType]] = None,
        max_slices: int = 10
    ) -> None:
        """Start a worker to process slices.

        Args:
            worker_id: Unique worker identifier
            source_name: Optional source name filter
            slice_types: Optional slice type filter
            max_slices: Maximum slices to process before stopping
        """
        if worker_id in self.workers:
            raise SliceProcessingError(f"Worker {worker_id} already exists")

        self.workers[worker_id] = {
            "source_name": source_name,
            "slice_types": slice_types or list(SliceType),
            "max_slices": max_slices,
            "processed_count": 0,
            "start_time": datetime.now(),
            "last_heartbeat": datetime.now()
        }

        # Start worker task
        task = asyncio.create_task(
            self._worker_loop(worker_id)
        )
        self.worker_tasks[worker_id] = task

        self.logger.log(
            LogLevel.INFO,
            f"Started slice worker: {worker_id}",
            "slice_worker_started",
            worker_id=worker_id,
            source_name=source_name,
            slice_types=[t.value for t in slice_types] if slice_types else "all",
            max_slices=max_slices
        )

    async def stop_worker(self, worker_id: str) -> None:
        """Stop a worker.

        Args:
            worker_id: Worker identifier
        """
        if worker_id not in self.workers:
            self.logger.log(
                LogLevel.WARNING,
                f"Worker {worker_id} not found",
                "slice_worker_not_found",
                worker_id=worker_id
            )
            return

        # Cancel worker task
        if worker_id in self.worker_tasks:
            task = self.worker_tasks[worker_id]
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self.worker_tasks[worker_id]

        # Remove worker
        del self.workers[worker_id]

        self.logger.log(
            LogLevel.INFO,
            f"Stopped slice worker: {worker_id}",
            "slice_worker_stopped",
            worker_id=worker_id
        )

    async def _worker_loop(self, worker_id: str) -> None:
        """Main worker loop for processing slices.

        Args:
            worker_id: Worker identifier
        """
        worker_info = self.workers[worker_id]

        self.logger.log(
            LogLevel.INFO,
            f"Slice worker {worker_id} starting loop",
            "slice_worker_loop_started",
            worker_id=worker_id
        )

        try:
            while worker_info["processed_count"] < worker_info["max_slices"]:
                # Get next slice to process
                slice_info = await self._get_next_slice(worker_id, worker_info)

                if not slice_info:
                    # No slices available, wait before checking again
                    await asyncio.sleep(10)
                    continue

                # Process the slice
                result = await self._process_slice(worker_id, slice_info)

                worker_info["processed_count"] += 1

                # Update worker heartbeat
                worker_info["last_heartbeat"] = datetime.now()

                # Trigger callbacks
                await self._trigger_callbacks("slice_processed", {
                    "worker_id": worker_id,
                    "slice_id": slice_info.id,
                    "result": result.__dict__
                })

                # Small delay between slices
                await asyncio.sleep(1)

        except asyncio.CancelledError:
            self.logger.log(
                LogLevel.INFO,
                f"Slice worker {worker_id} cancelled",
                "slice_worker_cancelled",
                worker_id=worker_id
            )
            raise
        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Slice worker {worker_id} error: {e}",
                "slice_worker_error",
                worker_id=worker_id,
                error=str(e)
            )
            raise

    async def _get_next_slice(self, worker_id: str, worker_info: Dict[str, Any]) -> Optional[SliceInfo]:
        """Get the next slice to process.

        Args:
            worker_id: Worker identifier
            worker_info: Worker information

        Returns:
            Next slice to process or None
        """
        source_name = worker_info["source_name"]
        slice_types = worker_info["slice_types"]

        # First, try to get slices ready for retry
        retry_slices = await self.client.get_slices_for_retry(max_slices=1)

        if retry_slices:
            slice_info = retry_slices[0]
            # Check if this worker can process this slice type
            if slice_info.slice_type in slice_types:
                await self.client.start_slice(slice_info.id, worker_id)
                return slice_info

        # Then, try to get pending slices
        for slice_type in slice_types:
            pending_slices = await self.client.get_pending_slices(
                max_slices=1,
                source_name=source_name,
                slice_type=slice_type
            )

            if pending_slices:
                slice_info = pending_slices[0]
                await self.client.start_slice(slice_info.id, worker_id)
                return slice_info

        return None

    async def _process_slice(self, worker_id: str, slice_info: SliceInfo) -> SliceProcessingResult:
        """Process a single slice.

        Args:
            worker_id: Worker identifier
            slice_info: Slice information

        Returns:
            Processing result
        """
        start_time = datetime.now()

        self.logger.log(
            LogLevel.INFO,
            f"Worker {worker_id} processing slice {slice_info.id}",
            "slice_processing_started",
            worker_id=worker_id,
            slice_id=slice_info.id,
            slice_type=slice_info.slice_type.value,
            slice_key=slice_info.slice_key
        )

        try:
            # Simulate slice processing based on type
            result = await self._execute_slice_processing(slice_info)

            # Complete the slice
            if result.success:
                await self.client.complete_slice(
                    slice_info.id,
                    result.records_processed,
                    result.metadata
                )
            else:
                await self.client.fail_slice(
                    slice_info.id,
                    result.error_message or "Processing failed",
                    result.metadata
                )

            processing_time = (datetime.now() - start_time).total_seconds()

            self.logger.log(
                LogLevel.INFO,
                f"Worker {worker_id} completed slice {slice_info.id}",
                "slice_processing_completed",
                worker_id=worker_id,
                slice_id=slice_info.id,
                success=result.success,
                processing_time_seconds=processing_time
            )

            return result

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()

            error_result = SliceProcessingResult(
                slice_id=slice_info.id,
                success=False,
                processing_time_seconds=processing_time,
                error_message=str(e),
                retryable=True,
                metadata={"worker_id": worker_id, "error": str(e)}
            )

            await self.client.fail_slice(
                slice_info.id,
                str(e),
                error_result.metadata
            )

            self.logger.log(
                LogLevel.ERROR,
                f"Worker {worker_id} failed slice {slice_info.id}: {e}",
                "slice_processing_failed",
                worker_id=worker_id,
                slice_id=slice_info.id,
                error=str(e)
            )

            return error_result

    async def _execute_slice_processing(self, slice_info: SliceInfo) -> SliceProcessingResult:
        """Execute the actual slice processing logic.

        Args:
            slice_info: Slice information

        Returns:
            Processing result
        """
        # This would integrate with actual processing systems
        # For now, simulate processing based on slice type

        await asyncio.sleep(2)  # Simulate processing time

        # Simulate different processing results based on slice type
        if slice_info.slice_type == SliceType.TIME_WINDOW:
            records = 1000 + (hash(slice_info.slice_key) % 5000)
            success = hash(slice_info.slice_key) % 10 != 0  # 90% success rate
        elif slice_info.slice_type == SliceType.DATASET:
            records = 500 + (hash(slice_info.slice_key) % 2000)
            success = hash(slice_info.slice_key) % 8 != 0  # 87.5% success rate
        elif slice_info.slice_type == SliceType.FAILED_RECORDS:
            records = 100 + (hash(slice_info.slice_key) % 500)
            success = hash(slice_info.slice_key) % 5 != 0  # 80% success rate
        else:
            records = 200 + (hash(slice_info.slice_key) % 1000)
            success = True

        if not success:
            error_message = f"Simulated processing failure for {slice_info.slice_key}"
            return SliceProcessingResult(
                slice_id=slice_info.id,
                success=False,
                error_message=error_message,
                retryable=True
            )

        return SliceProcessingResult(
            slice_id=slice_info.id,
            success=True,
            records_processed=records,
            processing_time_seconds=2.0,
            metadata={
                "slice_type": slice_info.slice_type.value,
                "slice_key": slice_info.slice_key,
                "records_processed": records
            }
        )

    async def create_slices_from_operation(
        self,
        source_name: str,
        operation_type: str,
        operation_data: Dict[str, Any],
        slice_configs: List[Dict[str, Any]]
    ) -> List[str]:
        """Create slices from a higher-level operation.

        Args:
            source_name: Name of the data source
            operation_type: Type of operation (backfill, replay, etc.)
            operation_data: Operation metadata
            slice_configs: Configuration for individual slices

        Returns:
            List of created slice IDs
        """
        slice_ids = []

        self.logger.log(
            LogLevel.INFO,
            f"Creating slices from {operation_type} operation",
            "slices_creation_started",
            source_name=source_name,
            operation_type=operation_type,
            slice_count=len(slice_configs)
        )

        for i, slice_config in enumerate(slice_configs):
            try:
                slice_key = slice_config.get("slice_key", f"{operation_type}_slice_{i}")
                slice_type = SliceType(slice_config.get("slice_type", "dataset"))
                slice_data = slice_config.get("slice_data", {})

                # Add operation context to slice data
                slice_data["operation_type"] = operation_type
                slice_data["operation_data"] = operation_data
                slice_data["created_from_operation"] = True

                slice_id = await self.client.register_slice(
                    source_name=source_name,
                    slice_key=slice_key,
                    slice_type=slice_type,
                    slice_data=slice_data,
                    priority=slice_config.get("priority", 100),
                    max_retries=slice_config.get("max_retries", 3),
                    records_expected=slice_config.get("records_expected"),
                    metadata=slice_config.get("metadata", {})
                )

                slice_ids.append(slice_id)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failed to create slice {i}: {e}",
                    "slice_creation_failed",
                    slice_index=i,
                    error=str(e)
                )

        self.logger.log(
            LogLevel.INFO,
            f"Created {len(slice_ids)} slices from {operation_type} operation",
            "slices_creation_completed",
            source_name=source_name,
            operation_type=operation_type,
            created_slices=len(slice_ids)
        )

        return slice_ids

    async def retry_failed_slices(
        self,
        source_name: Optional[str] = None,
        max_slices: int = 10
    ) -> List[str]:
        """Retry failed slices.

        Args:
            source_name: Optional source name filter
            max_slices: Maximum number of slices to retry

        Returns:
            List of slice IDs that were retried
        """
        retry_slices = await self.client.get_slices_for_retry(max_slices)

        if not retry_slices:
            self.logger.log(
                LogLevel.INFO,
                "No slices ready for retry",
                "no_slices_for_retry"
            )
            return []

        retried_ids = []

        for slice_info in retry_slices:
            try:
                # Reset slice status to pending for retry
                # This would require a database function to reset failed slices
                self.logger.log(
                    LogLevel.INFO,
                    f"Retrying failed slice: {slice_info.id}",
                    "slice_retry_started",
                    slice_id=slice_info.id,
                    retry_count=slice_info.retry_count + 1,
                    max_retries=slice_info.max_retries
                )

                # For now, just log the retry attempt
                # In a real implementation, this would reset the slice status
                retried_ids.append(slice_info.id)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Failed to retry slice {slice_info.id}: {e}",
                    "slice_retry_failed",
                    slice_id=slice_info.id,
                    error=str(e)
                )

        self.logger.log(
            LogLevel.INFO,
            f"Retried {len(retried_ids)} failed slices",
            "slices_retry_completed",
            retried_count=len(retried_ids)
        )

        return retried_ids

    def register_processing_callback(self, callback: Callable) -> None:
        """Register a callback for processing events.

        Args:
            callback: Callback function
        """
        self.processing_callbacks.append(callback)

    def unregister_processing_callback(self, callback: Callable) -> None:
        """Unregister a processing callback.

        Args:
            callback: Callback function
        """
        if callback in self.processing_callbacks:
            self.processing_callbacks.remove(callback)

    async def _trigger_callbacks(self, event_type: str, data: Dict[str, Any]) -> None:
        """Trigger registered callbacks.

        Args:
            event_type: Type of event
            data: Event data
        """
        for callback in self.processing_callbacks:
            try:
                await callback(event_type, data)
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Processing callback error: {e}",
                    "processing_callback_error",
                    event_type=event_type,
                    error=str(e)
                )

    async def get_worker_status(self) -> Dict[str, Any]:
        """Get status of all workers.

        Returns:
            Worker status information
        """
        now = datetime.now()

        workers_status = {}
        for worker_id, worker_info in self.workers.items():
            workers_status[worker_id] = {
                "source_name": worker_info["source_name"],
                "slice_types": [t.value for t in worker_info["slice_types"]],
                "max_slices": worker_info["max_slices"],
                "processed_count": worker_info["processed_count"],
                "start_time": worker_info["start_time"].isoformat(),
                "last_heartbeat": worker_info["last_heartbeat"].isoformat(),
                "is_alive": (now - worker_info["last_heartbeat"]).total_seconds() < 60,  # Alive if heartbeat < 1 min ago
                "task_status": "running" if worker_id in self.worker_tasks else "stopped"
            }

        return {
            "total_workers": len(self.workers),
            "active_workers": len([w for w in workers_status.values() if w["is_alive"]]),
            "workers": workers_status
        }

    async def stop_all_workers(self) -> None:
        """Stop all workers."""
        worker_ids = list(self.workers.keys())

        for worker_id in worker_ids:
            await self.stop_worker(worker_id)

        self.logger.log(
            LogLevel.INFO,
            f"Stopped all {len(worker_ids)} slice workers",
            "all_workers_stopped"
        )
