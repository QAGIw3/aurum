"""Monitoring and metrics for bulk archive operations."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from pathlib import Path
import psutil

from ..logging import create_logger, LogLevel


@dataclass
class ArchiveMetrics:
    """Metrics for bulk archive operations."""

    operation_id: str
    source_name: str
    operation_type: str  # download, extract, verify, process
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    # File metrics
    files_processed: int = 0
    bytes_processed: int = 0
    files_successful: int = 0
    files_failed: int = 0

    # Performance metrics
    throughput_files_per_second: float = 0.0
    throughput_bytes_per_second: float = 0.0
    average_file_size_bytes: int = 0

    # Error metrics
    errors: List[str] = field(default_factory=list)
    retry_count: int = 0

    # Resource usage
    peak_memory_mb: float = 0.0
    peak_cpu_percent: float = 0.0

    # Custom metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ProcessingMetrics:
    """Metrics for bulk data processing operations."""

    operation_id: str
    source_name: str
    processing_type: str
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_seconds: float = 0.0

    # Data metrics
    records_processed: int = 0
    records_valid: int = 0
    records_invalid: int = 0
    data_quality_score: float = 0.0

    # File metrics
    input_files: int = 0
    output_files: int = 0
    total_input_size_mb: float = 0.0
    total_output_size_mb: float = 0.0

    # Performance metrics
    throughput_records_per_second: float = 0.0
    processing_rate_mb_per_second: float = 0.0

    # Resource usage
    peak_memory_mb: float = 0.0
    peak_cpu_percent: float = 0.0
    average_cpu_percent: float = 0.0

    # Custom metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


class BulkArchiveMonitor:
    """Monitor for bulk archive operations with metrics collection."""

    def __init__(self,
                 metrics_collection_interval: float = 5.0,
                 enable_resource_monitoring: bool = True):
        """Initialize bulk archive monitor.

        Args:
            metrics_collection_interval: Interval for metrics collection in seconds
            enable_resource_monitoring: Whether to collect system resource metrics
        """
        self.metrics_collection_interval = metrics_collection_interval
        self.enable_resource_monitoring = enable_resource_monitoring

        self.logger = create_logger(
            source_name="bulk_archive_monitor",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.bulk_archive.monitoring",
            dataset="bulk_archive_monitoring"
        )

        # Metrics storage
        self.archive_metrics: Dict[str, ArchiveMetrics] = {}
        self.processing_metrics: Dict[str, ProcessingMetrics] = {}

        # Active operations tracking
        self.active_operations: Dict[str, Dict[str, Any]] = {}

        # Resource monitoring
        self.resource_monitoring_task: Optional[asyncio.Task] = None
        self._stop_monitoring = asyncio.Event()

        # Callbacks
        self.metrics_callbacks: List[Callable] = []

    async def start_monitoring(self) -> None:
        """Start the monitoring system."""
        self.logger.log(
            LogLevel.INFO,
            "Starting bulk archive monitoring",
            "bulk_archive_monitoring_started",
            collection_interval_seconds=self.metrics_collection_interval,
            enable_resource_monitoring=self.enable_resource_monitoring
        )

        if self.enable_resource_monitoring:
            self.resource_monitoring_task = asyncio.create_task(
                self._resource_monitoring_loop()
            )

    async def stop_monitoring(self) -> None:
        """Stop the monitoring system."""
        self.logger.log(
            LogLevel.INFO,
            "Stopping bulk archive monitoring",
            "bulk_archive_monitoring_stopping"
        )

        self._stop_monitoring.set()

        if self.resource_monitoring_task:
            self.resource_monitoring_task.cancel()
            try:
                await self.resource_monitoring_task
            except asyncio.CancelledError:
                pass

    async def start_operation(
        self,
        operation_id: str,
        operation_type: str,
        source_name: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Start monitoring an operation.

        Args:
            operation_id: Unique operation identifier
            operation_type: Type of operation (download, extract, verify, process)
            source_name: Name of the data source
            metadata: Optional metadata for the operation
        """
        now = datetime.now()

        if operation_type in ["download", "extract", "verify"]:
            metrics = ArchiveMetrics(
                operation_id=operation_id,
                source_name=source_name,
                operation_type=operation_type,
                start_time=now,
                metadata=metadata or {}
            )
            self.archive_metrics[operation_id] = metrics

        elif operation_type in ["process", "transform"]:
            metrics = ProcessingMetrics(
                operation_id=operation_id,
                source_name=source_name,
                processing_type=operation_type,
                start_time=now,
                metadata=metadata or {}
            )
            self.processing_metrics[operation_id] = metrics

        self.active_operations[operation_id] = {
            "type": operation_type,
            "start_time": now,
            "last_update": now
        }

        self.logger.log(
            LogLevel.INFO,
            f"Started monitoring operation {operation_id}",
            "operation_monitoring_started",
            operation_id=operation_id,
            operation_type=operation_type,
            source_name=source_name
        )

    async def update_operation(
        self,
        operation_id: str,
        files_processed: int = 0,
        bytes_processed: int = 0,
        records_processed: int = 0,
        errors: Optional[List[str]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Update operation progress.

        Args:
            operation_id: Operation identifier
            files_processed: Number of files processed
            bytes_processed: Number of bytes processed
            records_processed: Number of records processed
            errors: List of errors encountered
            metadata: Additional metadata
        """
        now = datetime.now()
        operation = self.active_operations.get(operation_id)

        if not operation:
            self.logger.log(
                LogLevel.WARNING,
                f"Attempted to update unknown operation: {operation_id}",
                "operation_update_failed",
                operation_id=operation_id
            )
            return

        operation["last_update"] = now

        # Update metrics based on operation type
        if operation_id in self.archive_metrics:
            metrics = self.archive_metrics[operation_id]
            if files_processed > 0:
                metrics.files_processed = files_processed
            if bytes_processed > 0:
                metrics.bytes_processed = bytes_processed
            if errors:
                metrics.errors.extend(errors)

            # Update resource usage
            if self.enable_resource_monitoring:
                await self._update_resource_metrics(metrics)

        elif operation_id in self.processing_metrics:
            metrics = self.processing_metrics[operation_id]
            if records_processed > 0:
                metrics.records_processed = records_processed
            if metadata:
                metrics.metadata.update(metadata)
            if errors:
                metrics.metadata["errors"] = errors

            # Update resource usage
            if self.enable_resource_monitoring:
                await self._update_resource_metrics(metrics)

        # Trigger callbacks
        await self._trigger_callbacks(operation_id, "update", {
            "files_processed": files_processed,
            "bytes_processed": bytes_processed,
            "records_processed": records_processed,
            "timestamp": now
        })

    async def complete_operation(
        self,
        operation_id: str,
        success: bool = True,
        final_metrics: Optional[Dict[str, Any]] = None
    ) -> None:
        """Complete monitoring of an operation.

        Args:
            operation_id: Operation identifier
            success: Whether the operation completed successfully
            final_metrics: Final metrics for the operation
        """
        now = datetime.now()
        operation = self.active_operations.get(operation_id)

        if not operation:
            self.logger.log(
                LogLevel.WARNING,
                f"Attempted to complete unknown operation: {operation_id}",
                "operation_completion_failed",
                operation_id=operation_id
            )
            return

        start_time = operation["start_time"]
        duration = (now - start_time).total_seconds()

        # Update metrics
        if operation_id in self.archive_metrics:
            metrics = self.archive_metrics[operation_id]
            metrics.end_time = now
            metrics.duration_seconds = duration

            # Calculate performance metrics
            if duration > 0:
                metrics.throughput_files_per_second = metrics.files_processed / duration
                metrics.throughput_bytes_per_second = metrics.bytes_processed / duration
                if metrics.files_processed > 0:
                    metrics.average_file_size_bytes = metrics.bytes_processed / metrics.files_processed

            # Mark success/failure
            if success:
                metrics.files_successful = metrics.files_processed
            else:
                metrics.files_failed = len(metrics.errors)

        elif operation_id in self.processing_metrics:
            metrics = self.processing_metrics[operation_id]
            metrics.end_time = now
            metrics.duration_seconds = duration

            # Calculate performance metrics
            if duration > 0:
                metrics.throughput_records_per_second = metrics.records_processed / duration
                metrics.processing_rate_mb_per_second = metrics.total_output_size_mb / (duration / 60)  # MB per minute

            # Calculate quality score
            if metrics.records_processed > 0:
                metrics.data_quality_score = metrics.records_valid / metrics.records_processed

        # Log completion
        self.logger.log(
            LogLevel.INFO,
            f"Operation {operation_id} completed in {duration:.2f}s",
            "operation_monitoring_completed",
            operation_id=operation_id,
            success=success,
            duration_seconds=duration,
            operation_type=operation["type"]
        )

        # Trigger callbacks
        await self._trigger_callbacks(operation_id, "complete", {
            "success": success,
            "duration_seconds": duration,
            "final_metrics": final_metrics,
            "timestamp": now
        })

        # Remove from active operations
        del self.active_operations[operation_id]

    async def _resource_monitoring_loop(self) -> None:
        """Background task for monitoring system resources."""
        while not self._stop_monitoring.is_set():
            try:
                # Get current resource usage
                memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
                cpu_percent = psutil.cpu_percent(interval=1)

                # Update active operations with resource metrics
                for operation_id in self.active_operations:
                    if operation_id in self.archive_metrics:
                        metrics = self.archive_metrics[operation_id]
                        metrics.peak_memory_mb = max(metrics.peak_memory_mb, memory_mb)
                        metrics.peak_cpu_percent = max(metrics.peak_cpu_percent, cpu_percent)

                    elif operation_id in self.processing_metrics:
                        metrics = self.processing_metrics[operation_id]
                        metrics.peak_memory_mb = max(metrics.peak_memory_mb, memory_mb)
                        metrics.peak_cpu_percent = max(metrics.peak_cpu_percent, cpu_percent)
                        metrics.average_cpu_percent = (
                            (metrics.average_cpu_percent + cpu_percent) / 2
                            if metrics.average_cpu_percent > 0 else cpu_percent
                        )

                # Sleep for collection interval
                await asyncio.sleep(self.metrics_collection_interval)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Resource monitoring error: {e}",
                    "resource_monitoring_error",
                    error=str(e)
                )
                await asyncio.sleep(5)  # Wait before retrying

    async def _update_resource_metrics(self, metrics: Union[ArchiveMetrics, ProcessingMetrics]) -> None:
        """Update resource metrics for an operation.

        Args:
            metrics: Metrics object to update
        """
        try:
            memory_mb = psutil.Process().memory_info().rss / 1024 / 1024
            cpu_percent = psutil.cpu_percent(interval=None)

            if isinstance(metrics, ArchiveMetrics):
                metrics.peak_memory_mb = max(metrics.peak_memory_mb, memory_mb)
                metrics.peak_cpu_percent = max(metrics.peak_cpu_percent, cpu_percent)
            else:
                metrics.peak_memory_mb = max(metrics.peak_memory_mb, memory_mb)
                metrics.peak_cpu_percent = max(metrics.peak_cpu_percent, cpu_percent)
                metrics.average_cpu_percent = (
                    (metrics.average_cpu_percent + cpu_percent) / 2
                    if metrics.average_cpu_percent > 0 else cpu_percent
                )

        except Exception as e:
            self.logger.log(
                LogLevel.DEBUG,
                f"Failed to update resource metrics: {e}",
                "resource_metrics_update_failed",
                error=str(e)
            )

    async def _trigger_callbacks(self, operation_id: str, event_type: str, data: Dict[str, Any]) -> None:
        """Trigger registered callbacks.

        Args:
            operation_id: Operation identifier
            event_type: Type of event (start, update, complete)
            data: Event data
        """
        for callback in self.metrics_callbacks:
            try:
                await callback(operation_id, event_type, data)
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Callback error: {e}",
                    "callback_execution_failed",
                    operation_id=operation_id,
                    event_type=event_type,
                    error=str(e)
                )

    def register_callback(self, callback: Callable) -> None:
        """Register a callback for monitoring events.

        Args:
            callback: Callback function to register
        """
        self.metrics_callbacks.append(callback)

    def unregister_callback(self, callback: Callable) -> None:
        """Unregister a callback.

        Args:
            callback: Callback function to unregister
        """
        if callback in self.metrics_callbacks:
            self.metrics_callbacks.remove(callback)

    def get_operation_metrics(self, operation_id: str) -> Optional[Union[ArchiveMetrics, ProcessingMetrics]]:
        """Get metrics for a specific operation.

        Args:
            operation_id: Operation identifier

        Returns:
            Operation metrics or None if not found
        """
        if operation_id in self.archive_metrics:
            return self.archive_metrics[operation_id]
        elif operation_id in self.processing_metrics:
            return self.processing_metrics[operation_id]
        else:
            return None

    def get_active_operations_summary(self) -> Dict[str, Any]:
        """Get summary of active operations.

        Returns:
            Summary of active operations
        """
        now = datetime.now()

        summary = {
            "total_active_operations": len(self.active_operations),
            "archive_operations": len([op for op in self.active_operations.values() if op["type"] in ["download", "extract", "verify"]]),
            "processing_operations": len([op for op in self.active_operations.values() if op["type"] in ["process", "transform"]]),
            "longest_running_operation": None,
            "operations": []
        }

        longest_duration = 0
        longest_operation = None

        for operation_id, operation in self.active_operations.items():
            start_time = operation["start_time"]
            duration = (now - start_time).total_seconds()

            if duration > longest_duration:
                longest_duration = duration
                longest_operation = operation_id

            summary["operations"].append({
                "operation_id": operation_id,
                "type": operation["type"],
                "duration_seconds": duration,
                "last_update_seconds_ago": (now - operation["last_update"]).total_seconds()
            })

        summary["longest_running_operation"] = longest_operation

        return summary

    def get_metrics_summary(self, time_range_hours: int = 24) -> Dict[str, Any]:
        """Get summary of all metrics.

        Args:
            time_range_hours: Time range to include in hours

        Returns:
            Metrics summary
        """
        cutoff_time = datetime.now() - timedelta(hours=time_range_hours)

        # Archive metrics summary
        archive_metrics = [m for m in self.archive_metrics.values() if m.start_time >= cutoff_time]
        processing_metrics = [m for m in self.processing_metrics.values() if m.start_time >= cutoff_time]

        summary = {
            "time_range_hours": time_range_hours,
            "cutoff_time": cutoff_time.isoformat(),
            "archive_operations": {
                "total": len(archive_metrics),
                "completed": len([m for m in archive_metrics if m.end_time]),
                "failed": len([m for m in archive_metrics if m.files_failed > 0]),
                "average_duration_seconds": sum(m.duration_seconds for m in archive_metrics) / max(len(archive_metrics), 1),
                "average_throughput_files_per_second": sum(m.throughput_files_per_second for m in archive_metrics) / max(len(archive_metrics), 1),
                "average_throughput_bytes_per_second": sum(m.throughput_bytes_per_second for m in archive_metrics) / max(len(archive_metrics), 1),
                "total_bytes_processed": sum(m.bytes_processed for m in archive_metrics),
                "total_files_processed": sum(m.files_processed for m in archive_metrics)
            },
            "processing_operations": {
                "total": len(processing_metrics),
                "completed": len([m for m in processing_metrics if m.end_time]),
                "failed": len([m for m in processing_metrics if m.records_processed == 0 and m.end_time]),
                "average_duration_seconds": sum(m.duration_seconds for m in processing_metrics) / max(len(processing_metrics), 1),
                "average_throughput_records_per_second": sum(m.throughput_records_per_second for m in processing_metrics) / max(len(processing_metrics), 1),
                "average_data_quality_score": sum(m.data_quality_score for m in processing_metrics) / max(len(processing_metrics), 1),
                "total_records_processed": sum(m.records_processed for m in processing_metrics),
                "total_output_size_mb": sum(m.total_output_size_mb for m in processing_metrics)
            }
        }

        return summary

    async def generate_report(self, output_file: Path) -> None:
        """Generate a comprehensive monitoring report.

        Args:
            output_file: Path to save the report
        """
        self.logger.log(
            LogLevel.INFO,
            f"Generating monitoring report: {output_file}",
            "monitoring_report_generation_started",
            output_file=str(output_file)
        )

        # Get current metrics
        active_summary = self.get_active_operations_summary()
        metrics_summary = self.get_metrics_summary()

        # Generate report
        report = {
            "generated_at": datetime.now().isoformat(),
            "report_type": "bulk_archive_monitoring",
            "active_operations": active_summary,
            "metrics_summary": metrics_summary,
            "archive_metrics": {
                operation_id: {
                    "operation_id": metrics.operation_id,
                    "source_name": metrics.source_name,
                    "operation_type": metrics.operation_type,
                    "duration_seconds": metrics.duration_seconds,
                    "files_processed": metrics.files_processed,
                    "bytes_processed": metrics.bytes_processed,
                    "throughput_files_per_second": metrics.throughput_files_per_second,
                    "throughput_bytes_per_second": metrics.throughput_bytes_per_second,
                    "errors": len(metrics.errors),
                    "peak_memory_mb": metrics.peak_memory_mb,
                    "peak_cpu_percent": metrics.peak_cpu_percent
                }
                for operation_id, metrics in self.archive_metrics.items()
            },
            "processing_metrics": {
                operation_id: {
                    "operation_id": metrics.operation_id,
                    "source_name": metrics.source_name,
                    "processing_type": metrics.processing_type,
                    "duration_seconds": metrics.duration_seconds,
                    "records_processed": metrics.records_processed,
                    "data_quality_score": metrics.data_quality_score,
                    "throughput_records_per_second": metrics.throughput_records_per_second,
                    "peak_memory_mb": metrics.peak_memory_mb,
                    "peak_cpu_percent": metrics.peak_cpu_percent,
                    "average_cpu_percent": metrics.average_cpu_percent
                }
                for operation_id, metrics in self.processing_metrics.items()
            }
        }

        # Save report
        with open(output_file, 'w') as f:
            import json
            json.dump(report, f, indent=2, default=str)

        self.logger.log(
            LogLevel.INFO,
            f"Monitoring report generated: {output_file}",
            "monitoring_report_generation_completed",
            output_file=str(output_file),
            active_operations=active_summary["total_active_operations"]
        )
