"""Core structured logging interface for data ingestion operations."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # Avoid runtime circular imports
    from .kafka_sink import KafkaLogSink
    from .clickhouse_sink import ClickHouseLogSink


class LogLevel(str, Enum):
    """Log levels for structured logging."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARN = "WARN"
    ERROR = "ERROR"
    FATAL = "FATAL"


@dataclass
class LogEvent:
    """Structured log event."""

    # Core fields
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    level: LogLevel = LogLevel.INFO
    message: str = ""
    event_type: str = "generic"

    # Context fields
    source_name: Optional[str] = None
    dataset: Optional[str] = None
    job_id: Optional[str] = None
    task_id: Optional[str] = None

    # Operational fields
    duration_ms: Optional[float] = None
    records_processed: Optional[int] = None
    bytes_processed: Optional[int] = None
    error_code: Optional[str] = None
    error_message: Optional[str] = None

    # Technical fields
    hostname: Optional[str] = None
    container_id: Optional[str] = None
    thread_id: Optional[str] = None

    # Custom fields
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert log event to dictionary."""
        result = {
            "timestamp": self.timestamp,
            "level": self.level.value,
            "message": self.message,
            "event_type": self.event_type,
        }

        # Add optional fields if present
        for field in ["source_name", "dataset", "job_id", "task_id",
                     "duration_ms", "records_processed", "bytes_processed",
                     "error_code", "error_message", "hostname", "container_id",
                     "thread_id"]:
            value = getattr(self, field)
            if value is not None:
                result[field] = value

        # Add metadata
        if self.metadata:
            result["metadata"] = self.metadata

        return result

    def to_json(self) -> str:
        """Convert log event to JSON string."""
        return json.dumps(self.to_dict(), default=str)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LogEvent":
        """Create log event from dictionary."""
        return cls(
            timestamp=data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            level=LogLevel(data.get("level", "INFO")),
            message=data.get("message", ""),
            event_type=data.get("event_type", "generic"),
            source_name=data.get("source_name"),
            dataset=data.get("dataset"),
            job_id=data.get("job_id"),
            task_id=data.get("task_id"),
            duration_ms=data.get("duration_ms"),
            records_processed=data.get("records_processed"),
            bytes_processed=data.get("bytes_processed"),
            error_code=data.get("error_code"),
            error_message=data.get("error_message"),
            hostname=data.get("hostname"),
            container_id=data.get("container_id"),
            thread_id=data.get("thread_id"),
            metadata=data.get("metadata", {})
        )


class StructuredLogger:
    """Structured logger for data ingestion operations."""

    def __init__(
        self,
        source_name: str,
        kafka_sink: Optional["KafkaLogSink"] = None,
        clickhouse_sink: Optional["ClickHouseLogSink"] = None,
        dataset: Optional[str] = None
    ):
        """Initialize structured logger.

        Args:
            source_name: Name of the data source
            kafka_sink: Optional Kafka sink for real-time logging
            clickhouse_sink: Optional ClickHouse sink for analytics
            dataset: Optional dataset name
        """
        self.source_name = source_name
        self.dataset = dataset
        self.kafka_sink = kafka_sink
        self.clickhouse_sink = clickhouse_sink

        # Generate session ID for grouping related logs
        self.session_id = str(uuid.uuid4())

        # Track operation start times
        self._operation_starts: Dict[str, float] = {}

    def log(
        self,
        level: LogLevel,
        message: str,
        event_type: str = "generic",
        **kwargs
    ) -> None:
        """Log structured event.

        Args:
            level: Log level
            message: Log message
            event_type: Event type
            **kwargs: Additional fields for the log event
        """
        event = LogEvent(
            level=level,
            message=message,
            event_type=event_type,
            source_name=self.source_name,
            dataset=self.dataset,
            hostname=kwargs.get("hostname"),
            container_id=kwargs.get("container_id"),
            thread_id=kwargs.get("thread_id"),
            duration_ms=kwargs.get("duration_ms"),
            records_processed=kwargs.get("records_processed"),
            bytes_processed=kwargs.get("bytes_processed"),
            error_code=kwargs.get("error_code"),
            error_message=kwargs.get("error_message"),
            metadata=dict(kwargs.get("metadata", {}))
        )

        # Add job/task IDs if available
        event.job_id = kwargs.get("job_id")
        event.task_id = kwargs.get("task_id")

        event.metadata.setdefault("session_id", self.session_id)

        used_keys = {"hostname","container_id","thread_id","duration_ms","records_processed","bytes_processed","error_code","error_message","metadata","job_id","task_id",}
        for k, v in kwargs.items():
            if k in used_keys or v is None:
                continue
            try:
                event.metadata[k] = v if isinstance(v, (str, int, float, bool)) else json.dumps(v, default=str)
            except Exception:
                event.metadata[k] = str(v)

        self._emit_event(event)

    def start_operation(self, operation_name: str) -> str:
        """Start tracking an operation.

        Args:
            operation_name: Name of the operation

        Returns:
            Operation ID for use with end_operation
        """
        operation_id = str(uuid.uuid4())
        self._operation_starts[operation_id] = time.time()

        self.log(
            LogLevel.INFO,
            f"Starting operation: {operation_name}",
            event_type="operation_start",
            operation_id=operation_id,
            operation_name=operation_name
        )

        return operation_id

    def end_operation(self, operation_id: str, success: bool = True, **kwargs) -> None:
        """End tracking an operation.

        Args:
            operation_id: Operation ID from start_operation
            success: Whether the operation succeeded
            **kwargs: Additional fields for the log event
        """
        if operation_id not in self._operation_starts:
            self.log(
                LogLevel.WARN,
                f"Ending unknown operation: {operation_id}",
                event_type="operation_end",
                operation_id=operation_id
            )
            return

        start_time = self._operation_starts[operation_id]
        duration_ms = (time.time() - start_time) * 1000

        level = LogLevel.INFO if success else LogLevel.ERROR
        event_type = "operation_success" if success else "operation_failure"

        self.log(
            level,
            f"Operation completed in {duration_ms:.2f}ms",
            event_type=event_type,
            operation_id=operation_id,
            duration_ms=duration_ms,
            **kwargs
        )

        del self._operation_starts[operation_id]

    def log_data_quality(
        self,
        records_processed: int,
        records_valid: int,
        records_invalid: int,
        **kwargs
    ) -> None:
        """Log data quality metrics.

        Args:
            records_processed: Total records processed
            records_valid: Number of valid records
            records_invalid: Number of invalid records
            **kwargs: Additional fields
        """
        quality_score = records_valid / records_processed if records_processed > 0 else 0

        percentage = quality_score * 100
        self.log(
            LogLevel.INFO if quality_score >= 0.95 else LogLevel.WARN,
            f"Data quality: {percentage:.1f}% ({records_valid}/{records_processed} valid)",
            event_type="data_quality",
            records_processed=records_processed,
            records_valid=records_valid,
            records_invalid=records_invalid,
            quality_score=quality_score,
            **kwargs
        )

    def log_api_call(
        self,
        endpoint: str,
        method: str = "GET",
        status_code: Optional[int] = None,
        response_time_ms: Optional[float] = None,
        **kwargs
    ) -> None:
        """Log API call details.

        Args:
            endpoint: API endpoint called
            method: HTTP method used
            status_code: HTTP status code
            response_time_ms: Response time in milliseconds
            **kwargs: Additional fields
        """
        level = LogLevel.ERROR if status_code and status_code >= 400 else LogLevel.DEBUG

        self.log(
            level,
            f"API call: {method} {endpoint}",
            event_type="api_call",
            endpoint=endpoint,
            method=method,
            status_code=status_code,
            response_time_ms=response_time_ms,
            **kwargs
        )

    def log_batch_progress(
        self,
        batch_id: int,
        total_batches: int,
        records_in_batch: int,
        **kwargs
    ) -> None:
        """Log batch processing progress.

        Args:
            batch_id: Current batch ID (1-indexed)
            total_batches: Total number of batches
            records_in_batch: Number of records in this batch
            **kwargs: Additional fields
        """
        progress_pct = (batch_id / total_batches) * 100 if total_batches > 0 else 0

        self.log(
            LogLevel.INFO,
            f"Batch {batch_id}/{total_batches} ({progress_pct:.1f}%) - {records_in_batch} records",
            event_type="batch_progress",
            batch_id=batch_id,
            total_batches=total_batches,
            records_in_batch=records_in_batch,
            progress_pct=progress_pct,
            **kwargs
        )

    def log_error(
        self,
        error: Exception,
        context: Optional[str] = None,
        **kwargs
    ) -> None:
        """Log an error with context.

        Args:
            error: Exception that occurred
            context: Optional context about where the error occurred
            **kwargs: Additional fields
        """
        error_message = f"{error.__class__.__name__}: {str(error)}"
        if context:
            error_message = f"{context}: {error_message}"

        self.log(
            LogLevel.ERROR,
            error_message,
            event_type="error",
            error_code=error.__class__.__name__,
            error_message=str(error),
            **kwargs
        )

    def _emit_event(self, event: LogEvent) -> None:
        """Emit log event to configured sinks.

        Args:
            event: Log event to emit
        """
        # Add session ID to all events
        event.metadata["session_id"] = self.session_id

        # Emit to Kafka (real-time)
        if self.kafka_sink:
            try:
                self.kafka_sink.emit(event)
            except Exception as e:
                print(f"Failed to emit to Kafka: {e}")

        # Emit to ClickHouse (analytics)
        if self.clickhouse_sink:
            try:
                self.clickhouse_sink.emit(event)
            except Exception as e:
                print(f"Failed to emit to ClickHouse: {e}")

        # Also emit to console for development
        try:
            print(event.to_json())
        except Exception:
            print(json.dumps({"timestamp": event.timestamp, "level": event.level.value, "event_type": event.event_type, "message": event.message}))

    def close(self) -> None:
        """Close the logger and clean up resources."""
        if self.kafka_sink:
            self.kafka_sink.close()
        if self.clickhouse_sink:
            self.clickhouse_sink.close()


# Convenience function for creating loggers
def create_logger(
    source_name: str,
    kafka_bootstrap_servers: Optional[str] = None,
    kafka_topic: str = "aurum.ops.logs",
    clickhouse_url: Optional[str] = None,
    clickhouse_table: str = "aurum_logs",
    dataset: Optional[str] = None
) -> StructuredLogger:
    """Create a structured logger with configured sinks.

    Args:
        source_name: Name of the data source
        kafka_bootstrap_servers: Kafka bootstrap servers
        kafka_topic: Kafka topic for logs
        clickhouse_url: ClickHouse JDBC URL
        clickhouse_table: ClickHouse table name
        dataset: Optional dataset name

    Returns:
        Configured StructuredLogger instance
    """
    from .kafka_sink import KafkaLogSink
    from .clickhouse_sink import ClickHouseLogSink

    sinks = []

    if kafka_bootstrap_servers:
        kafka_sink = KafkaLogSink(kafka_bootstrap_servers, kafka_topic)
        sinks.append(kafka_sink)

    if clickhouse_url:
        clickhouse_sink = ClickHouseLogSink(clickhouse_url, clickhouse_table)
        sinks.append(clickhouse_sink)

    return StructuredLogger(
        source_name=source_name,
        kafka_sink=kafka_sink if kafka_bootstrap_servers else None,
        clickhouse_sink=clickhouse_sink if clickhouse_url else None,
        dataset=dataset
    )


# Compatibility shim: many modules import get_logger from this module.
def get_logger(name: str):  # pragma: no cover - trivial adapter
    from aurum.observability.logging import get_logger as _get
    return _get(name)
