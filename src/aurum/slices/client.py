"""Client for interacting with the slice ledger system."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from enum import Enum
from pathlib import Path

from ..db.base import DatabaseClient, DatabaseError
from ..logging import create_logger, LogLevel


class SliceStatus(str, Enum):
    """Status of a slice."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SliceType(str, Enum):
    """Type of slice."""

    TIME_WINDOW = "time_window"
    DATASET = "dataset"
    FAILED_RECORDS = "failed_records"
    INCREMENTAL = "incremental"
    QUALITY_ISSUES = "quality_issues"


@dataclass
class SliceInfo:
    """Information about a slice."""

    id: str
    source_name: str
    slice_key: str
    slice_type: SliceType
    slice_data: Dict[str, Any]
    status: SliceStatus
    priority: int = 100
    max_retries: int = 3
    retry_count: int = 0
    progress_percent: float = 0.0
    records_processed: int = 0
    records_expected: Optional[int] = None
    error_message: Optional[str] = None
    worker_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    failed_at: Optional[datetime] = None
    next_retry_at: Optional[datetime] = None

    def is_retryable(self) -> bool:
        """Check if this slice can be retried."""
        return (
            self.status == SliceStatus.FAILED and
            self.retry_count < self.max_retries and
            (self.next_retry_at is None or self.next_retry_at <= datetime.now())
        )

    def is_ready_for_processing(self) -> bool:
        """Check if this slice is ready to be processed."""
        return self.status in [SliceStatus.PENDING, SliceStatus.FAILED] and self.is_retryable()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "source_name": self.source_name,
            "slice_key": self.slice_key,
            "slice_type": self.slice_type.value,
            "slice_data": self.slice_data,
            "status": self.status.value,
            "priority": self.priority,
            "max_retries": self.max_retries,
            "retry_count": self.retry_count,
            "progress_percent": self.progress_percent,
            "records_processed": self.records_processed,
            "records_expected": self.records_expected,
            "error_message": self.error_message,
            "worker_id": self.worker_id,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "failed_at": self.failed_at.isoformat() if self.failed_at else None,
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None
        }


@dataclass
class SliceQuery:
    """Query parameters for finding slices."""

    source_name: Optional[str] = None
    slice_type: Optional[SliceType] = None
    status: Optional[SliceStatus] = None
    priority_max: Optional[int] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None
    limit: int = 100
    offset: int = 0
    include_metadata: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source_name": self.source_name,
            "slice_type": self.slice_type.value if self.slice_type else None,
            "status": self.status.value if self.status else None,
            "priority_max": self.priority_max,
            "created_after": self.created_after.isoformat() if self.created_after else None,
            "created_before": self.created_before.isoformat() if self.created_before else None,
            "limit": self.limit,
            "offset": self.offset,
            "include_metadata": self.include_metadata
        }


@dataclass
class SliceConfig:
    """Configuration for slice operations."""

    database_url: str
    max_concurrent_slices: int = 5
    default_priority: int = 100
    default_max_retries: int = 3
    retry_base_delay_minutes: int = 1
    retry_max_delay_minutes: int = 60
    enable_progress_updates: bool = True
    progress_update_interval_seconds: int = 30
    enable_detailed_logging: bool = False


class SliceClient:
    """Client for interacting with the slice ledger system."""

    def __init__(self, config: SliceConfig):
        """Initialize slice client.

        Args:
            config: Slice configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="slice_client",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.slices.operations",
            dataset="slice_operations"
        )

        self.db = DatabaseClient(config.database_url)

    async def __aenter__(self):
        """Async context manager entry."""
        await self.db.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.db.disconnect()

    async def register_slice(
        self,
        source_name: str,
        slice_key: str,
        slice_type: SliceType,
        slice_data: Dict[str, Any],
        priority: Optional[int] = None,
        max_retries: Optional[int] = None,
        records_expected: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """Register a new slice or update existing one.

        Args:
            source_name: Name of the data source
            slice_key: Unique key for this slice
            slice_type: Type of slice
            slice_data: Additional data for the slice
            priority: Priority for processing (lower = higher priority)
            max_retries: Maximum number of retry attempts
            records_expected: Expected number of records to process
            metadata: Additional metadata

        Returns:
            Slice ID
        """
        priority = priority or self.config.default_priority
        max_retries = max_retries or self.config.default_max_retries

        try:
            slice_id = await self.db.fetch_single(
                "SELECT upsert_ingest_slice(%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    source_name,
                    slice_key,
                    slice_type.value,
                    json.dumps(slice_data),
                    "pending",
                    priority,
                    max_retries,
                    records_expected,
                    json.dumps(metadata or {})
                )
            )

            self.logger.log(
                LogLevel.INFO,
                f"Registered slice: {slice_key}",
                "slice_registered",
                source_name=source_name,
                slice_key=slice_key,
                slice_type=slice_type.value,
                priority=priority
            )

            return slice_id

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to register slice {slice_key}: {e}",
                "slice_registration_failed",
                source_name=source_name,
                slice_key=slice_key,
                error=str(e)
            )
            raise

    async def get_slice(self, slice_id: str) -> Optional[SliceInfo]:
        """Get slice information by ID.

        Args:
            slice_id: Slice ID

        Returns:
            Slice information or None if not found
        """
        try:
            row = await self.db.fetch_single(
                """
                SELECT id, source_name, slice_key, slice_type, slice_data,
                       status, priority, max_retries, retry_count, progress_percent,
                       records_processed, records_expected, error_message, worker_id,
                       metadata, created_at, updated_at, started_at, completed_at,
                       failed_at, next_retry_at
                FROM ingest_slice
                WHERE id = %s
                """,
                (slice_id,)
            )

            if row:
                return self._row_to_slice_info(row)
            else:
                return None

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to get slice {slice_id}: {e}",
                "slice_get_failed",
                slice_id=slice_id,
                error=str(e)
            )
            raise

    async def get_slices(self, query: SliceQuery) -> List[SliceInfo]:
        """Get slices based on query parameters.

        Args:
            query: Query parameters

        Returns:
            List of slice information
        """
        try:
            conditions = ["1=1"]
            params = []

            if query.source_name:
                conditions.append("source_name = %s")
                params.append(query.source_name)

            if query.slice_type:
                conditions.append("slice_type = %s")
                params.append(query.slice_type.value)

            if query.status:
                conditions.append("status = %s")
                params.append(query.status.value)

            if query.priority_max is not None:
                conditions.append("priority <= %s")
                params.append(query.priority_max)

            if query.created_after:
                conditions.append("created_at >= %s")
                params.append(query.created_after)

            if query.created_before:
                conditions.append("created_at <= %s")
                params.append(query.created_before)

            where_clause = " AND ".join(conditions)

            # Build query
            if query.include_metadata:
                select_fields = """
                    id, source_name, slice_key, slice_type, slice_data,
                    status, priority, max_retries, retry_count, progress_percent,
                    records_processed, records_expected, error_message, worker_id,
                    metadata, created_at, updated_at, started_at, completed_at,
                    failed_at, next_retry_at
                """
            else:
                select_fields = """
                    id, source_name, slice_key, slice_type, slice_data,
                    status, priority, max_retries, retry_count, progress_percent,
                    records_processed, records_expected, error_message, worker_id,
                    '{}'::jsonb as metadata, created_at, updated_at, started_at, completed_at,
                    failed_at, next_retry_at
                """

            sql = f"""
                SELECT {select_fields}
                FROM ingest_slice
                WHERE {where_clause}
                ORDER BY priority ASC, created_at ASC
                LIMIT %s OFFSET %s
            """

            params.extend([query.limit, query.offset])

            rows = await self.db.fetch_all(sql, tuple(params))

            slices = []
            for row in rows:
                slice_info = self._row_to_slice_info(row)
                slices.append(slice_info)

            return slices

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to get slices: {e}",
                "slices_get_failed",
                error=str(e)
            )
            raise

    async def start_slice(
        self,
        slice_id: str,
        worker_id: Optional[str] = None
    ) -> bool:
        """Start processing a slice.

        Args:
            slice_id: Slice ID
            worker_id: Optional worker ID

        Returns:
            True if slice was started successfully
        """
        try:
            success = await self.db.fetch_single(
                "SELECT start_ingest_slice(%s, %s)",
                (slice_id, worker_id)
            )

            if success:
                self.logger.log(
                    LogLevel.INFO,
                    f"Started slice: {slice_id}",
                    "slice_started",
                    slice_id=slice_id,
                    worker_id=worker_id
                )
            else:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Failed to start slice (may be in progress): {slice_id}",
                    "slice_start_failed",
                    slice_id=slice_id
                )

            return success

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to start slice {slice_id}: {e}",
                "slice_start_error",
                slice_id=slice_id,
                error=str(e)
            )
            raise

    async def complete_slice(
        self,
        slice_id: str,
        records_processed: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Complete a slice successfully.

        Args:
            slice_id: Slice ID
            records_processed: Number of records processed
            metadata: Additional metadata

        Returns:
            True if slice was completed successfully
        """
        try:
            success = await self.db.fetch_single(
                "SELECT complete_ingest_slice(%s, %s, %s, %s)",
                (
                    slice_id,
                    records_processed,
                    None,  # processing_time_seconds - calculated by DB
                    json.dumps(metadata or {})
                )
            )

            if success:
                self.logger.log(
                    LogLevel.INFO,
                    f"Completed slice: {slice_id}",
                    "slice_completed",
                    slice_id=slice_id,
                    records_processed=records_processed
                )
            else:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Failed to complete slice (may not be running): {slice_id}",
                    "slice_complete_failed",
                    slice_id=slice_id
                )

            return success

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to complete slice {slice_id}: {e}",
                "slice_complete_error",
                slice_id=slice_id,
                error=str(e)
            )
            raise

    async def fail_slice(
        self,
        slice_id: str,
        error_message: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Mark a slice as failed.

        Args:
            slice_id: Slice ID
            error_message: Error message
            metadata: Additional metadata

        Returns:
            True if slice was marked as failed
        """
        try:
            success = await self.db.fetch_single(
                "SELECT fail_ingest_slice(%s, %s, %s)",
                (slice_id, error_message, json.dumps(metadata or {}))
            )

            if success:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Failed slice: {slice_id}",
                    "slice_failed",
                    slice_id=slice_id,
                    error_message=error_message
                )
            else:
                self.logger.log(
                    LogLevel.WARNING,
                    f"Failed to mark slice as failed: {slice_id}",
                    "slice_fail_failed",
                    slice_id=slice_id
                )

            return success

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to fail slice {slice_id}: {e}",
                "slice_fail_error",
                slice_id=slice_id,
                error=str(e)
            )
            raise

    async def update_slice_progress(
        self,
        slice_id: str,
        progress_percent: float,
        records_processed: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update slice progress.

        Args:
            slice_id: Slice ID
            progress_percent: Progress percentage (0.0-100.0)
            records_processed: Number of records processed
            metadata: Additional metadata

        Returns:
            True if progress was updated
        """
        try:
            success = await self.db.fetch_single(
                "SELECT update_ingest_slice_progress(%s, %s, %s, %s)",
                (
                    slice_id,
                    progress_percent,
                    records_processed,
                    json.dumps(metadata or {})
                )
            )

            if success and self.config.enable_progress_updates:
                self.logger.log(
                    LogLevel.DEBUG,
                    f"Updated slice progress: {slice_id}",
                    "slice_progress_updated",
                    slice_id=slice_id,
                    progress_percent=progress_percent,
                    records_processed=records_processed
                )

            return success

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to update slice progress {slice_id}: {e}",
                "slice_progress_update_error",
                slice_id=slice_id,
                error=str(e)
            )
            raise

    async def get_slices_for_retry(self, max_slices: int = 10) -> List[SliceInfo]:
        """Get slices that are ready for retry.

        Args:
            max_slices: Maximum number of slices to return

        Returns:
            List of slices ready for retry
        """
        try:
            rows = await self.db.fetch_all(
                "SELECT * FROM get_slices_for_retry(%s)",
                (max_slices,)
            )

            slices = []
            for row in rows:
                slice_info = self._row_to_slice_info(row)
                slices.append(slice_info)

            self.logger.log(
                LogLevel.INFO,
                f"Found {len(slices)} slices ready for retry",
                "slices_for_retry_found",
                slice_count=len(slices)
            )

            return slices

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to get slices for retry: {e}",
                "slices_for_retry_error",
                error=str(e)
            )
            raise

    async def get_pending_slices(
        self,
        max_slices: int = 10,
        source_name: Optional[str] = None,
        slice_type: Optional[SliceType] = None
    ) -> List[SliceInfo]:
        """Get pending slices for processing.

        Args:
            max_slices: Maximum number of slices to return
            source_name: Optional source name filter
            slice_type: Optional slice type filter

        Returns:
            List of pending slices
        """
        try:
            rows = await self.db.fetch_all(
                "SELECT * FROM get_pending_slices(%s, %s, %s)",
                (max_slices, source_name, slice_type.value if slice_type else None)
            )

            slices = []
            for row in rows:
                slice_info = self._row_to_slice_info(row)
                slices.append(slice_info)

            self.logger.log(
                LogLevel.INFO,
                f"Found {len(slices)} pending slices",
                "pending_slices_found",
                slice_count=len(slices),
                source_name=source_name,
                slice_type=slice_type.value if slice_type else None
            )

            return slices

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to get pending slices: {e}",
                "pending_slices_error",
                error=str(e)
            )
            raise

    async def get_slice_statistics(
        self,
        source_name: Optional[str] = None,
        hours_back: int = 24
    ) -> Dict[str, Any]:
        """Get slice operation statistics.

        Args:
            source_name: Optional source name filter
            hours_back: Time range in hours

        Returns:
            Statistics dictionary
        """
        try:
            stats_json = await self.db.fetch_single(
                "SELECT get_slice_statistics(%s, %s)",
                (source_name, hours_back)
            )

            return json.loads(stats_json)

        except DatabaseError as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to get slice statistics: {e}",
                "slice_statistics_error",
                error=str(e)
            )
            raise

    def _row_to_slice_info(self, row) -> SliceInfo:
        """Convert database row to SliceInfo.

        Args:
            row: Database row

        Returns:
            SliceInfo object
        """
        return SliceInfo(
            id=str(row['id']),
            source_name=row['source_name'],
            slice_key=row['slice_key'],
            slice_type=SliceType(row['slice_type']),
            slice_data=json.loads(row['slice_data']) if isinstance(row['slice_data'], str) else row['slice_data'],
            status=SliceStatus(row['status']),
            priority=row['priority'],
            max_retries=row['max_retries'],
            retry_count=row['retry_count'],
            progress_percent=float(row['progress_percent'] or 0),
            records_processed=row['records_processed'] or 0,
            records_expected=row['records_expected'],
            error_message=row['error_message'],
            worker_id=row['worker_id'],
            metadata=json.loads(row['metadata']) if isinstance(row['metadata'], str) else row['metadata'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
            started_at=row['started_at'],
            completed_at=row['completed_at'],
            failed_at=row['failed_at'],
            next_retry_at=row['next_retry_at']
        )
