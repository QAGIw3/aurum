"""Watermark tracking for staleness monitoring."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any

from ..db import get_ingest_watermark, update_ingest_watermark
from ..logging import StructuredLogger, LogLevel, create_logger


class WatermarkStatus(str, Enum):
    """Watermark status types."""
    CURRENT = "current"       # Watermark is recent
    STALE = "stale"           # Watermark is stale
    MISSING = "missing"        # No watermark found
    ERROR = "error"           # Error retrieving watermark


@dataclass
class WatermarkInfo:
    """Information about a dataset's watermark."""

    dataset: str
    watermark_key: str = "logical_date"
    last_watermark: Optional[datetime] = None
    last_check_time: Optional[datetime] = None
    status: WatermarkStatus = WatermarkStatus.MISSING

    # Tracking fields
    expected_frequency_hours: int = 24
    grace_period_hours: int = 1
    consecutive_failures: int = 0

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_current(self) -> bool:
        """Check if watermark is current."""
        return self.status == WatermarkStatus.CURRENT

    def is_stale(self) -> bool:
        """Check if watermark is stale."""
        return self.status == WatermarkStatus.STALE

    def is_missing(self) -> bool:
        """Check if watermark is missing."""
        return self.status == WatermarkStatus.MISSING

    def hours_since_update(self, current_time: Optional[datetime] = None) -> float:
        """Get hours since last watermark update.

        Args:
            current_time: Current time (defaults to now)

        Returns:
            Hours since last update (inf if no watermark)
        """
        if current_time is None:
            current_time = datetime.now()

        if self.last_watermark is None:
            return float('inf')

        return (current_time - self.last_watermark).total_seconds() / 3600

    def should_trigger_alert(self, current_time: Optional[datetime] = None) -> bool:
        """Check if this watermark should trigger a staleness alert.

        Args:
            current_time: Current time

        Returns:
            True if alert should be triggered
        """
        hours_since_update = self.hours_since_update(current_time)

        if self.is_missing():
            return True

        if self.is_stale():
            return hours_since_update > (self.expected_frequency_hours + self.grace_period_hours)

        return False


class WatermarkTracker:
    """Track watermarks for multiple datasets."""

    def __init__(self):
        """Initialize watermark tracker."""
        self.logger = create_logger(
            source_name="watermark_tracker",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.watermark.events",
            dataset="watermark_tracking"
        )

        # Watermark cache
        self.watermarks: Dict[str, WatermarkInfo] = {}

        # Configuration
        self.default_frequency_hours = 24
        self.default_grace_period_hours = 1

    def register_dataset(
        self,
        dataset: str,
        expected_frequency_hours: int = 24,
        grace_period_hours: int = 1,
        watermark_key: str = "logical_date"
    ) -> None:
        """Register a dataset for watermark tracking.

        Args:
            dataset: Dataset name
            expected_frequency_hours: Expected update frequency
            grace_period_hours: Grace period before considering stale
            watermark_key: Watermark key to track
        """
        self.watermarks[dataset] = WatermarkInfo(
            dataset=dataset,
            watermark_key=watermark_key,
            expected_frequency_hours=expected_frequency_hours,
            grace_period_hours=grace_period_hours
        )

        self.logger.log(
            LogLevel.INFO,
            f"Registered dataset for watermark tracking: {dataset}",
            "dataset_registered",
            dataset=dataset,
            expected_frequency_hours=expected_frequency_hours,
            grace_period_hours=grace_period_hours
        )

    def get_watermark(self, dataset: str) -> Optional[datetime]:
        """Get the last watermark for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            Last watermark timestamp or None
        """
        if dataset not in self.watermarks:
            self.register_dataset(dataset)

        return self._get_watermark_from_db(dataset)

    def update_watermark(
        self,
        dataset: str,
        watermark: datetime,
        policy: str = "exact"
    ) -> bool:
        """Update watermark for a dataset.

        Args:
            dataset: Dataset name
            watermark: New watermark timestamp
            policy: Rounding policy for watermark

        Returns:
            True if watermark was updated
        """
        try:
            # Update in database
            update_ingest_watermark(dataset, "logical_date", watermark, policy=policy)

            # Update cache
            if dataset in self.watermarks:
                self.watermarks[dataset].last_watermark = watermark
                self.watermarks[dataset].last_check_time = datetime.now()
                self.watermarks[dataset].status = WatermarkStatus.CURRENT
                self.watermarks[dataset].consecutive_failures = 0

            self.logger.log(
                LogLevel.INFO,
                f"Updated watermark for {dataset}",
                "watermark_updated",
                dataset=dataset,
                watermark=watermark.isoformat(),
                policy=policy
            )

            return True

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to update watermark for {dataset}: {e}",
                "watermark_update_error",
                dataset=dataset,
                error=str(e)
            )

            if dataset in self.watermarks:
                self.watermarks[dataset].consecutive_failures += 1

            return False

    def check_watermark_status(
        self,
        dataset: str,
        current_time: Optional[datetime] = None
    ) -> WatermarkStatus:
        """Check the status of a dataset's watermark.

        Args:
            dataset: Dataset name
            current_time: Current time

        Returns:
            Watermark status
        """
        if current_time is None:
            current_time = datetime.now()

        if dataset not in self.watermarks:
            self.register_dataset(dataset)

        watermark_info = self.watermarks[dataset]

        try:
            # Get watermark from database
            last_watermark = self._get_watermark_from_db(dataset)

            if last_watermark is None:
                watermark_info.status = WatermarkStatus.MISSING
                watermark_info.last_check_time = current_time
                return WatermarkStatus.MISSING

            # Update watermark info
            watermark_info.last_watermark = last_watermark
            watermark_info.last_check_time = current_time

            # Calculate hours since update
            hours_since_update = (current_time - last_watermark).total_seconds() / 3600

            # Determine status
            if hours_since_update <= watermark_info.grace_period_hours:
                watermark_info.status = WatermarkStatus.CURRENT
            else:
                watermark_info.status = WatermarkStatus.STALE

            return watermark_info.status

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Error checking watermark status for {dataset}: {e}",
                "watermark_status_error",
                dataset=dataset,
                error=str(e)
            )

            watermark_info.status = WatermarkStatus.ERROR
            watermark_info.consecutive_failures += 1
            return WatermarkStatus.ERROR

    def get_stale_watermarks(
        self,
        current_time: Optional[datetime] = None,
        include_missing: bool = True
    ) -> List[WatermarkInfo]:
        """Get watermarks that are stale or missing.

        Args:
            current_time: Current time
            include_missing: Whether to include missing watermarks

        Returns:
            List of stale/missing watermark information
        """
        if current_time is None:
            current_time = datetime.now()

        stale_watermarks = []

        for dataset, watermark_info in self.watermarks.items():
            try:
                # Update status
                self.check_watermark_status(dataset, current_time)

                # Check if stale
                if watermark_info.is_stale() or (include_missing and watermark_info.is_missing()):
                    stale_watermarks.append(watermark_info)

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error checking staleness for {dataset}: {e}",
                    "staleness_check_error",
                    dataset=dataset,
                    error=str(e)
                )

        return stale_watermarks

    def get_tracking_summary(self) -> Dict[str, Any]:
        """Get summary of watermark tracking status.

        Returns:
            Dictionary with tracking summary
        """
        total_datasets = len(self.watermarks)
        current_watermarks = 0
        stale_watermarks = 0
        missing_watermarks = 0
        error_watermarks = 0

        for watermark_info in self.watermarks.values():
            if watermark_info.is_current():
                current_watermarks += 1
            elif watermark_info.is_stale():
                stale_watermarks += 1
            elif watermark_info.is_missing():
                missing_watermarks += 1
            elif watermark_info.status == WatermarkStatus.ERROR:
                error_watermarks += 1

        return {
            "total_tracked_datasets": total_datasets,
            "current_watermarks": current_watermarks,
            "stale_watermarks": stale_watermarks,
            "missing_watermarks": missing_watermarks,
            "error_watermarks": error_watermarks,
            "tracking_coverage": current_watermarks / total_datasets if total_datasets > 0 else 0,
            "datasets": {
                dataset: info.__dict__
                for dataset, info in self.watermarks.items()
            }
        }

    def _get_watermark_from_db(self, dataset: str) -> Optional[datetime]:
        """Get watermark from database.

        Args:
            dataset: Dataset name

        Returns:
            Watermark timestamp or None
        """
        try:
            watermark_info = self.watermarks.get(dataset)
            if not watermark_info:
                return None

            watermark_str = get_ingest_watermark(dataset, watermark_info.watermark_key)
            if watermark_str:
                return datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Error getting watermark from DB for {dataset}: {e}",
                "db_watermark_error",
                dataset=dataset,
                error=str(e)
            )

        return None
