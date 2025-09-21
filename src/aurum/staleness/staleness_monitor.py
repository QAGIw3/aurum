"""Core staleness monitoring system."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

from ..db import get_ingest_watermark
from ..logging import StructuredLogger, LogLevel, create_logger


class StalenessLevel(str, Enum):
    """Staleness severity levels."""
    FRESH = "fresh"           # Data is current
    STALE = "stale"           # Data is stale but within grace period
    CRITICAL = "critical"     # Data is critically stale
    UNKNOWN = "unknown"       # Cannot determine staleness


@dataclass
class StalenessConfig:
    """Configuration for staleness monitoring."""

    # Staleness thresholds (in hours)
    warning_threshold_hours: int = 2      # Warning when data older than this
    critical_threshold_hours: int = 6     # Critical when data older than this
    grace_period_hours: int = 1           # Allow brief delays

    # Dataset-specific overrides
    dataset_thresholds: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # Monitoring configuration
    check_interval_minutes: int = 5
    timezone: str = "UTC"

    # Alert configuration
    enable_alerts: bool = True
    alert_cooldown_minutes: int = 15

    def get_thresholds(self, dataset: str) -> Dict[str, int]:
        """Get staleness thresholds for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            Dictionary with warning and critical thresholds
        """
        if dataset in self.dataset_thresholds:
            return self.dataset_thresholds[dataset]

        return {
            "warning": self.warning_threshold_hours,
            "critical": self.critical_threshold_hours,
            "grace": self.grace_period_hours
        }


@dataclass
class DatasetStaleness:
    """Staleness information for a dataset."""

    dataset: str
    last_watermark: Optional[datetime]
    current_time: datetime
    staleness_level: StalenessLevel
    hours_since_update: float
    warning_threshold_hours: int
    critical_threshold_hours: int
    grace_period_hours: int

    # Additional context
    expected_frequency_hours: Optional[int] = None
    sla_breach_duration_minutes: Optional[int] = None
    consecutive_failures: int = 0

    def is_fresh(self) -> bool:
        """Check if dataset is considered fresh."""
        return self.staleness_level == StalenessLevel.FRESH

    def is_stale(self) -> bool:
        """Check if dataset is stale."""
        return self.staleness_level == StalenessLevel.STALE

    def is_critical(self) -> bool:
        """Check if dataset staleness is critical."""
        return self.staleness_level == StalenessLevel.CRITICAL

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dataset": self.dataset,
            "last_watermark": self.last_watermark.isoformat() if self.last_watermark else None,
            "current_time": self.current_time.isoformat(),
            "staleness_level": self.staleness_level.value,
            "hours_since_update": self.hours_since_update,
            "warning_threshold_hours": self.warning_threshold_hours,
            "critical_threshold_hours": self.critical_threshold_hours,
            "grace_period_hours": self.grace_period_hours,
            "expected_frequency_hours": self.expected_frequency_hours,
            "sla_breach_duration_minutes": self.sla_breach_duration_minutes,
            "consecutive_failures": self.consecutive_failures
        }


class StalenessMonitor:
    """Monitor dataset staleness based on watermarks."""

    def __init__(self, config: StalenessConfig):
        """Initialize staleness monitor.

        Args:
            config: Staleness configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="staleness_monitor",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.staleness.events",
            dataset="staleness_monitoring"
        )

        # State tracking
        self.dataset_states: Dict[str, DatasetStaleness] = {}
        self.last_check_times: Dict[str, datetime] = {}
        self.alert_cooldowns: Dict[str, datetime] = {}

        # Register expected datasets
        self.expected_datasets: Dict[str, Dict[str, Any]] = {}

    def register_dataset(
        self,
        dataset: str,
        expected_frequency_hours: int = 1,
        sla_config: Optional[Dict[str, Any]] = None
    ) -> None:
        """Register a dataset for staleness monitoring.

        Args:
            dataset: Dataset name
            expected_frequency_hours: Expected update frequency in hours
            sla_config: SLA configuration for this dataset
        """
        self.expected_datasets[dataset] = {
            "expected_frequency_hours": expected_frequency_hours,
            "sla_config": sla_config or {},
            "registered_at": datetime.now()
        }

        self.logger.log(
            LogLevel.INFO,
            f"Registered dataset for staleness monitoring: {dataset}",
            "dataset_registered",
            dataset=dataset,
            expected_frequency_hours=expected_frequency_hours
        )

    def check_dataset_staleness(
        self,
        dataset: str,
        current_time: Optional[datetime] = None
    ) -> DatasetStaleness:
        """Check staleness of a specific dataset.

        Args:
            dataset: Dataset name
            current_time: Current time (defaults to now)

        Returns:
            DatasetStaleness information
        """
        if current_time is None:
            current_time = datetime.now()

        # Get watermark
        watermark = self._get_dataset_watermark(dataset)

        # Get thresholds
        thresholds = self.config.get_thresholds(dataset)

        # Calculate staleness
        staleness_info = self._calculate_staleness(
            dataset, watermark, current_time, thresholds
        )

        # Update state
        self.dataset_states[dataset] = staleness_info
        self.last_check_times[dataset] = current_time

        # Log the check
        self.logger.log(
            LogLevel.INFO,
            f"Staleness check for {dataset}: {staleness_info.staleness_level.value} "
            f"({staleness_info.hours_since_update".2f"}h old)",
            "staleness_check",
            dataset=dataset,
            staleness_level=staleness_info.staleness_level.value,
            hours_since_update=staleness_info.hours_since_update,
            is_stale=staleness_info.is_stale(),
            is_critical=staleness_info.is_critical()
        )

        return staleness_info

    def check_all_datasets(self) -> Dict[str, DatasetStaleness]:
        """Check staleness for all registered datasets.

        Returns:
            Dictionary mapping dataset names to staleness information
        """
        current_time = datetime.now()
        results = {}

        for dataset in self.expected_datasets.keys():
            try:
                staleness_info = self.check_dataset_staleness(dataset, current_time)
                results[dataset] = staleness_info
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error checking staleness for {dataset}: {e}",
                    "staleness_check_error",
                    dataset=dataset,
                    error=str(e)
                )

        return results

    def get_stale_datasets(
        self,
        min_level: StalenessLevel = StalenessLevel.STALE
    ) -> List[DatasetStaleness]:
        """Get all datasets that are stale or worse.

        Args:
            min_level: Minimum staleness level to include

        Returns:
            List of stale dataset information
        """
        stale_datasets = []

        for dataset, staleness_info in self.dataset_states.items():
            if self._is_staleness_level_at_least(staleness_info.staleness_level, min_level):
                stale_datasets.append(staleness_info)

        return stale_datasets

    def get_critical_datasets(self) -> List[DatasetStaleness]:
        """Get datasets with critical staleness."""
        return self.get_stale_datasets(StalenessLevel.CRITICAL)

    def should_alert(self, dataset: str, staleness_level: StalenessLevel) -> bool:
        """Check if an alert should be sent for dataset staleness.

        Args:
            dataset: Dataset name
            staleness_level: Current staleness level

        Returns:
            True if alert should be sent
        """
        alert_key = f"{dataset}:{staleness_level.value}"

        now = datetime.now()
        last_alert = self.alert_cooldowns.get(alert_key)

        if last_alert is None:
            self.alert_cooldowns[alert_key] = now
            return True

        time_since_last_alert = now - last_alert
        if time_since_last_alert.total_seconds() > (self.config.alert_cooldown_minutes * 60):
            self.alert_cooldowns[alert_key] = now
            return True

        return False

    def _get_dataset_watermark(self, dataset: str) -> Optional[datetime]:
        """Get the last watermark for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            Last watermark timestamp or None
        """
        try:
            watermark_str = get_ingest_watermark(dataset, "logical_date")
            if watermark_str:
                return datetime.fromisoformat(watermark_str.replace('Z', '+00:00'))
        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Error getting watermark for {dataset}: {e}",
                "watermark_retrieval_error",
                dataset=dataset,
                error=str(e)
            )

        return None

    def _calculate_staleness(
        self,
        dataset: str,
        watermark: Optional[datetime],
        current_time: datetime,
        thresholds: Dict[str, int]
    ) -> DatasetStaleness:
        """Calculate staleness for a dataset.

        Args:
            dataset: Dataset name
            watermark: Last watermark timestamp
            current_time: Current time
            thresholds: Staleness thresholds

        Returns:
            DatasetStaleness information
        """
        warning_hours = thresholds["warning"]
        critical_hours = thresholds["critical"]
        grace_hours = thresholds["grace"]

        if watermark is None:
            # No watermark available - assume critical
            staleness_level = StalenessLevel.CRITICAL
            hours_since_update = float('inf')
            sla_breach_duration = None
        else:
            hours_since_update = (current_time - watermark).total_seconds() / 3600

            if hours_since_update <= grace_hours:
                staleness_level = StalenessLevel.FRESH
                sla_breach_duration = None
            elif hours_since_update <= warning_hours:
                staleness_level = StalenessLevel.STALE
                sla_breach_duration = int((hours_since_update - grace_hours) * 60)
            else:
                staleness_level = StalenessLevel.CRITICAL
                sla_breach_duration = int((hours_since_update - grace_hours) * 60)

        # Get expected frequency
        expected_freq = None
        if dataset in self.expected_datasets:
            expected_freq = self.expected_datasets[dataset]["expected_frequency_hours"]

        return DatasetStaleness(
            dataset=dataset,
            last_watermark=watermark,
            current_time=current_time,
            staleness_level=staleness_level,
            hours_since_update=hours_since_update,
            warning_threshold_hours=warning_hours,
            critical_threshold_hours=critical_hours,
            grace_period_hours=grace_hours,
            expected_frequency_hours=expected_freq,
            sla_breach_duration_minutes=sla_breach_duration,
            consecutive_failures=self._get_consecutive_failures(dataset)
        )

    def _is_staleness_level_at_least(
        self,
        actual_level: StalenessLevel,
        min_level: StalenessLevel
    ) -> bool:
        """Check if actual staleness level is at least as severe as minimum level.

        Args:
            actual_level: Actual staleness level
            min_level: Minimum level to check against

        Returns:
            True if actual level is at least as severe
        """
        level_order = [
            StalenessLevel.FRESH,
            StalenessLevel.STALE,
            StalenessLevel.CRITICAL,
            StalenessLevel.UNKNOWN
        ]

        actual_index = level_order.index(actual_level)
        min_index = level_order.index(min_level)

        return actual_index >= min_index

    def _get_consecutive_failures(self, dataset: str) -> int:
        """Get count of consecutive staleness checks for a dataset.

        Args:
            dataset: Dataset name

        Returns:
            Number of consecutive failures
        """
        # This would be implemented with persistent state
        # For now, return a placeholder
        return 0

    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get summary of staleness monitoring status.

        Returns:
            Dictionary with monitoring summary
        """
        all_datasets = list(self.expected_datasets.keys())
        checked_datasets = list(self.dataset_states.keys())

        stale_datasets = self.get_stale_datasets()
        critical_datasets = self.get_critical_datasets()

        summary = {
            "total_registered_datasets": len(all_datasets),
            "total_checked_datasets": len(checked_datasets),
            "stale_datasets": len(stale_datasets),
            "critical_datasets": len(critical_datasets),
            "monitoring_coverage": len(checked_datasets) / len(all_datasets) if all_datasets else 0,
            "staleness_by_level": {
                "fresh": len([d for d in self.dataset_states.values() if d.is_fresh()]),
                "stale": len([d for d in self.dataset_states.values() if d.is_stale()]),
                "critical": len([d for d in self.dataset_states.values() if d.is_critical()]),
            },
            "datasets": {}
        }

        for dataset, staleness_info in self.dataset_states.items():
            summary["datasets"][dataset] = staleness_info.to_dict()

        return summary
