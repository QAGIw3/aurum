"""Staleness detection and alerting system."""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable

from .staleness_monitor import StalenessMonitor, StalenessConfig, StalenessLevel
from ..logging import StructuredLogger, LogLevel, create_logger


@dataclass
class StalenessCheck:
    """Configuration for a staleness check."""

    dataset: str
    check_interval_minutes: int = 5
    enabled: bool = True
    alert_on_stale: bool = True
    alert_on_critical: bool = True
    custom_thresholds: Optional[Dict[str, int]] = None

    # Callback functions
    on_stale_callback: Optional[Callable] = None
    on_critical_callback: Optional[Callable] = None
    on_fresh_callback: Optional[Callable] = None


class StalenessDetector:
    """Detect and alert on dataset staleness."""

    def __init__(self, config: StalenessConfig):
        """Initialize staleness detector.

        Args:
            config: Staleness configuration
        """
        self.config = config
        self.monitor = StalenessMonitor(config)
        self.logger = create_logger(
            source_name="staleness_detector",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.staleness.detections",
            dataset="staleness_detection"
        )

        # Check configurations
        self.checks: Dict[str, StalenessCheck] = {}

        # State tracking
        self.last_check_times: Dict[str, datetime] = {}
        self.consecutive_failures: Dict[str, int] = {}

    def add_staleness_check(self, check: StalenessCheck) -> None:
        """Add a staleness check configuration.

        Args:
            check: Staleness check configuration
        """
        self.checks[check.dataset] = check
        self.monitor.register_dataset(
            dataset=check.dataset,
            expected_frequency_hours=24  # Default to daily
        )

        self.logger.log(
            LogLevel.INFO,
            f"Added staleness check for dataset: {check.dataset}",
            "staleness_check_added",
            dataset=check.dataset,
            check_interval_minutes=check.check_interval_minutes
        )

    def run_staleness_checks(self) -> Dict[str, Any]:
        """Run staleness checks for all configured datasets.

        Returns:
            Dictionary with check results
        """
        current_time = datetime.now()
        results = {
            "timestamp": current_time.isoformat(),
            "total_checks": len(self.checks),
            "datasets_checked": 0,
            "stale_datasets": 0,
            "critical_datasets": 0,
            "alerts_sent": 0,
            "results": {}
        }

        for dataset, check in self.checks.items():
            if not check.enabled:
                continue

            try:
                # Check if we should run this check
                if not self._should_run_check(dataset, current_time):
                    continue

                # Run the check
                staleness_info = self.monitor.check_dataset_staleness(dataset, current_time)

                # Record results
                results["datasets_checked"] += 1
                results["results"][dataset] = staleness_info.to_dict()

                if staleness_info.is_stale():
                    results["stale_datasets"] += 1
                    if check.alert_on_stale:
                        self._handle_stale_dataset(dataset, staleness_info, check)

                if staleness_info.is_critical():
                    results["critical_datasets"] += 1
                    if check.alert_on_critical:
                        self._handle_critical_dataset(dataset, staleness_info, check)

                if staleness_info.is_fresh() and check.on_fresh_callback:
                    try:
                        check.on_fresh_callback(dataset, staleness_info)
                    except Exception as e:
                        self.logger.log(
                            LogLevel.ERROR,
                            f"Error in fresh callback for {dataset}: {e}",
                            "fresh_callback_error",
                            dataset=dataset,
                            error=str(e)
                        )

                # Reset consecutive failures on successful check
                if dataset in self.consecutive_failures:
                    self.consecutive_failures[dataset] = 0

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error running staleness check for {dataset}: {e}",
                    "staleness_check_error",
                    dataset=dataset,
                    error=str(e)
                )

                # Track consecutive failures
                self.consecutive_failures[dataset] = self.consecutive_failures.get(dataset, 0) + 1

                # Alert on repeated check failures
                if self.consecutive_failures[dataset] >= 3:
                    self._alert_check_failure(dataset, self.consecutive_failures[dataset])

        return results

    def _should_run_check(self, dataset: str, current_time: datetime) -> bool:
        """Determine if a staleness check should run now.

        Args:
            dataset: Dataset name
            current_time: Current time

        Returns:
            True if check should run
        """
        check = self.checks.get(dataset)
        if not check:
            return False

        last_check = self.last_check_times.get(dataset)
        if last_check is None:
            return True

        time_since_last_check = current_time - last_check
        should_run = time_since_last_check.total_seconds() >= (check.check_interval_minutes * 60)

        return should_run

    def _handle_stale_dataset(
        self,
        dataset: str,
        staleness_info: Any,
        check: StalenessCheck
    ) -> None:
        """Handle a stale dataset.

        Args:
            dataset: Dataset name
            staleness_info: Staleness information
            check: Check configuration
        """
        if not self.monitor.should_alert(dataset, StalenessLevel.STALE):
            return

        self.logger.log(
            LogLevel.WARN,
            f"Dataset {dataset} is stale ({staleness_info.hours_since_update:.2f}h old)",
            "dataset_stale",
            dataset=dataset,
            hours_since_update=staleness_info.hours_since_update,
            expected_threshold=staleness_info.warning_threshold_hours
        )

        # Call custom callback if provided
        if check.on_stale_callback:
            try:
                check.on_stale_callback(dataset, staleness_info)
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error in stale callback for {dataset}: {e}",
                    "stale_callback_error",
                    dataset=dataset,
                    error=str(e)
                )

    def _handle_critical_dataset(
        self,
        dataset: str,
        staleness_info: Any,
        check: StalenessCheck
    ) -> None:
        """Handle a critically stale dataset.

        Args:
            dataset: Dataset name
            staleness_info: Staleness information
            check: Check configuration
        """
        if not self.monitor.should_alert(dataset, StalenessLevel.CRITICAL):
            return

        self.logger.log(
            LogLevel.ERROR,
            f"Dataset {dataset} is critically stale ({staleness_info.hours_since_update:.2f}h old)",
            "dataset_critical",
            dataset=dataset,
            hours_since_update=staleness_info.hours_since_update,
            expected_threshold=staleness_info.critical_threshold_hours
        )

        # Call custom callback if provided
        if check.on_critical_callback:
            try:
                check.on_critical_callback(dataset, staleness_info)
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Error in critical callback for {dataset}: {e}",
                    "critical_callback_error",
                    dataset=dataset,
                    error=str(e)
                )

    def _alert_check_failure(self, dataset: str, failure_count: int) -> None:
        """Alert on repeated staleness check failures.

        Args:
            dataset: Dataset name
            failure_count: Number of consecutive failures
        """
        self.logger.log(
            LogLevel.ERROR,
            f"Staleness check for {dataset} failed {failure_count} consecutive times",
            "check_failure_alert",
            dataset=dataset,
            failure_count=failure_count
        )

    def get_detector_status(self) -> Dict[str, Any]:
        """Get status of the staleness detector.

        Returns:
            Dictionary with detector status
        """
        return {
            "total_checks": len(self.checks),
            "enabled_checks": len([c for c in self.checks.values() if c.enabled]),
            "consecutive_failures": self.consecutive_failures.copy(),
            "last_check_times": {
                dataset: check_time.isoformat()
                for dataset, check_time in self.last_check_times.items()
            },
            "monitor_summary": self.monitor.get_monitoring_summary()
        }

    def set_check_enabled(self, dataset: str, enabled: bool) -> None:
        """Enable or disable a staleness check.

        Args:
            dataset: Dataset name
            enabled: Whether to enable the check
        """
        if dataset in self.checks:
            self.checks[dataset].enabled = enabled
            self.logger.log(
                LogLevel.INFO,
                f"{'Enabled' if enabled else 'Disabled'} staleness check for {dataset}",
                "check_enabled_state_changed",
                dataset=dataset,
                enabled=enabled
            )
