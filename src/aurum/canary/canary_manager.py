"""Core canary dataset management system."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Any, Callable

from ..logging import StructuredLogger, LogLevel, create_logger


class CanaryStatus(str, Enum):
    """Canary execution status."""
    HEALTHY = "healthy"         # Canary ran successfully
    UNHEALTHY = "unhealthy"     # Canary failed or returned unexpected data
    TIMEOUT = "timeout"         # Canary execution timed out
    ERROR = "error"            # Error during canary execution
    DISABLED = "disabled"      # Canary is disabled


@dataclass
class CanaryConfig:
    """Configuration for canary monitoring."""

    # Basic configuration
    name: str
    source: str
    dataset: str
    api_endpoint: str
    schedule: str = "0 */6 * * *"  # Every 6 hours
    enabled: bool = True

    # API configuration
    api_params: Dict[str, Any] = field(default_factory=dict)
    expected_response_format: str = "json"  # json, csv, xml
    expected_fields: List[str] = field(default_factory=list)

    # Health check configuration
    timeout_seconds: int = 60
    max_retries: int = 3
    retry_delay_seconds: int = 5

    # Data validation
    min_records_expected: int = 1
    max_records_expected: Optional[int] = None
    validation_rules: Dict[str, Any] = field(default_factory=dict)

    # Alerting configuration
    alert_on_failure: bool = True
    alert_on_recovery: bool = True
    alert_channels: List[str] = field(default_factory=lambda: ["email"])

    # Metadata
    description: str = ""
    tags: List[str] = field(default_factory=list)
    owner: str = "aurum-data"

    def __post_init__(self):
        if not self.tags:
            self.tags = ["canary", self.source]


@dataclass
class CanaryDataset:
    """Information about a canary dataset execution."""

    name: str
    source: str
    dataset: str
    config: CanaryConfig

    # Execution state
    last_run: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    status: CanaryStatus = CanaryStatus.DISABLED

    # Results
    execution_time_seconds: Optional[float] = None
    records_processed: int = 0
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    # Health metrics
    consecutive_failures: int = 0
    total_runs: int = 0
    successful_runs: int = 0
    failure_rate: float = 0.0

    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_healthy(self) -> bool:
        """Check if canary is currently healthy."""
        return self.status == CanaryStatus.HEALTHY

    def is_unhealthy(self) -> bool:
        """Check if canary is currently unhealthy."""
        return self.status in [CanaryStatus.UNHEALTHY, CanaryStatus.TIMEOUT, CanaryStatus.ERROR]

    def should_alert(self, alert_on_failure: bool = True, alert_on_recovery: bool = True) -> bool:
        """Check if this canary should trigger an alert."""
        if not alert_on_failure and self.is_unhealthy():
            return False
        if not alert_on_recovery and self.is_healthy():
            return False

        # Alert on state changes
        if self.last_run:
            time_since_last_run = datetime.now() - self.last_run
            # Don't alert if we just ran
            if time_since_last_run < timedelta(minutes=5):
                return False

        return True

    def update_status(self, status: CanaryStatus, execution_time: Optional[float] = None,
                     records_processed: int = 0, errors: List[str] = None,
                     warnings: List[str] = None) -> None:
        """Update canary status and metrics."""
        now = datetime.now()
        self.last_run = now
        self.execution_time_seconds = execution_time
        self.records_processed = records_processed

        if errors:
            self.errors.extend(errors)
        if warnings:
            self.warnings.extend(warnings)

        old_status = self.status
        self.status = status

        # Update run counters
        self.total_runs += 1
        if status == CanaryStatus.HEALTHY:
            self.successful_runs += 1
            self.last_success = now
            if old_status != CanaryStatus.HEALTHY:
                # Recovery - reset consecutive failures
                self.consecutive_failures = 0
        else:
            self.consecutive_failures += 1
            self.last_failure = now

        # Update failure rate
        if self.total_runs > 0:
            self.failure_rate = (self.total_runs - self.successful_runs) / self.total_runs

    def to_dict(self) -> Dict[str, Any]:
        """Convert canary to dictionary."""
        return {
            "name": self.name,
            "source": self.source,
            "dataset": self.dataset,
            "status": self.status.value,
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_failure": self.last_failure.isoformat() if self.last_failure else None,
            "execution_time_seconds": self.execution_time_seconds,
            "records_processed": self.records_processed,
            "consecutive_failures": self.consecutive_failures,
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "failure_rate": self.failure_rate,
            "errors": self.errors,
            "warnings": self.warnings,
            "metadata": self.metadata,
            "is_healthy": self.is_healthy(),
            "is_unhealthy": self.is_unhealthy()
        }


class CanaryManager:
    """Manage canary datasets for API health monitoring."""

    def __init__(self):
        """Initialize canary manager."""
        self.logger = create_logger(
            source_name="canary_manager",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.canary.events",
            dataset="canary_monitoring"
        )

        # Canary storage
        self.canaries: Dict[str, CanaryDataset] = {}

        # Configuration registry
        self.config_registry: Dict[str, CanaryConfig] = {}

    def register_canary(self, config: CanaryConfig) -> CanaryDataset:
        """Register a new canary dataset.

        Args:
            config: Canary configuration

        Returns:
            Created CanaryDataset instance
        """
        canary = CanaryDataset(
            name=config.name,
            source=config.source,
            dataset=config.dataset,
            config=config,
            status=CanaryStatus.DISABLED
        )

        self.canaries[config.name] = canary
        self.config_registry[config.name] = config

        self.logger.log(
            LogLevel.INFO,
            f"Registered canary dataset: {config.name}",
            "canary_registered",
            canary_name=config.name,
            source=config.source,
            dataset=config.dataset,
            schedule=config.schedule
        )

        return canary

    def get_canary(self, name: str) -> Optional[CanaryDataset]:
        """Get a canary dataset by name.

        Args:
            name: Canary name

        Returns:
            CanaryDataset instance or None
        """
        return self.canaries.get(name)

    def get_canaries_by_source(self, source: str) -> List[CanaryDataset]:
        """Get all canaries for a specific source.

        Args:
            source: Data source name

        Returns:
            List of canary datasets
        """
        return [c for c in self.canaries.values() if c.source == source]

    def get_unhealthy_canaries(self) -> List[CanaryDataset]:
        """Get all canaries that are currently unhealthy.

        Returns:
            List of unhealthy canary datasets
        """
        return [c for c in self.canaries.values() if c.is_unhealthy()]

    def get_critical_canaries(self) -> List[CanaryDataset]:
        """Get canaries with critical issues (high failure rates, timeouts, etc.).

        Returns:
            List of critical canary datasets
        """
        critical_canaries = []

        for canary in self.canaries.values():
            if (canary.consecutive_failures >= 3 or
                canary.failure_rate > 0.5 or
                canary.status in [CanaryStatus.TIMEOUT, CanaryStatus.ERROR]):
                critical_canaries.append(canary)

        return critical_canaries

    def update_canary_status(
        self,
        name: str,
        status: CanaryStatus,
        execution_time: Optional[float] = None,
        records_processed: int = 0,
        errors: List[str] = None,
        warnings: List[str] = None
    ) -> bool:
        """Update the status of a canary.

        Args:
            name: Canary name
            status: New status
            execution_time: Execution time in seconds
            records_processed: Number of records processed
            errors: List of error messages
            warnings: List of warning messages

        Returns:
            True if status was updated
        """
        canary = self.canaries.get(name)
        if not canary:
            self.logger.log(
                LogLevel.ERROR,
                f"Cannot update status for unknown canary: {name}",
                "canary_not_found",
                canary_name=name
            )
            return False

        old_status = canary.status
        canary.update_status(status, execution_time, records_processed, errors, warnings)

        self.logger.log(
            LogLevel.INFO,
            f"Updated canary {name} status: {old_status.value} -> {status.value}",
            "canary_status_updated",
            canary_name=name,
            old_status=old_status.value,
            new_status=status.value,
            consecutive_failures=canary.consecutive_failures
        )

        return True

    def get_canary_health_summary(self) -> Dict[str, Any]:
        """Get health summary for all canaries.

        Returns:
            Dictionary with health summary
        """
        total_canaries = len(self.canaries)
        healthy_canaries = len([c for c in self.canaries.values() if c.is_healthy()])
        unhealthy_canaries = len([c for c in self.canaries.values() if c.is_unhealthy()])
        disabled_canaries = len([c for c in self.canaries.values() if c.status == CanaryStatus.DISABLED])

        # Health by source
        health_by_source = {}
        for canary in self.canaries.values():
            source = canary.source
            if source not in health_by_source:
                health_by_source[source] = {"healthy": 0, "unhealthy": 0, "disabled": 0}

            if canary.is_healthy():
                health_by_source[source]["healthy"] += 1
            elif canary.is_unhealthy():
                health_by_source[source]["unhealthy"] += 1
            else:
                health_by_source[source]["disabled"] += 1

        return {
            "total_canaries": total_canaries,
            "healthy_canaries": healthy_canaries,
            "unhealthy_canaries": unhealthy_canaries,
            "disabled_canaries": disabled_canaries,
            "overall_health_rate": healthy_canaries / total_canaries if total_canaries > 0 else 0,
            "health_by_source": health_by_source,
            "critical_canaries": [
                canary.to_dict() for canary in self.get_critical_canaries()
            ],
            "canaries": {
                name: canary.to_dict() for name, canary in self.canaries.items()
            }
        }

    def enable_canary(self, name: str) -> bool:
        """Enable a canary dataset.

        Args:
            name: Canary name

        Returns:
            True if canary was enabled
        """
        canary = self.canaries.get(name)
        if canary:
            canary.status = CanaryStatus.HEALTHY  # Reset to healthy when enabling
            self.logger.log(
                LogLevel.INFO,
                f"Enabled canary dataset: {name}",
                "canary_enabled",
                canary_name=name
            )
            return True
        return False

    def disable_canary(self, name: str) -> bool:
        """Disable a canary dataset.

        Args:
            name: Canary name

        Returns:
            True if canary was disabled
        """
        canary = self.canaries.get(name)
        if canary:
            canary.status = CanaryStatus.DISABLED
            self.logger.log(
                LogLevel.INFO,
                f"Disabled canary dataset: {name}",
                "canary_disabled",
                canary_name=name
            )
            return True
        return False
