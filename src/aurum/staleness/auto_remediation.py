"""Auto-remediation system for staleness detection with circuit breakers.

This module provides:
- Automatic remediation actions for stale datasets
- Circuit breaker patterns for failing collectors
- Intelligent retry and backoff strategies
- Integration with existing staleness monitoring
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Callable, Awaitable, Any
from abc import ABC, abstractmethod

from .staleness_monitor import StalenessLevel, DatasetStaleness, SLOConfig
from ..logging import StructuredLogger, LogLevel, create_logger
from ..common.circuit_breaker import CircuitBreakerState

logger = logging.getLogger(__name__)


class RemediationAction(str, Enum):
    """Types of remediation actions."""
    RETRY_INGESTION = "retry_ingestion"
    RESET_COLLECTOR = "reset_collector"
    FAILOVER_SOURCE = "failover_source"
    REDUCE_THROUGHPUT = "reduce_throughput"
    INCREASE_TIMEOUT = "increase_timeout"
    NOTIFY_ONCALL = "notify_oncall"
    CREATE_INCIDENT = "create_incident"
    SLO_BREACH_REMEDIATION = "slo_breach_remediation"
    EMERGENCY_RECOVERY = "emergency_recovery"


@dataclass
class RemediationConfig:
    """Configuration for auto-remediation."""

    # Circuit breaker settings
    failure_threshold: int = 3
    recovery_timeout_minutes: int = 10
    half_open_max_calls: int = 5

    # Remediation triggers
    auto_remediation_enabled: bool = True
    remediation_cooldown_minutes: int = 15

    # Action-specific settings
    max_retry_attempts: int = 3
    retry_backoff_multiplier: float = 2.0
    max_failover_sources: int = 2

    # Notification settings
    notify_on_remediation: bool = True
    incident_threshold_failures: int = 5


@dataclass
class CollectorHealth:
    """Health status of a data collector."""

    collector_name: str
    dataset: str
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    consecutive_failures: int = 0
    total_requests: int = 0
    total_failures: int = 0

    # Performance metrics
    avg_latency_seconds: float = 0.0
    throughput_rps: float = 0.0

    def record_success(self, latency_seconds: float):
        """Record a successful collection."""
        self.state = CircuitBreakerState.CLOSED
        self.success_count += 1
        self.last_success_time = datetime.now()
        self.consecutive_failures = 0

        # Update metrics
        self._update_latency_metrics(latency_seconds)
        self.total_requests += 1

    def record_failure(self, error_type: str):
        """Record a failed collection."""
        self.failure_count += 1
        self.total_failures += 1
        self.consecutive_failures += 1
        self.last_failure_time = datetime.now()

        # Update throughput
        elapsed = (datetime.now() - (self.last_success_time or datetime.now())).total_seconds()
        self.throughput_rps = self.success_count / max(elapsed, 1)

        # Check if we should open circuit breaker
        if (self.consecutive_failures >= self._get_failure_threshold() and
            self.state != CircuitBreakerState.OPEN):
            self._open_circuit_breaker()

    def can_execute(self) -> bool:
        """Check if collector can execute (circuit breaker logic)."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
                return True
            return False
        elif self.state == CircuitBreakerState.HALF_OPEN:
            return True
        return False

    def _get_failure_threshold(self) -> int:
        """Get dynamic failure threshold based on performance."""
        # Increase threshold if collector has been reliable
        base_threshold = 3
        if self.total_requests > 100 and (self.total_failures / self.total_requests) < 0.1:
            return base_threshold + 2
        return base_threshold

    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker."""
        if self.last_failure_time is None:
            return True

        recovery_time = datetime.now() - self.last_failure_time
        return recovery_time.total_seconds() >= (self._get_recovery_timeout() * 60)

    def _get_recovery_timeout(self) -> int:
        """Get adaptive recovery timeout."""
        # Longer timeout for consistently failing collectors
        base_timeout = 10
        if self.consecutive_failures > 5:
            return base_timeout * 2
        return base_timeout

    def _open_circuit_breaker(self):
        """Open the circuit breaker."""
        self.state = CircuitBreakerState.OPEN
        logger.warning(f"Circuit breaker opened for collector '{self.collector_name}' after {self.consecutive_failures} failures")

    def _transition_to_half_open(self):
        """Transition to half-open state."""
        self.state = CircuitBreakerState.HALF_OPEN
        self.failure_count = 0
        self.success_count = 0
        logger.info(f"Circuit breaker half-open for collector '{self.collector_name}'")

    def _update_latency_metrics(self, latency_seconds: float):
        """Update latency metrics."""
        if self.total_requests == 0:
            self.avg_latency_seconds = latency_seconds
        else:
            # Exponential moving average
            alpha = 0.1
            self.avg_latency_seconds = alpha * latency_seconds + (1 - alpha) * self.avg_latency_seconds


@dataclass
class RemediationActionResult:
    """Result of a remediation action."""

    action: RemediationAction
    collector_name: str
    dataset: str
    success: bool
    executed_at: datetime
    error_message: Optional[str] = None
    remediation_time_seconds: Optional[float] = None


class RemediationStrategy(ABC):
    """Abstract base class for remediation strategies."""

    @abstractmethod
    async def execute(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> RemediationActionResult:
        """Execute the remediation strategy."""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Get strategy name."""
        pass

    @abstractmethod
    def can_handle(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> bool:
        """Check if this strategy can handle the situation."""
        pass


class RetryIngestionStrategy(RemediationStrategy):
    """Strategy to retry failed ingestion."""

    def get_name(self) -> str:
        return "retry_ingestion"

    def can_handle(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> bool:
        return collector_health.consecutive_failures < 3

    async def execute(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> RemediationActionResult:
        """Execute retry strategy."""
        start_time = datetime.now()

        try:
            # Implementation would trigger ingestion retry
            # For now, simulate retry logic
            await asyncio.sleep(1)  # Simulate retry time

            result = RemediationActionResult(
                action=RemediationAction.RETRY_INGESTION,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=True,
                executed_at=start_time,
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )

            logger.info(f"Retry ingestion executed for {staleness_info.dataset}")
            return result

        except Exception as e:
            return RemediationActionResult(
                action=RemediationAction.RETRY_INGESTION,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=False,
                executed_at=start_time,
                error_message=str(e),
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )


class ResetCollectorStrategy(RemediationStrategy):
    """Strategy to reset a failing collector."""

    def get_name(self) -> str:
        return "reset_collector"

    def can_handle(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> bool:
        return collector_health.consecutive_failures >= 3

    async def execute(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> RemediationActionResult:
        """Execute collector reset strategy."""
        start_time = datetime.now()

        try:
            # Implementation would reset the collector
            # For now, simulate reset
            await asyncio.sleep(2)  # Simulate reset time

            result = RemediationActionResult(
                action=RemediationAction.RESET_COLLECTOR,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=True,
                executed_at=start_time,
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )

            logger.info(f"Collector reset executed for {staleness_info.dataset}")
            return result

        except Exception as e:
            return RemediationActionResult(
                action=RemediationAction.RESET_COLLECTOR,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=False,
                executed_at=start_time,
                error_message=str(e),
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )


class FailoverSourceStrategy(RemediationStrategy):
    """Strategy to failover to alternative data source."""

    def get_name(self) -> str:
        return "failover_source"

    def can_handle(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> bool:
        return collector_health.consecutive_failures >= 5

    async def execute(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> RemediationActionResult:
        """Execute failover strategy."""
        start_time = datetime.now()

        try:
            # Implementation would switch to alternative source
            # For now, simulate failover
            await asyncio.sleep(3)  # Simulate failover time

            result = RemediationActionResult(
                action=RemediationAction.FAILOVER_SOURCE,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=True,
                executed_at=start_time,
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )

            logger.info(f"Source failover executed for {staleness_info.dataset}")
            return result

        except Exception as e:
            return RemediationActionResult(
                action=RemediationAction.FAILOVER_SOURCE,
                collector_name=collector_health.collector_name,
                dataset=staleness_info.dataset,
                success=False,
                executed_at=start_time,
                error_message=str(e),
                remediation_time_seconds=(datetime.now() - start_time).total_seconds()
            )


class SLOBasedRemediationStrategy(RemediationStrategy):
    """Strategy for SLO-based remediation."""

    def get_name(self) -> str:
        return "slo_breach_remediation"

    def can_handle(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> bool:
        """Check if SLO-based remediation is needed."""
        return (staleness_info.slo_status == "breached" and
                staleness_info.slo_breach_duration_minutes is not None and
                staleness_info.slo_breach_duration_minutes > 0)

    async def execute(self, collector_health: CollectorHealth, staleness_info: DatasetStaleness) -> RemediationActionResult:
        """Execute SLO-based remediation."""
        start_time = datetime.now()

        try:
            # Determine remediation actions based on SLO breach severity
            actions_taken = []

            if staleness_info.slo_breach_duration_minutes and staleness_info.slo_breach_duration_minutes > 60:
                # Critical SLO breach - trigger emergency recovery
                actions_taken.append("emergency_recovery")
            elif staleness_info.slo_breach_duration_minutes and staleness_info.slo_breach_duration_minutes > 30:
                # Significant SLO breach - trigger incident creation
                actions_taken.append("create_incident")
            else:
                # Minor SLO breach - just notify
                actions_taken.append("notify_oncall")

            # Additional actions based on collector health
            if collector_health.consecutive_failures > 0:
                actions_taken.append("reset_collector")
            else:
                actions_taken.append("retry_ingestion")

            duration = (datetime.now() - start_time).total_seconds()

            return RemediationActionResult(
                action=RemediationAction.SLO_BREACH_REMEDIATION,
                collector_name=collector_health.collector_name,
                dataset=collector_health.dataset,
                success=True,
                executed_at=start_time,
                remediation_time_seconds=duration,
                error_message=None
            )

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()

            return RemediationActionResult(
                action=RemediationAction.SLO_BREACH_REMEDIATION,
                collector_name=collector_health.collector_name,
                dataset=collector_health.dataset,
                success=False,
                executed_at=start_time,
                remediation_time_seconds=duration,
                error_message=str(e)
            )


class AutoRemediationManager:
    """Manager for automatic staleness remediation."""

    def __init__(self, config: RemediationConfig):
        self.config = config
        self.logger = create_logger(
            source_name="auto_remediation",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.staleness.remediation",
            dataset="staleness_remediation"
        )

        # Collector health tracking
        self.collector_health: Dict[str, CollectorHealth] = {}

        # Remediation strategies
        self.strategies: List[RemediationStrategy] = [
            RetryIngestionStrategy(),
            ResetCollectorStrategy(),
            FailoverSourceStrategy(),
            SLOBasedRemediationStrategy()
        ]

        # State management
        self.last_remediation_times: Dict[str, datetime] = {}
        self.remediation_history: List[RemediationActionResult] = []

        # Monitoring
        self.metrics = {}  # Would integrate with actual metrics client

    async def register_collector(self, collector_name: str, dataset: str):
        """Register a data collector for monitoring."""
        key = f"{dataset}:{collector_name}"
        self.collector_health[key] = CollectorHealth(
            collector_name=collector_name,
            dataset=dataset
        )

        self.logger.log(
            LogLevel.INFO,
            f"Registered collector for monitoring: {key}",
            "collector_registered",
            collector_name=collector_name,
            dataset=dataset
        )

    async def record_collector_success(self, collector_name: str, dataset: str, latency_seconds: float):
        """Record successful collection."""
        key = f"{dataset}:{collector_name}"
        if key not in self.collector_health:
            await self.register_collector(collector_name, dataset)

        collector_health = self.collector_health[key]
        collector_health.record_success(latency_seconds)

        # Emit metrics
        await self._emit_collector_metrics(collector_health)

    async def record_collector_failure(self, collector_name: str, dataset: str, error_type: str):
        """Record failed collection."""
        key = f"{dataset}:{collector_name}"
        if key not in self.collector_health:
            await self.register_collector(collector_name, dataset)

        collector_health = self.collector_health[key]
        collector_health.record_failure(error_type)

        # Emit metrics
        await self._emit_collector_metrics(collector_health)

        # Check if remediation is needed
        if self.config.auto_remediation_enabled:
            await self._evaluate_remediation(collector_health, dataset)

    async def _evaluate_remediation(self, collector_health: CollectorHealth, dataset: str):
        """Evaluate and execute remediation if needed."""
        # Check cooldown period
        key = f"{dataset}:{collector_health.collector_name}"
        if key in self.last_remediation_times:
            cooldown_elapsed = datetime.now() - self.last_remediation_times[key]
            if cooldown_elapsed.total_seconds() < (self.config.remediation_cooldown_minutes * 60):
                return

        # Find appropriate strategy
        strategy = await self._select_strategy(collector_health)
        if strategy is None:
            return

        # Execute remediation
        # In a real implementation, this would get staleness info from the staleness monitor
        staleness_info = await self._get_staleness_info(dataset)

        result = await strategy.execute(collector_health, staleness_info)

        # Record result
        self.remediation_history.append(result)
        self.last_remediation_times[key] = datetime.now()

        # Log result
        if result.success:
            self.logger.log(
                LogLevel.INFO,
                f"Remediation successful: {strategy.get_name()} for {dataset}",
                "remediation_success",
                strategy=strategy.get_name(),
                dataset=dataset,
                collector=collector_health.collector_name,
                remediation_time_seconds=result.remediation_time_seconds
            )
        else:
            self.logger.log(
                LogLevel.WARNING,
                f"Remediation failed: {strategy.get_name()} for {dataset} - {result.error_message}",
                "remediation_failed",
                strategy=strategy.get_name(),
                dataset=dataset,
                collector=collector_health.collector_name,
                error=result.error_message
            )

    async def _select_strategy(self, collector_health: CollectorHealth) -> Optional[RemediationStrategy]:
        """Select the most appropriate remediation strategy."""
        for strategy in self.strategies:
            if strategy.can_handle(collector_health, await self._get_staleness_info(collector_health.dataset)):
                return strategy
        return None

    async def _get_staleness_info(self, dataset: str) -> DatasetStaleness:
        """Get staleness information for a dataset."""
        # This would integrate with the actual staleness monitor
        # For now, return mock data
        return DatasetStaleness(
            dataset=dataset,
            last_watermark=datetime.now() - timedelta(hours=3),
            current_time=datetime.now(),
            staleness_level=StalenessLevel.STALE,
            hours_since_update=3.0,
            warning_threshold_hours=2,
            critical_threshold_hours=6,
            grace_period_hours=1,
            consecutive_failures=2
        )

    async def _emit_collector_metrics(self, collector_health: CollectorHealth):
        """Emit collector health metrics."""
        # This would integrate with the actual metrics system
        pass

    async def get_collector_status(self, collector_name: str, dataset: str) -> Optional[CollectorHealth]:
        """Get collector health status."""
        key = f"{dataset}:{collector_name}"
        return self.collector_health.get(key)

    async def get_remediation_history(self, dataset: Optional[str] = None) -> List[RemediationActionResult]:
        """Get remediation history."""
        if dataset is None:
            return self.remediation_history

        return [r for r in self.remediation_history if r.dataset == dataset]


# Global remediation manager
_global_remediation_manager: Optional[AutoRemediationManager] = None


async def get_auto_remediation_manager() -> AutoRemediationManager:
    """Get or create the global auto-remediation manager."""
    global _global_remediation_manager

    if _global_remediation_manager is None:
        config = RemediationConfig()
        _global_remediation_manager = AutoRemediationManager(config)

    return _global_remediation_manager


async def initialize_auto_remediation():
    """Initialize the global auto-remediation system."""
    manager = await get_auto_remediation_manager()
    return manager
