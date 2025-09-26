"""Resource monitoring service for SLO tracking and auto-scaling metrics.

This module provides comprehensive resource monitoring with burn-rate calculations
and SLO violation predictions for HPA/VPA integration.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Callable, Dict, List, Optional

import psutil

from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class SLOTarget(Enum):
    """SLO target configurations."""
    API_AVAILABILITY_99_9 = "api_availability_99.9"
    WORKER_SUCCESS_99 = "worker_success_99"
    CACHE_HIT_RATE_80 = "cache_hit_rate_80"


@dataclass
class BurnRateAlert:
    """Configuration for burn rate alerts."""

    slo_target: SLOTarget
    window_seconds: int
    burn_rate_threshold: float
    severity: str
    description: str


@dataclass
class ResourceMetrics:
    """Current resource usage metrics."""

    timestamp: datetime
    cpu_usage_percent: float
    memory_usage_percent: float
    memory_usage_mb: float
    disk_io_read_bytes: float
    disk_io_write_bytes: float
    network_io_rx_bytes: float
    network_io_tx_bytes: float

    # SLO related metrics
    error_rate: float
    request_rate: float
    response_time_p95: float
    response_time_p99: float


@dataclass
class BurnRateMetrics:
    """Burn rate calculation results."""

    slo_target: SLOTarget
    window_seconds: int
    current_burn_rate: float
    burn_rate_threshold: float
    is_violated: bool
    predicted_violation_time: Optional[datetime] = None


class ResourceMonitor:
    """Monitors system resources and calculates SLO burn rates."""

    def __init__(self):
        self.metrics = get_metrics_client()

        # Burn rate alert configurations for different SLO targets
        self.burn_rate_alerts = {
            SLOTarget.API_AVAILABILITY_99_9: [
                BurnRateAlert(
                    slo_target=SLOTarget.API_AVAILABILITY_99_9,
                    window_seconds=60,  # 1 minute
                    burn_rate_threshold=60.0,  # 60x for 99.9% over 30 days
                    severity="warning",
                    description="API availability burn rate high (1m window)"
                ),
                BurnRateAlert(
                    slo_target=SLOTarget.API_AVAILABILITY_99_9,
                    window_seconds=300,  # 5 minutes
                    burn_rate_threshold=12.0,  # 12x for 99.9% over 30 days
                    severity="warning",
                    description="API availability burn rate high (5m window)"
                ),
                BurnRateAlert(
                    slo_target=SLOTarget.API_AVAILABILITY_99_9,
                    window_seconds=1800,  # 30 minutes
                    burn_rate_threshold=2.0,  # 2x for 99.9% over 30 days
                    severity="critical",
                    description="API availability burn rate high (30m window)"
                )
            ],
            SLOTarget.WORKER_SUCCESS_99: [
                BurnRateAlert(
                    slo_target=SLOTarget.WORKER_SUCCESS_99,
                    window_seconds=60,
                    burn_rate_threshold=60.0,  # 60x for 99% over 30 days
                    severity="warning",
                    description="Worker success rate burn rate high (1m window)"
                ),
                BurnRateAlert(
                    slo_target=SLOTarget.WORKER_SUCCESS_99,
                    window_seconds=300,
                    burn_rate_threshold=12.0,  # 12x for 99% over 30 days
                    severity="warning",
                    description="Worker success rate burn rate high (5m window)"
                ),
                BurnRateAlert(
                    slo_target=SLOTarget.WORKER_SUCCESS_99,
                    window_seconds=1800,
                    burn_rate_threshold=2.0,  # 2x for 99% over 30 days
                    severity="critical",
                    description="Worker success rate burn rate high (30m window)"
                )
            ]
        }

        # Historical data for burn rate calculations
        self.error_history: Dict[str, List[float]] = {}
        self.request_history: Dict[str, List[float]] = {}
        self.history_window_seconds = 3600  # Keep 1 hour of history

        # Monitoring state
        self.is_monitoring = False
        self.monitoring_task: Optional[asyncio.Task] = None
        self.last_metrics: Optional[ResourceMetrics] = None

        # Alert callbacks
        self.alert_callbacks: List[Callable[[BurnRateMetrics], Awaitable[None]]] = []

    async def start_monitoring(self):
        """Start the resource monitoring service."""
        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Resource monitor started")

    async def stop_monitoring(self):
        """Stop the resource monitoring service."""
        if not self.is_monitoring:
            return

        self.is_monitoring = False

        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Resource monitor stopped")

    async def add_alert_callback(self, callback: Callable[[BurnRateMetrics], Awaitable[None]]):
        """Add a callback for burn rate alerts."""
        self.alert_callbacks.append(callback)

    async def get_current_metrics(self) -> ResourceMetrics:
        """Get current resource metrics."""
        return await self._collect_metrics()

    async def calculate_burn_rate(self, slo_target: SLOTarget, window_seconds: int) -> BurnRateMetrics:
        """Calculate burn rate for a specific SLO target."""

        # Get current error and request rates
        current_error_rate = await self._get_current_error_rate(slo_target)
        current_request_rate = await self._get_current_request_rate(slo_target)

        if current_request_rate == 0:
            return BurnRateMetrics(
                slo_target=slo_target,
                window_seconds=window_seconds,
                current_burn_rate=0.0,
                burn_rate_threshold=1.0,
                is_violated=False
            )

        # Calculate current burn rate
        error_rate_percent = current_error_rate / current_request_rate if current_request_rate > 0 else 0

        # Get SLO target error rate (1 - availability_percentage)
        slo_error_rate = self._get_slo_error_rate(slo_target)

        # Calculate burn rate: (current_error_rate / slo_error_rate)
        burn_rate = error_rate_percent / slo_error_rate if slo_error_rate > 0 else 0

        # Get threshold for this window
        alert_config = self._get_burn_rate_alert(slo_target, window_seconds)
        is_violated = burn_rate > alert_config.burn_rate_threshold

        # Predict when SLO will be violated if current burn rate continues
        predicted_violation_time = None
        if is_violated and burn_rate > 0:
            # Time to burn through error budget: error_budget / (current_error_rate - slo_error_rate)
            error_budget = 1.0 - slo_error_rate  # Available error budget
            excess_error_rate = error_rate_percent - slo_error_rate
            if excess_error_rate > 0:
                time_to_violation_seconds = error_budget / excess_error_rate
                predicted_violation_time = datetime.now() + timedelta(seconds=time_to_violation_seconds)

        return BurnRateMetrics(
            slo_target=slo_target,
            window_seconds=window_seconds,
            current_burn_rate=burn_rate,
            burn_rate_threshold=alert_config.burn_rate_threshold,
            is_violated=is_violated,
            predicted_violation_time=predicted_violation_time
        )

    async def _monitoring_loop(self):
        """Main monitoring loop."""
        while self.is_monitoring:
            try:
                # Collect current metrics
                metrics = await self._collect_metrics()

                # Store metrics for burn rate calculations
                await self._store_metrics(metrics)

                # Calculate and emit burn rates for all SLO targets
                await self._calculate_and_emit_burn_rates()

                # Wait before next collection
                await asyncio.sleep(30)  # Collect every 30 seconds

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retry

    async def _collect_metrics(self) -> ResourceMetrics:
        """Collect current system metrics."""
        now = datetime.now()

        # System resource metrics
        cpu_usage = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk_io = psutil.disk_io_counters()
        network_io = psutil.net_io_counters()

        # Application-specific metrics (these would be populated from actual monitoring)
        error_rate = await self._get_current_error_rate_from_metrics()
        request_rate = await self._get_current_request_rate_from_metrics()
        response_time_p95 = await self._get_response_time_p95()
        response_time_p99 = await self._get_response_time_p99()

        metrics = ResourceMetrics(
            timestamp=now,
            cpu_usage_percent=cpu_usage,
            memory_usage_percent=memory.percent,
            memory_usage_mb=memory.used / (1024 * 1024),
            disk_io_read_bytes=disk_io.read_bytes if disk_io else 0,
            disk_io_write_bytes=disk_io.write_bytes if disk_io else 0,
            network_io_rx_bytes=network_io.bytes_recv if network_io else 0,
            network_io_tx_bytes=network_io.bytes_sent if network_io else 0,
            error_rate=error_rate,
            request_rate=request_rate,
            response_time_p95=response_time_p95,
            response_time_p99=response_time_p99
        )

        self.last_metrics = metrics

        # Emit metrics
        await self._emit_metrics_to_monitoring(metrics)

        return metrics

    async def _store_metrics(self, metrics: ResourceMetrics):
        """Store metrics for burn rate calculations."""
        # This would store metrics in a time-series database or in-memory cache
        # For now, we'll just keep recent history
        pass

    async def _calculate_and_emit_burn_rates(self):
        """Calculate and emit burn rate metrics for all SLO targets."""
        for slo_target, alerts in self.burn_rate_alerts.items():
            for alert in alerts:
                try:
                    burn_rate_metrics = await self.calculate_burn_rate(
                        slo_target, alert.window_seconds
                    )

                    # Emit burn rate metrics
                    await self._emit_burn_rate_metrics(burn_rate_metrics)

                    # Check if alert should be triggered
                    if burn_rate_metrics.is_violated:
                        await self._trigger_burn_rate_alert(burn_rate_metrics, alert)

                except Exception as e:
                    logger.error(f"Error calculating burn rate for {slo_target}: {e}")

    async def _get_current_error_rate(self, slo_target: SLOTarget) -> float:
        """Get current error rate for SLO target."""
        # This would query actual monitoring systems
        # For now, return mock values
        if slo_target == SLOTarget.API_AVAILABILITY_99_9:
            return 0.001  # 0.1% error rate
        elif slo_target == SLOTarget.WORKER_SUCCESS_99:
            return 0.01   # 1% error rate
        return 0.0

    async def _get_current_request_rate(self, slo_target: SLOTarget) -> float:
        """Get current request rate for SLO target."""
        # This would query actual monitoring systems
        return 100.0  # 100 requests per second

    async def _get_slo_error_rate(self, slo_target: SLOTarget) -> float:
        """Get the target error rate for SLO."""
        if slo_target == SLOTarget.API_AVAILABILITY_99_9:
            return 0.001  # 99.9% availability = 0.1% error rate
        elif slo_target == SLOTarget.WORKER_SUCCESS_99:
            return 0.01   # 99% success rate = 1% error rate
        return 0.0

    def _get_burn_rate_alert(self, slo_target: SLOTarget, window_seconds: int) -> BurnRateAlert:
        """Get burn rate alert configuration."""
        for alert in self.burn_rate_alerts[slo_target]:
            if alert.window_seconds == window_seconds:
                return alert
        raise ValueError(f"No burn rate alert found for {slo_target} at {window_seconds}s")

    async def _emit_metrics_to_monitoring(self, metrics: ResourceMetrics):
        """Emit resource metrics to monitoring system."""
        self.metrics.gauge("resource_monitor_cpu_usage_percent", metrics.cpu_usage_percent)
        self.metrics.gauge("resource_monitor_memory_usage_percent", metrics.memory_usage_percent)
        self.metrics.gauge("resource_monitor_memory_usage_mb", metrics.memory_usage_mb)
        self.metrics.gauge("resource_monitor_error_rate", metrics.error_rate)
        self.metrics.gauge("resource_monitor_request_rate", metrics.request_rate)
        self.metrics.gauge("resource_monitor_response_time_p95", metrics.response_time_p95)
        self.metrics.gauge("resource_monitor_response_time_p99", metrics.response_time_p99)

    async def _emit_burn_rate_metrics(self, burn_rate_metrics: BurnRateMetrics):
        """Emit burn rate metrics."""
        metric_name = f"burn_rate_{burn_rate_metrics.slo_target.value}_{burn_rate_metrics.window_seconds}s"
        self.metrics.gauge(metric_name, burn_rate_metrics.current_burn_rate)

        if burn_rate_metrics.is_violated:
            self.metrics.increment_counter("burn_rate_violations_total")

    async def _trigger_burn_rate_alert(self, burn_rate_metrics: BurnRateMetrics, alert: BurnRateAlert):
        """Trigger burn rate alert."""
        logger.warning(
            f"Burn rate alert triggered: {alert.description} "
            f"(current: {burn_rate_metrics.current_burn_rate:.2f}x, "
            f"threshold: {alert.burn_rate_threshold:.2f}x)"
        )

        # Call alert callbacks
        for callback in self.alert_callbacks:
            try:
                await callback(burn_rate_metrics)
            except Exception as e:
                logger.error(f"Error in burn rate alert callback: {e}")

    # Placeholder methods - these would integrate with actual monitoring systems
    async def _get_current_error_rate_from_metrics(self) -> float:
        """Get current error rate from monitoring system."""
        return self.last_metrics.error_rate if self.last_metrics else 0.0

    async def _get_current_request_rate_from_metrics(self) -> float:
        """Get current request rate from monitoring system."""
        return self.last_metrics.request_rate if self.last_metrics else 0.0

    async def _get_response_time_p95(self) -> float:
        """Get p95 response time from monitoring system."""
        return self.last_metrics.response_time_p95 if self.last_metrics else 0.0

    async def _get_response_time_p99(self) -> float:
        """Get p99 response time from monitoring system."""
        return self.last_metrics.response_time_p99 if self.last_metrics else 0.0


# Global resource monitor instance
_global_resource_monitor: Optional[ResourceMonitor] = None


async def get_resource_monitor() -> ResourceMonitor:
    """Get or create the global resource monitor."""
    global _global_resource_monitor

    if _global_resource_monitor is None:
        _global_resource_monitor = ResourceMonitor()
        await _global_resource_monitor.start_monitoring()

    return _global_resource_monitor


async def initialize_resource_monitoring():
    """Initialize the global resource monitoring service."""
    monitor = await get_resource_monitor()
    return monitor
