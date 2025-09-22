"""Health monitoring system for canary deployments.

This module provides comprehensive health monitoring capabilities for canary deployments,
including endpoint health checks, performance monitoring, and automated decision-making.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Awaitable

from ...observability.metrics import get_metrics_client

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Overall health status of a deployment."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class HealthMetric:
    """A single health metric measurement."""

    name: str
    value: float
    timestamp: datetime = field(default_factory=lambda: datetime.now())
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckResult:
    """Result of a health check."""

    check_name: str
    status: str  # "pass", "fail", "warn"
    response_time_ms: float
    timestamp: datetime = field(default_factory=lambda: datetime.now())
    error_message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentHealthReport:
    """Comprehensive health report for a deployment."""

    deployment_name: str
    timestamp: datetime = field(default_factory=lambda: datetime.now())

    # Overall status
    overall_status: HealthStatus = HealthStatus.UNKNOWN
    confidence_score: float = 0.0  # 0.0 to 1.0

    # Component health
    endpoint_health: Dict[str, HealthStatus] = field(default_factory=dict)
    database_health: HealthStatus = HealthStatus.UNKNOWN
    kafka_health: HealthStatus = HealthStatus.UNKNOWN
    api_health: HealthStatus = HealthStatus.UNKNOWN

    # Performance metrics
    avg_latency_ms: float = 0.0
    p95_latency_ms: float = 0.0
    p99_latency_ms: float = 0.0
    error_rate_percent: float = 0.0
    throughput_rps: float = 0.0

    # Health check results
    health_checks: List[HealthCheckResult] = field(default_factory=list)
    failed_checks: List[str] = field(default_factory=list)

    # Alerts
    alerts: List[Dict[str, Any]] = field(default_factory=list)

    # Metadata
    version: str = ""
    environment: str = "production"


class HealthCheckProvider:
    """Base class for health check providers."""

    def __init__(self, name: str):
        self.name = name
        self.metrics = get_metrics_client()

    async def check_health(self, target: str, **kwargs) -> HealthCheckResult:
        """Perform health check on target."""
        start_time = datetime.now()

        try:
            result = await self._perform_check(target, **kwargs)
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds() * 1000

            return HealthCheckResult(
                check_name=f"{self.name}_{target}",
                status=result["status"],
                response_time_ms=response_time,
                error_message=result.get("error"),
                details=result.get("details", {})
            )

        except Exception as e:
            end_time = datetime.now()
            response_time = (end_time - start_time).total_seconds() * 1000

            logger.error(f"Health check failed for {target}: {e}")
            return HealthCheckResult(
                check_name=f"{self.name}_{target}",
                status="fail",
                response_time_ms=response_time,
                error_message=str(e)
            )

    @abstractmethod
    async def _perform_check(self, target: str, **kwargs) -> Dict[str, Any]:
        """Perform the actual health check."""
        pass

    def get_provider_name(self) -> str:
        """Get the provider name."""
        return self.name


class HttpHealthCheckProvider(HealthCheckProvider):
    """HTTP-based health check provider."""

    def __init__(self):
        super().__init__("http")

    async def _perform_check(self, target: str, **kwargs) -> Dict[str, Any]:
        """Perform HTTP health check."""
        # Mock implementation - in production would use aiohttp
        timeout = kwargs.get("timeout", 10)
        expected_codes = kwargs.get("expected_codes", [200, 201, 202])

        # Simulate HTTP request
        await asyncio.sleep(0.1)  # Simulate network latency

        # Mock response
        status_code = 200  # Mock successful response

        if status_code in expected_codes:
            return {
                "status": "pass",
                "details": {"status_code": status_code, "response_time": 100}
            }
        else:
            return {
                "status": "fail",
                "error": f"Unexpected status code: {status_code}",
                "details": {"status_code": status_code}
            }


class DatabaseHealthCheckProvider(HealthCheckProvider):
    """Database health check provider."""

    def __init__(self):
        super().__init__("database")

    async def _perform_check(self, target: str, **kwargs) -> Dict[str, Any]:
        """Perform database health check."""
        # Mock implementation
        await asyncio.sleep(0.05)  # Simulate database query

        # Mock successful connection
        return {
            "status": "pass",
            "details": {"connection_time": 50, "query_time": 25}
        }


class KafkaHealthCheckProvider(HealthCheckProvider):
    """Kafka health check provider."""

    def __init__(self):
        super().__init__("kafka")

    async def _perform_check(self, target: str, **kwargs) -> Dict[str, Any]:
        """Perform Kafka health check."""
        # Mock implementation
        await asyncio.sleep(0.02)  # Simulate Kafka operation

        # Mock successful connection
        return {
            "status": "pass",
            "details": {"bootstrap_time": 20, "metadata_fetch_time": 15}
        }


class MetricsHealthCheckProvider(HealthCheckProvider):
    """Metrics-based health check provider."""

    def __init__(self):
        super().__init__("metrics")

    async def _perform_check(self, target: str, **kwargs) -> Dict[str, Any]:
        """Perform metrics-based health check."""
        # Mock implementation - would check actual metrics
        await asyncio.sleep(0.01)

        # Mock healthy metrics
        return {
            "status": "pass",
            "details": {
                "error_rate": 0.1,
                "avg_latency": 150,
                "throughput": 1000
            }
        }


class HealthMonitor:
    """Main health monitoring system for canary deployments."""

    def __init__(self):
        self.health_check_providers: Dict[str, HealthCheckProvider] = {
            "http": HttpHealthCheckProvider(),
            "database": DatabaseHealthCheckProvider(),
            "kafka": KafkaHealthCheckProvider(),
            "metrics": MetricsHealthCheckProvider()
        }

        # Configuration
        self.check_interval_seconds = 30
        self.degraded_threshold_percent = 5.0
        self.critical_threshold_percent = 20.0
        self.min_healthy_checks_required = 3

        # State
        self.monitored_deployments: Dict[str, Dict[str, Any]] = {}
        self.health_history: Dict[str, List[DeploymentHealthReport]] = {}
        self.alert_callbacks: List[Callable[[Dict[str, Any]], Awaitable[None]]] = []

        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._alerting_task: Optional[asyncio.Task] = None
        self._is_running = False

        # Metrics
        self.metrics = get_metrics_client()

    async def start(self):
        """Start the health monitor."""
        if self._is_running:
            return

        self._is_running = True

        # Start background tasks
        self._monitoring_task = asyncio.create_task(self._continuous_monitoring())
        self._alerting_task = asyncio.create_task(self._process_alerts())

        logger.info("Health monitor started")

    async def stop(self):
        """Stop the health monitor."""
        self._is_running = False

        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        if self._alerting_task:
            self._alerting_task.cancel()
            try:
                await self._alerting_task
            except asyncio.CancelledError:
                pass

        logger.info("Health monitor stopped")

    async def add_deployment_monitoring(
        self,
        deployment_name: str,
        endpoints: Dict[str, str],
        health_checks: List[Dict[str, Any]]
    ):
        """Add a deployment to be monitored."""
        self.monitored_deployments[deployment_name] = {
            "endpoints": endpoints,
            "health_checks": health_checks,
            "last_check": None,
            "consecutive_failures": 0,
            "alerts_sent": 0
        }

        logger.info(f"Added monitoring for deployment: {deployment_name}")

    async def remove_deployment_monitoring(self, deployment_name: str):
        """Remove deployment monitoring."""
        if deployment_name in self.monitored_deployments:
            del self.monitored_deployments[deployment_name]

        if deployment_name in self.health_history:
            del self.health_history[deployment_name]

        logger.info(f"Removed monitoring for deployment: {deployment_name}")

    async def get_deployment_health(self, deployment_name: str) -> Optional[DeploymentHealthReport]:
        """Get the current health status of a deployment."""
        if deployment_name not in self.monitored_deployments:
            return None

        # Perform fresh health assessment
        report = await self._assess_deployment_health(deployment_name)

        # Store in history
        if deployment_name not in self.health_history:
            self.health_history[deployment_name] = []

        self.health_history[deployment_name].append(report)

        # Keep only last 100 reports
        if len(self.health_history[deployment_name]) > 100:
            self.health_history[deployment_name] = self.health_history[deployment_name][-100:]

        return report

    async def _assess_deployment_health(self, deployment_name: str) -> DeploymentHealthReport:
        """Assess the health of a deployment."""
        deployment_info = self.monitored_deployments[deployment_name]
        endpoints = deployment_info["endpoints"]
        health_checks = deployment_info["health_checks"]

        # Perform all health checks
        health_checks_results = []
        endpoint_health = {}

        for endpoint_name, endpoint_url in endpoints.items():
            # Determine provider type from endpoint name or configuration
            provider_name = "http"  # Default to HTTP
            if "database" in endpoint_name.lower():
                provider_name = "database"
            elif "kafka" in endpoint_name.lower():
                provider_name = "kafka"

            provider = self.health_check_providers.get(provider_name)
            if provider:
                result = await provider.check_health(endpoint_url, timeout=10)
                health_checks_results.append(result)

                # Map result to health status
                if result.status == "pass":
                    endpoint_health[endpoint_name] = HealthStatus.HEALTHY
                elif result.status == "warn":
                    endpoint_health[endpoint_name] = HealthStatus.DEGRADED
                else:
                    endpoint_health[endpoint_name] = HealthStatus.CRITICAL

        # Calculate overall health
        failed_checks = [r for r in health_checks_results if r.status == "fail"]
        warned_checks = [r for r in health_checks_results if r.status == "warn"]

        failed_count = len(failed_checks)
        warned_count = len(warned_checks)
        total_checks = len(health_checks_results)

        # Determine overall status
        if failed_count == 0 and warned_count == 0:
            overall_status = HealthStatus.HEALTHY
            confidence_score = 1.0
        elif failed_count > 0:
            if failed_count > total_checks * 0.5:  # More than 50% failed
                overall_status = HealthStatus.CRITICAL
            else:
                overall_status = HealthStatus.DEGRADED
            confidence_score = 1.0 - (failed_count / total_checks)
        elif warned_count > 0:
            overall_status = HealthStatus.DEGRADED
            confidence_score = 0.8
        else:
            overall_status = HealthStatus.UNKNOWN
            confidence_score = 0.5

        # Calculate performance metrics (mock values)
        avg_latency = sum(r.response_time_ms for r in health_checks_results) / max(len(health_checks_results), 1)
        error_rate = (failed_count / total_checks) * 100 if total_checks > 0 else 0

        # Create health report
        report = DeploymentHealthReport(
            deployment_name=deployment_name,
            overall_status=overall_status,
            confidence_score=confidence_score,
            endpoint_health=endpoint_health,
            database_health=endpoint_health.get("database", HealthStatus.UNKNOWN),
            kafka_health=endpoint_health.get("kafka", HealthStatus.UNKNOWN),
            api_health=endpoint_health.get("api", HealthStatus.UNKNOWN),
            avg_latency_ms=avg_latency,
            error_rate_percent=error_rate,
            health_checks=health_checks_results,
            failed_checks=[r.check_name for r in failed_checks]
        )

        # Generate alerts
        await self._generate_alerts(report)

        # Emit metrics
        await self._emit_health_metrics(report)

        return report

    async def _generate_alerts(self, report: DeploymentHealthReport):
        """Generate alerts based on health report."""
        alerts = []

        # Overall status alerts
        if report.overall_status in [HealthStatus.CRITICAL, HealthStatus.DEGRADED]:
            severity = AlertSeverity.CRITICAL if report.overall_status == HealthStatus.CRITICAL else AlertSeverity.WARNING

            alerts.append({
                "deployment": report.deployment_name,
                "severity": severity.value,
                "type": "overall_health",
                "message": f"Deployment health is {report.overall_status.value}",
                "details": {
                    "confidence_score": report.confidence_score,
                    "failed_checks": report.failed_checks
                },
                "timestamp": report.timestamp.isoformat()
            })

        # Individual endpoint alerts
        for endpoint_name, status in report.endpoint_health.items():
            if status in [HealthStatus.CRITICAL, HealthStatus.DEGRADED]:
                severity = AlertSeverity.CRITICAL if status == HealthStatus.CRITICAL else AlertSeverity.WARNING

                alerts.append({
                    "deployment": report.deployment_name,
                    "severity": severity.value,
                    "type": "endpoint_health",
                    "message": f"Endpoint {endpoint_name} is {status.value}",
                    "details": {"endpoint": endpoint_name, "status": status.value},
                    "timestamp": report.timestamp.isoformat()
                })

        # Performance alerts
        if report.error_rate_percent > 5.0:
            alerts.append({
                "deployment": report.deployment_name,
                "severity": AlertSeverity.ERROR.value,
                "type": "error_rate",
                "message": f"Error rate is {report.error_rate_percent".1f"}%",
                "details": {"error_rate": report.error_rate_percent},
                "timestamp": report.timestamp.isoformat()
            })

        if report.avg_latency_ms > 2000:  # 2 second threshold
            alerts.append({
                "deployment": report.deployment_name,
                "severity": AlertSeverity.WARNING.value,
                "type": "latency",
                "message": f"Average latency is {report.avg_latency_ms".1f"}ms",
                "details": {"avg_latency": report.avg_latency_ms},
                "timestamp": report.timestamp.isoformat()
            })

        # Send alerts to callbacks
        for alert in alerts:
            report.alerts.append(alert)

            # Call alert callbacks
            for callback in self.alert_callbacks:
                try:
                    await callback(alert)
                except Exception as e:
                    logger.error(f"Alert callback failed: {e}")

    async def _emit_health_metrics(self, report: DeploymentHealthReport):
        """Emit health metrics to monitoring system."""
        # Overall health score
        health_score = {
            HealthStatus.HEALTHY: 1.0,
            HealthStatus.DEGRADED: 0.5,
            HealthStatus.CRITICAL: 0.0,
            HealthStatus.UNKNOWN: 0.5
        }.get(report.overall_status, 0.5)

        self.metrics.gauge(
            f"canary.health_score.{report.deployment_name}",
            health_score
        )

        # Component health
        for component, status in [
            ("endpoints", report.api_health),
            ("database", report.database_health),
            ("kafka", report.kafka_health)
        ]:
            status_score = {
                HealthStatus.HEALTHY: 1.0,
                HealthStatus.DEGRADED: 0.5,
                HealthStatus.CRITICAL: 0.0,
                HealthStatus.UNKNOWN: 0.5
            }.get(status, 0.5)

            self.metrics.gauge(
                f"canary.{component}_health.{report.deployment_name}",
                status_score
            )

        # Performance metrics
        self.metrics.gauge(
            f"canary.avg_latency.{report.deployment_name}",
            report.avg_latency_ms
        )

        self.metrics.gauge(
            f"canary.error_rate.{report.deployment_name}",
            report.error_rate_percent
        )

        # Health check counts
        self.metrics.gauge(
            f"canary.health_checks_total.{report.deployment_name}",
            len(report.health_checks)
        )

        self.metrics.gauge(
            f"canary.health_checks_failed.{report.deployment_name}",
            len(report.failed_checks)
        )

    async def add_alert_callback(self, callback: Callable[[Dict[str, Any]], Awaitable[None]]):
        """Add an alert callback function."""
        self.alert_callbacks.append(callback)

    async def _continuous_monitoring(self):
        """Background task for continuous health monitoring."""
        while self._is_running:
            try:
                await asyncio.sleep(self.check_interval_seconds)

                # Monitor all deployments
                for deployment_name in list(self.monitored_deployments.keys()):
                    try:
                        report = await self.get_deployment_health(deployment_name)

                        # Log significant health changes
                        if report and report.overall_status in [HealthStatus.CRITICAL, HealthStatus.DEGRADED]:
                            logger.warning(
                                f"Deployment {deployment_name} health degraded: {report.overall_status.value}"
                            )

                    except Exception as e:
                        logger.error(f"Error monitoring deployment {deployment_name}: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {e}")

    async def _process_alerts(self):
        """Background task for processing alerts."""
        while self._is_running:
            try:
                await asyncio.sleep(60)  # Process alerts every minute

                # Aggregate and correlate alerts
                # This would implement alert correlation, deduplication, etc.

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in alert processing: {e}")


# Global health monitor instance
_global_health_monitor: Optional[HealthMonitor] = None


async def get_health_monitor() -> HealthMonitor:
    """Get or create the global health monitor."""
    global _global_health_monitor

    if _global_health_monitor is None:
        _global_health_monitor = HealthMonitor()
        await _global_health_monitor.start()

    return _global_health_monitor


# Convenience functions for common health monitoring operations
async def monitor_caiso_deployment(
    deployment_name: str,
    version: str
) -> None:
    """Set up health monitoring for CAISO deployment."""
    monitor = await get_health_monitor()

    endpoints = {
        "caiso_api": "https://oasis.caiso.com/oasisapi/SingleZip",
        "caiso_database": "postgresql://caiso-prod:5432/aurum",
        "caiso_kafka": "kafka://caiso-kafka:9092"
    }

    health_checks = [
        {
            "name": "caiso_api_connectivity",
            "provider": "http",
            "target": "https://oasis.caiso.com/oasisapi/SingleZip",
            "expected_codes": [200, 201, 202]
        },
        {
            "name": "caiso_data_freshness",
            "provider": "http",
            "target": "https://internal.caiso-health.com/freshness"
        }
    ]

    await monitor.add_deployment_monitoring(
        deployment_name,
        endpoints,
        health_checks
    )


async def get_deployment_health_status(deployment_name: str) -> Optional[DeploymentHealthReport]:
    """Get health status for a deployment."""
    monitor = await get_health_monitor()
    return await monitor.get_deployment_health(deployment_name)


async def add_health_alert_callback(callback: Callable[[Dict[str, Any]], Awaitable[None]]):
    """Add a callback for health alerts."""
    monitor = await get_health_monitor()
    await monitor.add_alert_callback(callback)
