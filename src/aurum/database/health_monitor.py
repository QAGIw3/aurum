"""Database connection pool health monitoring and alerting.

This module provides:
- Health checks for all database connection pools
- Performance monitoring and alerting
- Connection pool metrics collection
- Automated recovery mechanisms
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable

from aurum.database import get_connection_manager_registry, PoolMetrics, ConnectionManagerRegistry
from aurum.observability import get_application_metrics

logger = logging.getLogger(__name__)


@dataclass
class HealthCheckResult:
    """Result of a database health check."""

    pool_name: str
    is_healthy: bool
    response_time_ms: float
    error_message: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class PoolHealthConfig:
    """Configuration for database pool health monitoring."""

    check_interval_seconds: float = 30.0
    timeout_seconds: float = 10.0
    max_consecutive_failures: int = 3
    alert_threshold_utilization: float = 0.8  # 80% utilization triggers alert
    alert_threshold_response_time_ms: float = 5000.0  # 5 second response time


class DatabaseHealthMonitor:
    """Monitor health and performance of database connection pools."""

    def __init__(
        self,
        config: Optional[PoolHealthConfig] = None,
        registry: Optional[ConnectionManagerRegistry] = None
    ):
        self.config = config or PoolHealthConfig()
        self.registry = registry or get_connection_manager_registry()
        self._monitoring_task: Optional[asyncio.Task] = None
        self._is_monitoring = False
        self._consecutive_failures: Dict[str, int] = {}
        self._last_check_results: Dict[str, HealthCheckResult] = {}

        # Get application metrics for recording
        self.app_metrics = get_application_metrics()

    async def start_monitoring(self) -> None:
        """Start continuous health monitoring."""
        if self._is_monitoring:
            return

        self._is_monitoring = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info("Started database health monitoring")

    async def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        if not self._is_monitoring:
            return

        self._is_monitoring = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass

        logger.info("Stopped database health monitoring")

    async def run_health_checks(self) -> Dict[str, HealthCheckResult]:
        """Run health checks on all registered pools."""
        pools = await self.registry.get_all_pools()
        results = {}

        for pool_name, pool in pools.items():
            start_time = time.time()
            try:
                is_healthy = await pool.health_check()
                response_time_ms = (time.time() - start_time) * 1000

                result = HealthCheckResult(
                    pool_name=pool_name,
                    is_healthy=is_healthy,
                    response_time_ms=response_time_ms
                )

                # Update consecutive failure tracking
                if is_healthy:
                    self._consecutive_failures[pool_name] = 0
                else:
                    self._consecutive_failures[pool_name] = self._consecutive_failures.get(pool_name, 0) + 1

                results[pool_name] = result
                self._last_check_results[pool_name] = result

                # Record metrics
                self.app_metrics.record_db_operation("health_check", pool_name, response_time_ms / 1000)

                if not is_healthy:
                    logger.warning(f"Health check failed for pool {pool_name}")

            except Exception as e:
                response_time_ms = (time.time() - start_time) * 1000
                result = HealthCheckResult(
                    pool_name=pool_name,
                    is_healthy=False,
                    response_time_ms=response_time_ms,
                    error_message=str(e)
                )

                self._consecutive_failures[pool_name] = self._consecutive_failures.get(pool_name, 0) + 1
                results[pool_name] = result
                logger.error(f"Health check error for pool {pool_name}: {e}")

        return results

    async def get_pool_metrics(self) -> Dict[str, PoolMetrics]:
        """Get metrics for all pools."""
        return await self.registry.get_all_metrics()

    async def get_health_status(self) -> Dict[str, Dict[str, Any]]:
        """Get comprehensive health status for all pools."""
        health_results = await self.run_health_checks()
        pool_metrics = await self.get_pool_metrics()

        status = {}
        for pool_name in health_results:
            result = health_results[pool_name]
            metrics = pool_metrics.get(pool_name, PoolMetrics())

            status[pool_name] = {
                "is_healthy": result.is_healthy,
                "response_time_ms": result.response_time_ms,
                "consecutive_failures": self._consecutive_failures.get(pool_name, 0),
                "metrics": metrics.to_dict() if metrics else {},
                "last_check": result.timestamp,
            }

            # Check for alert conditions
            if not result.is_healthy and self._consecutive_failures.get(pool_name, 0) >= self.config.max_consecutive_failures:
                status[pool_name]["alert_level"] = "critical"
            elif metrics.pool_utilization >= self.config.alert_threshold_utilization:
                status[pool_name]["alert_level"] = "warning"
            elif result.response_time_ms >= self.config.alert_threshold_response_time_ms:
                status[pool_name]["alert_level"] = "warning"
            else:
                status[pool_name]["alert_level"] = "healthy"

        return status

    def get_alerting_pools(self) -> List[str]:
        """Get pools that require alerting."""
        pools_needing_alerts = []

        for pool_name, failures in self._consecutive_failures.items():
            if failures >= self.config.max_consecutive_failures:
                pools_needing_alerts.append(pool_name)

        return pools_needing_alerts

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self._is_monitoring:
            try:
                await self.run_health_checks()

                # Check for alerting conditions
                alert_pools = self.get_alerting_pools()
                if alert_pools:
                    logger.warning(f"Pools requiring attention: {alert_pools}")

            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")

            # Wait for next check
            await asyncio.sleep(self.config.check_interval_seconds)


# Global health monitor instance
_health_monitor: Optional[DatabaseHealthMonitor] = None


def get_database_health_monitor() -> DatabaseHealthMonitor:
    """Get the global database health monitor."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = DatabaseHealthMonitor()
    return _health_monitor


async def run_database_health_checks() -> Dict[str, HealthCheckResult]:
    """Convenience function to run health checks on all pools."""
    monitor = get_database_health_monitor()
    return await monitor.run_health_checks()


async def get_database_health_status() -> Dict[str, Dict[str, Any]]:
    """Convenience function to get comprehensive health status."""
    monitor = get_database_health_monitor()
    return await monitor.get_health_status()


async def start_database_monitoring() -> None:
    """Convenience function to start health monitoring."""
    monitor = get_database_health_monitor()
    await monitor.start_monitoring()


async def stop_database_monitoring() -> None:
    """Convenience function to stop health monitoring."""
    monitor = get_database_health_monitor()
    await monitor.stop_monitoring()
