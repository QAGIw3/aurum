"""
Observability and monitoring for the Advanced Configuration Management System.

This module provides:
- Metrics collection for configuration operations
- Performance monitoring and alerting
- Health checks and diagnostics
- Integration with existing observability stack
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


@dataclass
class ConfigMetrics:
    """Configuration system metrics."""
    # Configuration loading metrics
    config_load_count: int = 0
    config_load_duration_ms: float = 0.0
    config_load_errors: int = 0

    # Validation metrics
    validation_count: int = 0
    validation_duration_ms: float = 0.0
    validation_errors: int = 0

    # Change tracking metrics
    changes_recorded: int = 0
    versions_created: int = 0
    backup_operations: int = 0
    restore_operations: int = 0

    # Cache metrics
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0

    # File system metrics
    file_watch_events: int = 0
    file_read_errors: int = 0

    # Performance metrics
    avg_config_size_bytes: float = 0.0
    max_config_size_bytes: int = 0
    min_config_size_bytes: int = 0


class ConfigMetricsCollector:
    """Collects and exposes configuration system metrics."""

    def __init__(self):
        self._metrics = ConfigMetrics()
        self._operation_durations: deque = deque(maxlen=1000)
        self._config_sizes: deque = deque(maxlen=1000)
        self._lock = asyncio.Lock()

    async def record_config_load(self, duration_ms: float, success: bool, config_size: int = 0) -> None:
        """Record a configuration load operation."""
        async with self._lock:
            self._metrics.config_load_count += 1
            self._metrics.config_load_duration_ms += duration_ms
            self._operation_durations.append(duration_ms)

            if config_size > 0:
                self._config_sizes.append(config_size)
                self._metrics.avg_config_size_bytes = sum(self._config_sizes) / len(self._config_sizes)
                self._metrics.max_config_size_bytes = max(self._metrics.max_config_size_bytes, config_size)
                if self._metrics.min_config_size_bytes == 0:
                    self._metrics.min_config_size_bytes = config_size
                else:
                    self._metrics.min_config_size_bytes = min(self._metrics.min_config_size_bytes, config_size)

            if not success:
                self._metrics.config_load_errors += 1

    async def record_validation(self, duration_ms: float, success: bool) -> None:
        """Record a configuration validation operation."""
        async with self._lock:
            self._metrics.validation_count += 1
            self._metrics.validation_duration_ms += duration_ms

            if not success:
                self._metrics.validation_errors += 1

    async def record_change(self, change_type: str) -> None:
        """Record a configuration change."""
        async with self._lock:
            self._metrics.changes_recorded += 1

            if change_type == "version":
                self._metrics.versions_created += 1
            elif change_type == "backup":
                self._metrics.backup_operations += 1
            elif change_type == "restore":
                self._metrics.restore_operations += 1

    async def record_cache_operation(self, hit: bool) -> None:
        """Record a cache operation."""
        async with self._lock:
            if hit:
                self._metrics.cache_hits += 1
            else:
                self._metrics.cache_misses += 1

    async def record_file_event(self, event_type: str) -> None:
        """Record a file system event."""
        async with self._lock:
            self._metrics.file_watch_events += 1

            if event_type == "error":
                self._metrics.file_read_errors += 1

    def get_metrics(self) -> ConfigMetrics:
        """Get current metrics snapshot."""
        return ConfigMetrics(
            config_load_count=self._metrics.config_load_count,
            config_load_duration_ms=self._metrics.config_load_duration_ms,
            config_load_errors=self._metrics.config_load_errors,
            validation_count=self._metrics.validation_count,
            validation_duration_ms=self._metrics.validation_duration_ms,
            validation_errors=self._metrics.validation_errors,
            changes_recorded=self._metrics.changes_recorded,
            versions_created=self._metrics.versions_created,
            backup_operations=self._metrics.backup_operations,
            restore_operations=self._metrics.restore_operations,
            cache_hits=self._metrics.cache_hits,
            cache_misses=self._metrics.cache_misses,
            cache_evictions=self._metrics.cache_evictions,
            file_watch_events=self._metrics.file_watch_events,
            file_read_errors=self._metrics.file_read_errors,
            avg_config_size_bytes=self._metrics.avg_config_size_bytes,
            max_config_size_bytes=self._metrics.max_config_size_bytes,
            min_config_size_bytes=self._metrics.min_config_size_bytes
        )

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        durations = list(self._operation_durations)
        if not durations:
            return {"avg_duration_ms": 0, "p95_duration_ms": 0, "p99_duration_ms": 0}

        durations.sort()
        return {
            "avg_duration_ms": sum(durations) / len(durations),
            "p95_duration_ms": durations[int(len(durations) * 0.95)],
            "p99_duration_ms": durations[int(len(durations) * 0.99)],
            "min_duration_ms": min(durations),
            "max_duration_ms": max(durations)
        }


class ConfigHealthChecker:
    """Health checker for configuration system components."""

    def __init__(self):
        self._checks: Dict[str, Callable[[], bool]] = {}
        self._check_results: Dict[str, Dict[str, Any]] = {}

    def register_check(self, name: str, check_func: Callable[[], bool], description: str = "") -> None:
        """Register a health check."""
        self._checks[name] = check_func

        # Store check metadata
        self._check_results[name] = {
            "name": name,
            "description": description,
            "last_check": None,
            "status": "unknown",
            "error": None
        }

    async def run_health_checks(self) -> Dict[str, Any]:
        """Run all registered health checks."""
        results = {
            "overall_status": "healthy",
            "timestamp": time.time(),
            "checks": {}
        }

        for name, check_func in self._checks.items():
            try:
                start_time = time.time()
                is_healthy = await asyncio.get_event_loop().run_in_executor(None, check_func)
                duration = time.time() - start_time

                result = {
                    "status": "healthy" if is_healthy else "unhealthy",
                    "duration_ms": duration,
                    "timestamp": time.time()
                }

                self._check_results[name].update({
                    "last_check": time.time(),
                    "status": result["status"],
                    "error": None
                })

                results["checks"][name] = result

                if not is_healthy:
                    results["overall_status"] = "unhealthy"

            except Exception as e:
                result = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.time()
                }

                self._check_results[name].update({
                    "last_check": time.time(),
                    "status": "error",
                    "error": str(e)
                })

                results["checks"][name] = result
                results["overall_status"] = "unhealthy"

        return results

    def get_check_results(self) -> Dict[str, Dict[str, Any]]:
        """Get the latest check results."""
        return self._check_results.copy()


class ConfigObservability:
    """Main observability coordinator for the configuration system."""

    def __init__(self):
        self._metrics_collector = ConfigMetricsCollector()
        self._health_checker = ConfigHealthChecker()
        self._alerts_enabled = True
        self._alert_thresholds = {
            "config_load_errors_per_minute": 5,
            "validation_errors_per_minute": 10,
            "cache_miss_rate": 0.8,
            "avg_load_duration_ms": 1000
        }

        self._register_default_checks()
        self._start_background_tasks()

    def _register_default_checks(self) -> None:
        """Register default health checks."""

        # Configuration loading check
        def check_config_loading():
            metrics = self._metrics_collector.get_metrics()
            error_rate = (metrics.config_load_errors / max(metrics.config_load_count, 1)) * 100
            return error_rate < 10  # Less than 10% error rate

        self._health_checker.register_check(
            "config_loading",
            check_config_loading,
            "Configuration loading health"
        )

        # Validation health check
        def check_validation():
            metrics = self._metrics_collector.get_metrics()
            error_rate = (metrics.validation_errors / max(metrics.validation_count, 1)) * 100
            return error_rate < 5  # Less than 5% validation error rate

        self._health_checker.register_check(
            "validation",
            check_validation,
            "Configuration validation health"
        )

        # Cache health check
        def check_cache():
            metrics = self._metrics_collector.get_metrics()
            total_requests = metrics.cache_hits + metrics.cache_misses
            if total_requests == 0:
                return True

            hit_rate = metrics.cache_hits / total_requests
            return hit_rate > 0.7  # At least 70% cache hit rate

        self._health_checker.register_check(
            "cache",
            check_cache,
            "Configuration cache health"
        )

        # File system health check
        def check_file_system():
            metrics = self._metrics_collector.get_metrics()
            error_rate = (metrics.file_read_errors / max(metrics.file_watch_events, 1)) * 100
            return error_rate < 5  # Less than 5% file system error rate

        self._health_checker.register_check(
            "file_system",
            check_file_system,
            "Configuration file system health"
        )

    def _start_background_tasks(self) -> None:
        """Start background monitoring tasks."""
        asyncio.create_task(self._monitor_alerts())

    async def _monitor_alerts(self) -> None:
        """Monitor metrics and trigger alerts if thresholds are exceeded."""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute

                if not self._alerts_enabled:
                    continue

                metrics = self._metrics_collector.get_metrics()

                # Check configuration load errors
                if metrics.config_load_errors > self._alert_thresholds["config_load_errors_per_minute"]:
                    logger.warning(
                        f"High configuration load error rate: {metrics.config_load_errors} errors"
                    )

                # Check validation errors
                if metrics.validation_errors > self._alert_thresholds["validation_errors_per_minute"]:
                    logger.warning(
                        f"High configuration validation error rate: {metrics.validation_errors} errors"
                    )

                # Check cache performance
                total_cache_requests = metrics.cache_hits + metrics.cache_misses
                if total_cache_requests > 0:
                    cache_hit_rate = metrics.cache_hits / total_cache_requests
                    if cache_hit_rate < self._alert_thresholds["cache_miss_rate"]:
                        logger.warning(
                            f"Low cache hit rate: {cache_hit_rate".2%"} (threshold: {self._alert_thresholds['cache_miss_rate']".2%"})"
                        )

                # Check performance
                if metrics.config_load_count > 0:
                    avg_duration = metrics.config_load_duration_ms / metrics.config_load_count
                    if avg_duration > self._alert_thresholds["avg_load_duration_ms"]:
                        logger.warning(
                            f"Slow configuration loading: {avg_duration".2f"}ms average (threshold: {self._alert_thresholds['avg_load_duration_ms']}ms)"
                        )

            except Exception as e:
                logger.error(f"Error in alert monitoring: {e}")

    def get_metrics(self) -> ConfigMetrics:
        """Get current metrics."""
        return self._metrics_collector.get_metrics()

    def get_health_status(self) -> Dict[str, Any]:
        """Get current health status."""
        return asyncio.run(self._health_checker.run_health_checks())

    def get_performance_stats(self) -> Dict[str, Any]:
        """Get performance statistics."""
        return self._metrics_collector.get_performance_stats()

    def enable_alerts(self, enabled: bool = True) -> None:
        """Enable or disable alerting."""
        self._alerts_enabled = enabled

    def update_alert_thresholds(self, thresholds: Dict[str, float]) -> None:
        """Update alert thresholds."""
        self._alert_thresholds.update(thresholds)


# Global observability instance
_config_observability: Optional[ConfigObservability] = None


def get_config_observability() -> ConfigObservability:
    """Get the global configuration observability instance."""
    global _config_observability
    if _config_observability is None:
        _config_observability = ConfigObservability()
    return _config_observability


def initialize_config_observability() -> ConfigObservability:
    """Initialize the global configuration observability."""
    global _config_observability
    _config_observability = ConfigObservability()
    return _config_observability


async def record_config_load(duration_ms: float, success: bool, config_size: int = 0) -> None:
    """Record a configuration load operation."""
    observability = get_config_observability()
    await observability._metrics_collector.record_config_load(duration_ms, success, config_size)


async def record_validation(duration_ms: float, success: bool) -> None:
    """Record a configuration validation operation."""
    observability = get_config_observability()
    await observability._metrics_collector.record_validation(duration_ms, success)


async def record_change(change_type: str) -> None:
    """Record a configuration change."""
    observability = get_config_observability()
    await observability._metrics_collector.record_change(change_type)


async def record_cache_operation(hit: bool) -> None:
    """Record a cache operation."""
    observability = get_config_observability()
    await observability._metrics_collector.record_cache_operation(hit)


async def record_file_event(event_type: str) -> None:
    """Record a file system event."""
    observability = get_config_observability()
    await observability._metrics_collector.record_file_event(event_type)
