"""Observability utilities for the Aurum API.

This module provides centralized observability configuration, dashboards,
and monitoring utilities for the Aurum energy trading platform.
"""

from __future__ import annotations

from typing import Dict, List, Any
from dataclasses import dataclass


@dataclass
class ObservabilityConfig:
    """Configuration for observability features."""

    enable_metrics: bool = True
    enable_tracing: bool = True
    enable_logging: bool = True
    metrics_prefix: str = "aurum"
    log_level: str = "INFO"
    sampling_rate: float = 0.1  # 10% sampling for traces

    # Dashboard configurations
    dashboards: Dict[str, Any] = None

    def __post_init__(self):
        if self.dashboards is None:
            self.dashboards = {
                "api_performance": self._get_api_performance_dashboard(),
                "database_health": self._get_database_health_dashboard(),
                "cache_performance": self._get_cache_performance_dashboard(),
            }

    def _get_api_performance_dashboard(self) -> Dict[str, Any]:
        """Get API performance monitoring dashboard configuration."""
        return {
            "title": "API Performance Dashboard",
            "description": "Monitor API endpoint performance and errors",
            "panels": [
                {
                    "title": "Request Rate",
                    "type": "counter",
                    "metric": "aurum_http_requests_total",
                    "description": "Total HTTP requests by endpoint and status code",
                },
                {
                    "title": "Request Duration",
                    "type": "histogram",
                    "metric": "aurum_http_request_duration_seconds",
                    "description": "Request duration percentiles",
                },
                {
                    "title": "Error Rate",
                    "type": "counter",
                    "metric": "aurum_http_errors_total",
                    "description": "HTTP errors by status code",
                },
            ],
        }

    def _get_database_health_dashboard(self) -> Dict[str, Any]:
        """Get database health monitoring dashboard configuration."""
        return {
            "title": "Database Health Dashboard",
            "description": "Monitor database performance and connection health",
            "panels": [
                {
                    "title": "Query Duration",
                    "type": "histogram",
                    "metric": "aurum_db_query_duration_seconds",
                    "description": "Database query duration percentiles",
                },
                {
                    "title": "Connection Pool",
                    "type": "gauge",
                    "metric": "aurum_db_connections",
                    "description": "Database connection pool utilization",
                },
                {
                    "title": "Slow Queries",
                    "type": "counter",
                    "metric": "aurum_db_slow_queries_total",
                    "description": "Number of slow queries (>1s)",
                },
            ],
        }

    def _get_cache_performance_dashboard(self) -> Dict[str, Any]:
        """Get cache performance monitoring dashboard configuration."""
        return {
            "title": "Cache Performance Dashboard",
            "description": "Monitor cache hit rates and performance",
            "panels": [
                {
                    "title": "Cache Hit Rate",
                    "type": "gauge",
                    "metric": "aurum_cache_hit_rate",
                    "description": "Cache hit rate percentage",
                },
                {
                    "title": "Cache Operations",
                    "type": "counter",
                    "metric": "aurum_cache_operations_total",
                    "description": "Cache operations by type (get/set/invalidate)",
                },
                {
                    "title": "Cache Memory Usage",
                    "type": "gauge",
                    "metric": "aurum_cache_memory_bytes",
                    "description": "Cache memory usage in bytes",
                },
            ],
        }


def get_observability_config() -> ObservabilityConfig:
    """Get the current observability configuration."""
    return ObservabilityConfig()


def configure_observability(config: ObservabilityConfig) -> None:
    """Configure observability features based on the provided config.

    Args:
        config: Observability configuration to apply
    """
    # This would configure logging levels, metrics collection, tracing, etc.
    # For now, this is a placeholder for the configuration logic
    pass


__all__ = [
    "ObservabilityConfig",
    "get_observability_config",
    "configure_observability",
]
