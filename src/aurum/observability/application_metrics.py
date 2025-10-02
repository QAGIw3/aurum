"""Application-specific metrics for monitoring Aurum platform performance.

This module provides metrics for:
- API request/response tracking
- Database operation metrics
- External data ingestion metrics
- Cache performance metrics
- Business operation metrics
"""

from __future__ import annotations

from typing import Dict, Optional

from .core_metrics import get_metrics_collector, MetricsCollector


class ApplicationMetrics:
    """Application-specific metrics collector."""

    def __init__(self, collector: Optional[MetricsCollector] = None) -> None:
        self.collector = collector or get_metrics_collector()

        # API metrics
        self.api_request_counter = self.collector.counter(
            "aurum_api_requests_total",
            "Total API requests",
            labels=["method", "endpoint", "status"]
        )

        self.api_request_duration = self.collector.histogram(
            "aurum_api_request_duration_seconds",
            "API request duration in seconds",
            labels=["method", "endpoint"],
            buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0)
        )

        # Database metrics
        self.db_operation_duration = self.collector.histogram(
            "aurum_db_operation_duration_seconds",
            "Database operation duration in seconds",
            labels=["operation", "database"],
            buckets=(0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0)
        )

        self.db_connection_pool_size = self.collector.gauge(
            "aurum_db_connection_pool_size",
            "Database connection pool size",
            labels=["database"]
        )

        # External data metrics
        self.external_api_requests = self.collector.counter(
            "aurum_external_api_requests_total",
            "Total external API requests",
            labels=["provider", "endpoint", "status"]
        )

        self.external_data_ingestion = self.collector.counter(
            "aurum_external_data_ingested_total",
            "Total external data records ingested",
            labels=["provider", "dataset"]
        )

        # Cache metrics
        self.cache_hits = self.collector.counter(
            "aurum_cache_hits_total",
            "Total cache hits",
            labels=["cache_type"]
        )

        self.cache_misses = self.collector.counter(
            "aurum_cache_misses_total",
            "Total cache misses",
            labels=["cache_type"]
        )

        self.cache_hit_ratio = self.collector.gauge(
            "aurum_cache_hit_ratio",
            "Cache hit ratio",
            labels=["cache_type"]
        )

    def record_api_request(
        self,
        method: str,
        endpoint: str,
        status: str,
        duration_seconds: float
    ) -> None:
        """Record API request metrics."""
        self.api_request_counter.labels(method, endpoint, status).inc()
        self.api_request_duration.labels(method, endpoint).observe(duration_seconds)

    def record_db_operation(
        self,
        operation: str,
        database: str,
        duration_seconds: float
    ) -> None:
        """Record database operation metrics."""
        self.db_operation_duration.labels(operation, database).observe(duration_seconds)

    def update_db_connection_pool_size(self, database: str, size: int) -> None:
        """Update database connection pool size gauge."""
        self.db_connection_pool_size.labels(database).set(size)

    def record_external_api_request(
        self,
        provider: str,
        endpoint: str,
        status: str
    ) -> None:
        """Record external API request metrics."""
        self.external_api_requests.labels(provider, endpoint, status).inc()

    def record_external_data_ingestion(
        self,
        provider: str,
        dataset: str,
        record_count: int
    ) -> None:
        """Record external data ingestion metrics."""
        self.external_data_ingestion.labels(provider, dataset).inc(record_count)

    def record_cache_hit(self, cache_type: str) -> None:
        """Record cache hit."""
        self.cache_hits.labels(cache_type).inc()

    def record_cache_miss(self, cache_type: str) -> None:
        """Record cache miss."""
        self.cache_misses.labels(cache_type).inc()

    def update_cache_hit_ratio(self, cache_type: str, ratio: float) -> None:
        """Update cache hit ratio gauge."""
        self.cache_hit_ratio.labels(cache_type).set(ratio)


# Global application metrics instance
_app_metrics = None


def get_application_metrics() -> ApplicationMetrics:
    """Get the global application metrics instance."""
    global _app_metrics
    if _app_metrics is None:
        _app_metrics = ApplicationMetrics()
    return _app_metrics


def record_external_api_request(endpoint: str, status: str, duration_seconds: float) -> None:
    """Record external API request metrics."""
    metrics = get_application_metrics()
    # Extract provider from endpoint (simplified logic)
    provider = endpoint.split('/')[1] if '/' in endpoint else 'unknown'
    metrics.record_external_api_request(provider, endpoint, status)


def record_external_contract_publish(provider: str, status: str) -> None:
    """Record external contract publish metrics."""
    metrics = get_application_metrics()
    metrics.record_external_api_request(provider, "publish", status)


def record_external_contract_merge(provider: str, target: str, records: int) -> None:
    """Record external contract merge metrics."""
    metrics = get_application_metrics()
    metrics.record_external_data_ingestion(provider, target, records)
