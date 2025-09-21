"""Comprehensive metrics collection system for the Aurum platform."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ..telemetry.context import get_request_id


class MetricType(Enum):
    """Types of metrics that can be collected."""
    COUNTER = "counter"          # Monotonically increasing value
    GAUGE = "gauge"              # Point-in-time value
    HISTOGRAM = "histogram"      # Distribution of values
    SUMMARY = "summary"          # Quantiles of observed values


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: Union[int, float]
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)
    metric_type: MetricType = MetricType.GAUGE


@dataclass
class HistogramBucket:
    """Histogram bucket configuration."""
    le: float  # Less than or equal
    count: int = 0


@dataclass
class HistogramMetric:
    """Histogram metric data."""
    name: str
    sum: float = 0.0
    count: int = 0
    buckets: List[HistogramBucket] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


class MetricsCollector:
    """Core metrics collection and aggregation system."""

    def __init__(self):
        self._counters: Dict[str, Dict[str, int]] = {}
        self._gauges: Dict[str, Dict[str, Union[int, float]]] = {}
        self._histograms: Dict[str, Dict[str, HistogramMetric]] = {}
        self._summaries: Dict[str, Dict[str, List[float]]] = {}
        self._lock = asyncio.Lock()

    async def increment_counter(
        self,
        name: str,
        value: int = 1,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Increment a counter metric."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            if name not in self._counters:
                self._counters[name] = {}
            self._counters[name][label_key] = (
                self._counters[name].get(label_key, 0) + value
            )

    async def set_gauge(
        self,
        name: str,
        value: Union[int, float],
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Set a gauge metric value."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            if name not in self._gauges:
                self._gauges[name] = {}
            self._gauges[name][label_key] = value

    async def observe_histogram(
        self,
        name: str,
        value: float,
        buckets: List[float] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Observe a value in a histogram."""
        if buckets is None:
            buckets = [0.1, 0.5, 1.0, 2.0, 5.0, 10.0, float('inf')]

        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            if name not in self._histograms:
                self._histograms[name] = {}

            if label_key not in self._histograms[name]:
                self._histograms[name][label_key] = HistogramMetric(
                    name=name,
                    buckets=[HistogramBucket(le=bucket) for bucket in buckets],
                    labels=labels.copy()
                )

            histogram = self._histograms[name][label_key]
            histogram.sum += value
            histogram.count += 1

            # Update buckets
            for bucket in histogram.buckets:
                if value <= bucket.le:
                    bucket.count += 1

    async def observe_summary(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Observe a value in a summary."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            if name not in self._summaries:
                self._summaries[name] = {}
            if label_key not in self._summaries[name]:
                self._summaries[name][label_key] = []

            values = self._summaries[name][label_key]
            values.append(value)

            # Keep only last 1000 values to prevent memory growth
            if len(values) > 1000:
                values.pop(0)

    def _make_label_key(self, labels: Dict[str, str]) -> str:
        """Create a consistent key from label dictionary."""
        if not labels:
            return ""

        # Sort labels for consistent ordering
        sorted_items = sorted(labels.items())
        return ",".join(f"{k}={v}" for k, v in sorted_items)

    async def get_counter(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None
    ) -> Optional[int]:
        """Get current value of a counter."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            return self._counters.get(name, {}).get(label_key)

    async def get_gauge(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None
    ) -> Optional[Union[int, float]]:
        """Get current value of a gauge."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            return self._gauges.get(name, {}).get(label_key)

    async def get_histogram(
        self,
        name: str,
        labels: Optional[Dict[str, str]] = None
    ) -> Optional[HistogramMetric]:
        """Get histogram data."""
        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            return self._histograms.get(name, {}).get(label_key)

    async def get_summary_quantiles(
        self,
        name: str,
        quantiles: List[float] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> Dict[float, float]:
        """Get quantiles from summary observations."""
        if quantiles is None:
            quantiles = [0.5, 0.9, 0.95, 0.99]

        labels = labels or {}
        label_key = self._make_label_key(labels)

        async with self._lock:
            values = self._summaries.get(name, {}).get(label_key, [])
            if not values:
                return {}

            sorted_values = sorted(values)
            result = {}
            for q in quantiles:
                if 0 <= q <= 1:
                    index = int(q * (len(sorted_values) - 1))
                    result[q] = sorted_values[index]

            return result

    async def collect_metrics(self) -> List[MetricPoint]:
        """Collect all metrics in Prometheus format."""
        metrics = []
        timestamp = time.time()

        # Collect counters
        for name, label_values in self._counters.items():
            for label_key, value in label_values.items():
                labels = self._parse_label_key(label_key)
                metrics.append(MetricPoint(
                    name=name,
                    value=value,
                    timestamp=timestamp,
                    labels=labels,
                    metric_type=MetricType.COUNTER
                ))

        # Collect gauges
        for name, label_values in self._gauges.items():
            for label_key, value in label_values.items():
                labels = self._parse_label_key(label_key)
                metrics.append(MetricPoint(
                    name=name,
                    value=value,
                    timestamp=timestamp,
                    labels=labels,
                    metric_type=MetricType.GAUGE
                ))

        # Collect histograms
        for name, label_values in self._histograms.items():
            for label_key, histogram in label_values.items():
                labels = self._parse_label_key(label_key)

                # Add count metric
                metrics.append(MetricPoint(
                    name=f"{name}_count",
                    value=histogram.count,
                    timestamp=timestamp,
                    labels=labels,
                    metric_type=MetricType.COUNTER
                ))

                # Add sum metric
                metrics.append(MetricPoint(
                    name=f"{name}_sum",
                    value=histogram.sum,
                    timestamp=timestamp,
                    labels=labels,
                    metric_type=MetricType.COUNTER
                ))

                # Add bucket metrics
                for bucket in histogram.buckets:
                    bucket_labels = labels.copy()
                    bucket_labels["le"] = str(bucket.le)
                    metrics.append(MetricPoint(
                        name=f"{name}_bucket",
                        value=bucket.count,
                        timestamp=timestamp,
                        labels=bucket_labels,
                        metric_type=MetricType.COUNTER
                    ))

        return metrics

    def _parse_label_key(self, label_key: str) -> Dict[str, str]:
        """Parse label key back into dictionary."""
        if not label_key:
            return {}

        labels = {}
        for part in label_key.split(","):
            if "=" in part:
                key, value = part.split("=", 1)
                labels[key] = value
        return labels

    async def reset_metrics(self) -> None:
        """Reset all metrics (for testing)."""
        async with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._histograms.clear()
            self._summaries.clear()


# Global metrics collector
_metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    return _metrics_collector


# Convenience functions for common metrics
async def increment_api_requests(
    endpoint: str,
    method: str = "GET",
    status_code: int = 200
) -> None:
    """Increment API request counter."""
    labels = {
        "endpoint": endpoint,
        "method": method,
        "status_code": str(status_code)
    }
    await _metrics_collector.increment_counter(
        "aurum_api_requests_total",
        labels=labels
    )


async def observe_api_latency(
    endpoint: str,
    method: str,
    duration_seconds: float
) -> None:
    """Observe API request latency."""
    labels = {"endpoint": endpoint, "method": method}
    await _metrics_collector.observe_histogram(
        "aurum_api_request_duration_seconds",
        duration_seconds,
        buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
        labels=labels
    )


async def set_active_connections(count: int) -> None:
    """Set number of active connections."""
    await _metrics_collector.set_gauge(
        "aurum_active_connections",
        count
    )


async def increment_cache_hits(cache_type: str = "memory") -> None:
    """Increment cache hit counter."""
    await _metrics_collector.increment_counter(
        "aurum_cache_hits_total",
        labels={"type": cache_type}
    )


async def increment_cache_misses(cache_type: str = "memory") -> None:
    """Increment cache miss counter."""
    await _metrics_collector.increment_counter(
        "aurum_cache_misses_total",
        labels={"type": cache_type}
    )


async def observe_query_duration(
    query_type: str,
    duration_seconds: float
) -> None:
    """Observe database query duration."""
    await _metrics_collector.observe_histogram(
        "aurum_db_query_duration_seconds",
        duration_seconds,
        buckets=[0.01, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0],
        labels={"type": query_type}
    )


async def set_queue_size(queue_name: str, size: int) -> None:
    """Set queue size gauge."""
    await _metrics_collector.set_gauge(
        "aurum_queue_size",
        size,
        labels={"queue": queue_name}
    )
