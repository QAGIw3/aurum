"""Core observability metrics infrastructure.

This module provides the foundational metrics infrastructure including:
- Metric types and point definitions
- Basic Prometheus integration
- Core metric collection utilities
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple, Union, Any
import os

try:  # pragma: no cover - optional dependency
    from prometheus_client import (
        CONTENT_TYPE_LATEST as _PROM_CONTENT_TYPE,
    )
    from prometheus_client import (
        REGISTRY,
        Counter,
        Gauge,
        Histogram,
        CollectorRegistry,
    )
    from prometheus_client import (
        generate_latest as _prom_generate_latest,
    )
except ImportError:  # pragma: no cover - Prometheus not installed
    class _NoopMetric:  # Lightweight shim so calls are no-ops when lib missing
        def __init__(self, *args, **kwargs) -> None:
            pass

        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs) -> None:
            pass

        def observe(self, *args, **kwargs) -> None:
            pass

        def set(self, *args, **kwargs) -> None:
            pass

    Counter = _NoopMetric  # type: ignore[assignment]
    Gauge = _NoopMetric  # type: ignore[assignment]
    Histogram = _NoopMetric  # type: ignore[assignment]
    REGISTRY = None  # type: ignore[assignment]
    _PROM_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
    _prom_generate_latest = None  # type: ignore[assignment]


METRICS_PATH = "/metrics"
PROMETHEUS_AVAILABLE = Counter is not None and Gauge is not None and Histogram is not None and REGISTRY is not None


class MetricType(Enum):
    """Metric types emitted by the observability endpoints."""

    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricPoint:
    """A single metric sample."""

    name: str
    value: Union[int, float]
    timestamp: float
    labels: Dict[str, str]
    metric_type: MetricType


_METRIC_TYPE_MAP = {
    "counter": MetricType.COUNTER,
    "gauge": MetricType.GAUGE,
    "histogram": MetricType.HISTOGRAM,
    "summary": MetricType.SUMMARY,
}


def get_metric_type(metric_type: str) -> MetricType:
    """Get MetricType enum from string."""
    return _METRIC_TYPE_MAP.get(metric_type, MetricType.GAUGE)


def _existing_collector(name: str):
    """Get existing collector if registered, None otherwise."""
    if not PROMETHEUS_AVAILABLE:
        return None

    try:
        return REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
    except Exception:
        return None


def generate_latest():
    """Generate latest metrics in Prometheus format."""
    if not PROMETHEUS_AVAILABLE or _prom_generate_latest is None:
        return b"# No metrics available - Prometheus client not installed"

    try:
        return _prom_generate_latest(REGISTRY)  # type: ignore[call-arg]
    except Exception:
        return b"# Error generating metrics"


class MetricsCollector:
    """Core metrics collector providing metric registration and collection."""

    def __init__(self) -> None:
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()

    def counter(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ) -> Counter:
        """Get or create a counter metric."""
        if name in self._counters:
            return self._counters[name]

        if PROMETHEUS_AVAILABLE:
            # Check if already registered in global registry
            existing = _existing_collector(name)
            if existing is not None:
                counter = existing  # type: ignore[assignment]
            else:
                label_names = labels or []
                counter = Counter(name, description, label_names)
        else:
            counter = Counter(name, description, labels or [])

        self._counters[name] = counter
        return counter

    def gauge(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None
    ) -> Gauge:
        """Get or create a gauge metric."""
        if name in self._gauges:
            return self._gauges[name]

        if PROMETHEUS_AVAILABLE:
            existing = _existing_collector(name)
            if existing is not None:
                gauge = existing  # type: ignore[assignment]
            else:
                label_names = labels or []
                gauge = Gauge(name, description, label_names)
        else:
            gauge = Gauge(name, description, labels or [])

        self._gauges[name] = gauge
        return gauge

    def histogram(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[Tuple[float, ...]] = None
    ) -> Histogram:
        """Get or create a histogram metric."""
        if name in self._histograms:
            return self._histograms[name]

        if PROMETHEUS_AVAILABLE:
            existing = _existing_collector(name)
            if existing is not None:
                histogram = existing  # type: ignore[assignment]
            else:
                label_names = labels or []
                histogram = Histogram(name, description, label_names, buckets=buckets)
        else:
            histogram = Histogram(name, description, labels or [], buckets=buckets)

        self._histograms[name] = histogram
        return histogram

    def get_metrics(self) -> Dict[str, Any]:
        """Get all registered metrics."""
        return {
            "counters": list(self._counters.keys()),
            "gauges": list(self._gauges.keys()),
            "histograms": list(self._histograms.keys()),
        }


# Global collector instance
_collector = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector."""
    global _collector
    if _collector is None:
        _collector = MetricsCollector()
    return _collector
