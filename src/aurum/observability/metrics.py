"""Observability metrics utilities backed by Prometheus."""

from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Union

try:  # pragma: no cover - optional dependency
    from prometheus_client import (
        CONTENT_TYPE_LATEST as _PROM_CONTENT_TYPE,
        Counter,
        Gauge,
        Histogram,
        REGISTRY,
        generate_latest as _prom_generate_latest,
    )
except ImportError:  # pragma: no cover - Prometheus not installed
    Counter = None  # type: ignore[assignment]
    Gauge = None  # type: ignore[assignment]
    Histogram = None  # type: ignore[assignment]
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


if PROMETHEUS_AVAILABLE:  # pragma: no branch - simplify instrumentation when available
    REQUEST_COUNTER = Counter(
        "aurum_api_requests_total",
        "Total API requests",
        ["method", "path", "status"],
    )
    REQUEST_LATENCY = Histogram(
        "aurum_api_request_duration_seconds",
        "API request duration in seconds",
        ["method", "path"],
    )
    TILE_CACHE_COUNTER = Counter(
        "aurum_drought_tile_cache_total",
        "Drought tile cache lookup results",
        ["endpoint", "result"],
    )
    TILE_FETCH_LATENCY = Histogram(
        "aurum_drought_tile_fetch_seconds",
        "Downstream drought tile/info fetch latency in seconds",
        ["endpoint", "status"],
    )
    CACHE_HIT_COUNTER = Counter(
        "aurum_cache_hits_total",
        "Cache hit counter",
        ["type"],
    )
    CACHE_MISS_COUNTER = Counter(
        "aurum_cache_misses_total",
        "Cache miss counter",
        ["type"],
    )
    DB_QUERY_DURATION = Histogram(
        "aurum_db_query_duration_seconds",
        "Database query duration",
        ["type"],
    )
    QUEUE_SIZE_GAUGE = Gauge(
        "aurum_queue_size",
        "Queue backlog size",
        ["queue"],
    )
    ACTIVE_CONNECTIONS_GAUGE = Gauge(
        "aurum_active_connections",
        "Active API connections",
    )

    CONTENT_TYPE_LATEST = _PROM_CONTENT_TYPE
    generate_latest = _prom_generate_latest

    async def _metrics_middleware(request, call_next):  # pragma: no cover - exercised in integration
        request_path = request.url.path
        if request_path == METRICS_PATH:
            return await call_next(request)

        method = request.method
        route = request.scope.get("route") if hasattr(request, "scope") else None
        path_template = getattr(route, "path", request_path)
        start_time = time.perf_counter()
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
            return response
        finally:
            duration = time.perf_counter() - start_time
            try:
                REQUEST_LATENCY.labels(method=method, path=path_template).observe(duration)
                REQUEST_COUNTER.labels(method=method, path=path_template, status=status_code).inc()
            except Exception:  # pragma: no cover - guard against label issues
                pass

    METRICS_MIDDLEWARE = _metrics_middleware
else:  # pragma: no cover - Prometheus not present
    REQUEST_COUNTER = REQUEST_LATENCY = None
    TILE_CACHE_COUNTER = TILE_FETCH_LATENCY = None
    CACHE_HIT_COUNTER = CACHE_MISS_COUNTER = None
    DB_QUERY_DURATION = None
    QUEUE_SIZE_GAUGE = ACTIVE_CONNECTIONS_GAUGE = None
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"

    def generate_latest():  # type: ignore[misc]
        raise RuntimeError("prometheus_client is not installed")

    METRICS_MIDDLEWARE = None


class MetricsCollector:
    """Thin wrapper that exposes Prometheus metrics in a structured form."""

    async def collect_metrics(self) -> List[MetricPoint]:
        if not PROMETHEUS_AVAILABLE or REGISTRY is None:
            return []

        now = time.time()
        points: List[MetricPoint] = []
        for metric in REGISTRY.collect():
            metric_type = _METRIC_TYPE_MAP.get(metric.type, MetricType.GAUGE)
            for sample in metric.samples:
                labels = {str(key): str(value) for key, value in sample.labels.items()}
                timestamp = sample.timestamp if sample.timestamp is not None else now
                points.append(
                    MetricPoint(
                        name=sample.name,
                        value=sample.value,
                        timestamp=timestamp,
                        labels=labels,
                        metric_type=metric_type,
                    )
                )
        return points


_metrics_collector = MetricsCollector()


def get_metrics_collector() -> MetricsCollector:
    """Return the shared metrics collector instance."""

    return _metrics_collector


async def increment_api_requests(
    endpoint: str,
    method: str = "GET",
    status_code: int = 200,
) -> None:
    """Increment the API request counter for the given endpoint."""

    if REQUEST_COUNTER is None:
        return
    try:
        REQUEST_COUNTER.labels(method=method, path=endpoint, status=str(status_code)).inc()
    except Exception:  # pragma: no cover - guard against label mismatches
        pass


async def observe_api_latency(
    endpoint: str,
    method: str,
    duration_seconds: float,
) -> None:
    """Record API latency histogram observations."""

    if REQUEST_LATENCY is None:
        return
    try:
        REQUEST_LATENCY.labels(method=method, path=endpoint).observe(duration_seconds)
    except Exception:  # pragma: no cover - guard against label mismatches
        pass


async def set_active_connections(count: int) -> None:
    """Set the number of active connections."""

    if ACTIVE_CONNECTIONS_GAUGE is None:
        return
    try:
        ACTIVE_CONNECTIONS_GAUGE.set(count)
    except Exception:  # pragma: no cover - guard against gauge errors
        pass


async def increment_cache_hits(cache_type: str = "memory") -> None:
    """Increment cache hit counter."""

    if CACHE_HIT_COUNTER is None:
        return
    try:
        CACHE_HIT_COUNTER.labels(type=cache_type).inc()
    except Exception:  # pragma: no cover
        pass


async def increment_cache_misses(cache_type: str = "memory") -> None:
    """Increment cache miss counter."""

    if CACHE_MISS_COUNTER is None:
        return
    try:
        CACHE_MISS_COUNTER.labels(type=cache_type).inc()
    except Exception:  # pragma: no cover
        pass


async def observe_query_duration(
    query_type: str,
    duration_seconds: float,
) -> None:
    """Observe database query duration."""

    if DB_QUERY_DURATION is None:
        return
    try:
        DB_QUERY_DURATION.labels(type=query_type).observe(duration_seconds)
    except Exception:  # pragma: no cover
        pass


async def set_queue_size(queue_name: str, size: int) -> None:
    """Set queue size gauge."""

    if QUEUE_SIZE_GAUGE is None:
        return
    try:
        QUEUE_SIZE_GAUGE.labels(queue=queue_name).set(size)
    except Exception:  # pragma: no cover
        pass


__all__ = [
    "MetricPoint",
    "MetricType",
    "METRICS_MIDDLEWARE",
    "METRICS_PATH",
    "PROMETHEUS_AVAILABLE",
    "REQUEST_COUNTER",
    "REQUEST_LATENCY",
    "TILE_CACHE_COUNTER",
    "TILE_FETCH_LATENCY",
    "CONTENT_TYPE_LATEST",
    "generate_latest",
    "get_metrics_collector",
    "increment_api_requests",
    "observe_api_latency",
    "set_active_connections",
    "increment_cache_hits",
    "increment_cache_misses",
    "observe_query_duration",
    "set_queue_size",
]
