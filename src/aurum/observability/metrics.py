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
    # External API metrics
    EXTERNAL_API_REQUEST_COUNTER = Counter(
        "aurum_external_api_requests_total",
        "External API request counter",
        ["endpoint", "status"],
    )
    EXTERNAL_API_LATENCY = Histogram(
        "aurum_external_api_request_duration_seconds",
        "External API request duration in seconds",
        ["endpoint"],
    )
    EXTERNAL_CACHE_HIT_COUNTER = Counter(
        "aurum_external_cache_hits_total",
        "External API cache hit counter",
        ["endpoint"],
    )
    EXTERNAL_CACHE_MISS_COUNTER = Counter(
        "aurum_external_cache_misses_total",
        "External API cache miss counter",
        ["endpoint"],
    )
    EXTERNAL_DAO_QUERY_COUNTER = Counter(
        "aurum_external_dao_queries_total",
        "External DAO query counter",
        ["operation", "status"],
    )
    EXTERNAL_DAO_LATENCY = Histogram(
        "aurum_external_dao_query_duration_seconds",
        "External DAO query duration in seconds",
        ["operation"],
    )
    EXTERNAL_CURVE_MAPPING_COUNTER = Counter(
        "aurum_external_curve_mappings_total",
        "External to curve mapping counter",
        ["mapping_type"],
    )

    # Rate limiting metrics
    RATE_LIMIT_REQUESTS = Counter(
        "aurum_rate_limit_requests_total",
        "Rate limit requests",
        ["endpoint", "limit_type"],
    )
    RATE_LIMIT_REJECTED = Counter(
        "aurum_rate_limit_rejected_total",
        "Rate limit rejected requests",
        ["endpoint", "limit_type"],
    )
    RATE_LIMIT_LATENCY = Histogram(
        "aurum_rate_limit_check_duration_seconds",
        "Rate limit check duration",
        ["endpoint"],
    )

    # Worker metrics
    WORKER_ACTIVE_TASKS = Gauge(
        "aurum_worker_active_tasks",
        "Number of active worker tasks",
        ["worker_type", "task_type"],
    )
    WORKER_TASK_DURATION = Histogram(
        "aurum_worker_task_duration_seconds",
        "Worker task duration",
        ["worker_type", "task_type", "status"],
        buckets=(0.01, 0.1, 1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0),
    )
    WORKER_QUEUE_SIZE = Gauge(
        "aurum_worker_queue_size",
        "Worker queue size",
        ["worker_type", "queue_name"],
    )
    WORKER_ERRORS = Counter(
        "aurum_worker_errors_total",
        "Worker errors",
        ["worker_type", "error_type"],
    )

    # Ingestion metrics
    INGESTION_RECORDS = Counter(
        "aurum_ingestion_records_total",
        "Ingested records",
        ["source", "dataset", "status"],
    )
    INGESTION_LATENCY = Histogram(
        "aurum_ingestion_duration_seconds",
        "Ingestion duration",
        ["source", "dataset"],
        buckets=(0.1, 1.0, 5.0, 30.0, 60.0, 300.0, 600.0, 1800.0),
    )
    INGESTION_BACKLOG = Gauge(
        "aurum_ingestion_backlog",
        "Ingestion backlog size",
        ["source", "dataset"],
    )
    INGESTION_CHECKPOINTS = Counter(
        "aurum_ingestion_checkpoints_total",
        "Ingestion checkpoints",
        ["source", "status"],
    )

    # Cache metrics (enhanced)
    CACHE_OPERATIONS = Counter(
        "aurum_cache_operations_total",
        "Cache operations",
        ["cache_type", "operation", "result"],
    )
    CACHE_SIZE = Gauge(
        "aurum_cache_size_bytes",
        "Cache size in bytes",
        ["cache_type"],
    )
    CACHE_EVICTIONS = Counter(
        "aurum_cache_evictions_total",
        "Cache evictions",
        ["cache_type", "reason"],
    )
    CACHE_TTL_EXPIRED = Counter(
        "aurum_cache_ttl_expired_total",
        "Cache TTL expirations",
        ["cache_type"],
    )

    # Database connection pool metrics
    DB_CONNECTIONS_ACTIVE = Gauge(
        "aurum_db_connections_active",
        "Active database connections",
        ["pool_name"],
    )
    DB_CONNECTIONS_IDLE = Gauge(
        "aurum_db_connections_idle",
        "Idle database connections",
        ["pool_name"],
    )
    DB_CONNECTIONS_TOTAL = Gauge(
        "aurum_db_connections_total",
        "Total database connections",
        ["pool_name"],
    )
    DB_CONNECTION_POOL_WAIT = Histogram(
        "aurum_db_connection_pool_wait_seconds",
        "Database connection pool wait time",
        ["pool_name"],
    )

    # Business metrics
    BUSINESS_TRANSACTIONS = Counter(
        "aurum_business_transactions_total",
        "Business transactions",
        ["transaction_type", "entity", "status"],
    )
    BUSINESS_VALUE = Histogram(
        "aurum_business_value",
        "Business value metrics",
        ["metric_type", "entity"],
    )
    BUSINESS_ERRORS = Counter(
        "aurum_business_errors_total",
        "Business operation errors",
        ["operation", "error_type"],
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
    # External API metrics
    EXTERNAL_API_REQUEST_COUNTER = EXTERNAL_API_LATENCY = None
    EXTERNAL_CACHE_HIT_COUNTER = EXTERNAL_CACHE_MISS_COUNTER = None
    EXTERNAL_DAO_QUERY_COUNTER = EXTERNAL_DAO_LATENCY = None
    EXTERNAL_CURVE_MAPPING_COUNTER = None
    # Rate limiting metrics
    RATE_LIMIT_REQUESTS = RATE_LIMIT_REJECTED = RATE_LIMIT_LATENCY = None
    # Worker metrics
    WORKER_ACTIVE_TASKS = WORKER_TASK_DURATION = WORKER_QUEUE_SIZE = WORKER_ERRORS = None
    # Ingestion metrics
    INGESTION_RECORDS = INGESTION_LATENCY = INGESTION_BACKLOG = INGESTION_CHECKPOINTS = None
    # Enhanced cache metrics
    CACHE_OPERATIONS = CACHE_SIZE = CACHE_EVICTIONS = CACHE_TTL_EXPIRED = None
    # Database connection pool metrics
    DB_CONNECTIONS_ACTIVE = DB_CONNECTIONS_IDLE = DB_CONNECTIONS_TOTAL = DB_CONNECTION_POOL_WAIT = None
    # Business metrics
    BUSINESS_TRANSACTIONS = BUSINESS_VALUE = BUSINESS_ERRORS = None
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


# Rate limiting helper functions
async def increment_rate_limit_requests(endpoint: str, limit_type: str = "user") -> None:
    """Increment rate limit request counter."""
    if RATE_LIMIT_REQUESTS is None:
        return
    try:
        RATE_LIMIT_REQUESTS.labels(endpoint=endpoint, limit_type=limit_type).inc()
    except Exception:
        pass

async def increment_rate_limit_rejected(endpoint: str, limit_type: str = "user") -> None:
    """Increment rate limit rejected counter."""
    if RATE_LIMIT_REJECTED is None:
        return
    try:
        RATE_LIMIT_REJECTED.labels(endpoint=endpoint, limit_type=limit_type).inc()
    except Exception:
        pass

async def observe_rate_limit_latency(endpoint: str, duration_seconds: float) -> None:
    """Observe rate limit check latency."""
    if RATE_LIMIT_LATENCY is None:
        return
    try:
        RATE_LIMIT_LATENCY.labels(endpoint=endpoint).observe(duration_seconds)
    except Exception:
        pass


# Worker helper functions
async def set_worker_active_tasks(worker_type: str, task_type: str, count: int) -> None:
    """Set number of active worker tasks."""
    if WORKER_ACTIVE_TASKS is None:
        return
    try:
        WORKER_ACTIVE_TASKS.labels(worker_type=worker_type, task_type=task_type).set(count)
    except Exception:
        pass

async def observe_worker_task_duration(worker_type: str, task_type: str, status: str, duration_seconds: float) -> None:
    """Observe worker task duration."""
    if WORKER_TASK_DURATION is None:
        return
    try:
        WORKER_TASK_DURATION.labels(worker_type=worker_type, task_type=task_type, status=status).observe(duration_seconds)
    except Exception:
        pass

async def set_worker_queue_size(worker_type: str, queue_name: str, size: int) -> None:
    """Set worker queue size."""
    if WORKER_QUEUE_SIZE is None:
        return
    try:
        WORKER_QUEUE_SIZE.labels(worker_type=worker_type, queue_name=queue_name).set(size)
    except Exception:
        pass

async def increment_worker_errors(worker_type: str, error_type: str) -> None:
    """Increment worker error counter."""
    if WORKER_ERRORS is None:
        return
    try:
        WORKER_ERRORS.labels(worker_type=worker_type, error_type=error_type).inc()
    except Exception:
        pass


# Ingestion helper functions
async def increment_ingestion_records(source: str, dataset: str, status: str = "success") -> None:
    """Increment ingestion records counter."""
    if INGESTION_RECORDS is None:
        return
    try:
        INGESTION_RECORDS.labels(source=source, dataset=dataset, status=status).inc()
    except Exception:
        pass

async def observe_ingestion_latency(source: str, dataset: str, duration_seconds: float) -> None:
    """Observe ingestion duration."""
    if INGESTION_LATENCY is None:
        return
    try:
        INGESTION_LATENCY.labels(source=source, dataset=dataset).observe(duration_seconds)
    except Exception:
        pass

async def set_ingestion_backlog(source: str, dataset: str, size: int) -> None:
    """Set ingestion backlog size."""
    if INGESTION_BACKLOG is None:
        return
    try:
        INGESTION_BACKLOG.labels(source=source, dataset=dataset).set(size)
    except Exception:
        pass

async def increment_ingestion_checkpoints(source: str, status: str = "success") -> None:
    """Increment ingestion checkpoints counter."""
    if INGESTION_CHECKPOINTS is None:
        return
    try:
        INGESTION_CHECKPOINTS.labels(source=source, status=status).inc()
    except Exception:
        pass


# Enhanced cache helper functions
async def increment_cache_operations(cache_type: str, operation: str, result: str = "hit") -> None:
    """Increment cache operation counter."""
    if CACHE_OPERATIONS is None:
        return
    try:
        CACHE_OPERATIONS.labels(cache_type=cache_type, operation=operation, result=result).inc()
    except Exception:
        pass

async def set_cache_size(cache_type: str, size_bytes: int) -> None:
    """Set cache size gauge."""
    if CACHE_SIZE is None:
        return
    try:
        CACHE_SIZE.labels(cache_type=cache_type).set(size_bytes)
    except Exception:
        pass

async def increment_cache_evictions(cache_type: str, reason: str = "capacity") -> None:
    """Increment cache eviction counter."""
    if CACHE_EVICTIONS is None:
        return
    try:
        CACHE_EVICTIONS.labels(cache_type=cache_type, reason=reason).inc()
    except Exception:
        pass

async def increment_cache_ttl_expired(cache_type: str) -> None:
    """Increment cache TTL expiration counter."""
    if CACHE_TTL_EXPIRED is None:
        return
    try:
        CACHE_TTL_EXPIRED.labels(cache_type=cache_type).inc()
    except Exception:
        pass


# Database connection pool helper functions
async def set_db_connections_active(pool_name: str, count: int) -> None:
    """Set active database connections."""
    if DB_CONNECTIONS_ACTIVE is None:
        return
    try:
        DB_CONNECTIONS_ACTIVE.labels(pool_name=pool_name).set(count)
    except Exception:
        pass

async def set_db_connections_idle(pool_name: str, count: int) -> None:
    """Set idle database connections."""
    if DB_CONNECTIONS_IDLE is None:
        return
    try:
        DB_CONNECTIONS_IDLE.labels(pool_name=pool_name).set(count)
    except Exception:
        pass

async def set_db_connections_total(pool_name: str, count: int) -> None:
    """Set total database connections."""
    if DB_CONNECTIONS_TOTAL is None:
        return
    try:
        DB_CONNECTIONS_TOTAL.labels(pool_name=pool_name).set(count)
    except Exception:
        pass

async def observe_db_connection_pool_wait(pool_name: str, duration_seconds: float) -> None:
    """Observe database connection pool wait time."""
    if DB_CONNECTION_POOL_WAIT is None:
        return
    try:
        DB_CONNECTION_POOL_WAIT.labels(pool_name=pool_name).observe(duration_seconds)
    except Exception:
        pass


# Business metrics helper functions
async def increment_business_transactions(transaction_type: str, entity: str, status: str = "success") -> None:
    """Increment business transaction counter."""
    if BUSINESS_TRANSACTIONS is None:
        return
    try:
        BUSINESS_TRANSACTIONS.labels(transaction_type=transaction_type, entity=entity, status=status).inc()
    except Exception:
        pass

async def observe_business_value(metric_type: str, entity: str, value: float) -> None:
    """Observe business value metric."""
    if BUSINESS_VALUE is None:
        return
    try:
        BUSINESS_VALUE.labels(metric_type=metric_type, entity=entity).observe(value)
    except Exception:
        pass

async def increment_business_errors(operation: str, error_type: str) -> None:
    """Increment business error counter."""
    if BUSINESS_ERRORS is None:
        return
    try:
        BUSINESS_ERRORS.labels(operation=operation, error_type=error_type).inc()
    except Exception:
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
    # Rate limiting metrics
    "RATE_LIMIT_REQUESTS",
    "RATE_LIMIT_REJECTED",
    "RATE_LIMIT_LATENCY",
    "increment_rate_limit_requests",
    "increment_rate_limit_rejected",
    "observe_rate_limit_latency",
    # Worker metrics
    "WORKER_ACTIVE_TASKS",
    "WORKER_TASK_DURATION",
    "WORKER_QUEUE_SIZE",
    "WORKER_ERRORS",
    "set_worker_active_tasks",
    "observe_worker_task_duration",
    "set_worker_queue_size",
    "increment_worker_errors",
    # Ingestion metrics
    "INGESTION_RECORDS",
    "INGESTION_LATENCY",
    "INGESTION_BACKLOG",
    "INGESTION_CHECKPOINTS",
    "increment_ingestion_records",
    "observe_ingestion_latency",
    "set_ingestion_backlog",
    "increment_ingestion_checkpoints",
    # Enhanced cache metrics
    "CACHE_OPERATIONS",
    "CACHE_SIZE",
    "CACHE_EVICTIONS",
    "CACHE_TTL_EXPIRED",
    "increment_cache_operations",
    "set_cache_size",
    "increment_cache_evictions",
    "increment_cache_ttl_expired",
    # Database connection pool metrics
    "DB_CONNECTIONS_ACTIVE",
    "DB_CONNECTIONS_IDLE",
    "DB_CONNECTIONS_TOTAL",
    "DB_CONNECTION_POOL_WAIT",
    "set_db_connections_active",
    "set_db_connections_idle",
    "set_db_connections_total",
    "observe_db_connection_pool_wait",
    # Business metrics
    "BUSINESS_TRANSACTIONS",
    "BUSINESS_VALUE",
    "BUSINESS_ERRORS",
    "increment_business_transactions",
    "observe_business_value",
    "increment_business_errors",
]
