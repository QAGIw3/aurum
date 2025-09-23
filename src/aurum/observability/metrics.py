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
    # Legacy basic API metrics (kept for backward compatibility). The enhanced
    # metrics defined later use the canonical names; these use distinct names to
    # avoid duplicate registration in the default CollectorRegistry. Additionally,
    # guard against duplicate registration by reusing existing collectors if present.
    def _existing_collector(name: str):
        try:
            return REGISTRY._names_to_collectors.get(name)  # type: ignore[attr-defined]
        except Exception:
            return None

    existing = _existing_collector("aurum_api_requests_basic_total")
    if existing is not None:
        REQUEST_COUNTER = existing  # type: ignore[assignment]
    else:
        REQUEST_COUNTER = Counter(
            "aurum_api_requests_basic_total",
            "Total API requests (basic labels)",
            ["method", "path", "status"],
        )

    existing = _existing_collector("aurum_api_request_duration_basic_seconds")
    if existing is not None:
        REQUEST_LATENCY = existing  # type: ignore[assignment]
    else:
        REQUEST_LATENCY = Histogram(
            "aurum_api_request_duration_basic_seconds",
            "API request duration in seconds (basic labels)",
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

    # SLO Metrics
    SLO_COMPLIANCE = Counter(
        "aurum_slo_compliance_total",
        "SLO compliance counter",
        ["slo_name", "status"],
    )
    SLO_VIOLATION_DURATION = Histogram(
        "aurum_slo_violation_duration_seconds",
        "SLO violation duration",
        ["slo_name"],
        buckets=(1, 5, 10, 30, 60, 300, 600, 1800, 3600),
    )
    SLO_AVAILABILITY = Gauge(
        "aurum_slo_availability",
        "SLO availability percentage",
        ["slo_name"],
    )
    SLO_LATENCY_P50 = Gauge(
        "aurum_slo_latency_p50_seconds",
        "SLO p50 latency",
        ["slo_name"],
    )
    SLO_LATENCY_P95 = Gauge(
        "aurum_slo_latency_p95_seconds",
        "SLO p95 latency",
        ["slo_name"],
    )
    SLO_LATENCY_P99 = Gauge(
        "aurum_slo_latency_p99_seconds",
        "SLO p99 latency",
        ["slo_name"],
    )
    SLO_ERROR_RATE = Gauge(
        "aurum_slo_error_rate",
        "SLO error rate percentage",
        ["slo_name"],
    )

    # API Performance Metrics (Enhanced)
    API_REQUESTS_TOTAL = Counter(
        "aurum_api_requests_total",
        "Total API requests",
        ["method", "path", "status", "tenant_id", "user_id"],
    )
    API_REQUEST_DURATION = Histogram(
        "aurum_api_request_duration_seconds",
        "API request duration",
        ["method", "path", "status", "tenant_id"],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0),
    )
    API_REQUEST_DURATION_PERCENTILES = Histogram(
        "aurum_api_request_duration_percentiles_seconds",
        "API request duration percentiles",
        ["method", "path", "percentile"],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0),
    )
    API_REQUEST_SIZE = Histogram(
        "aurum_api_request_size_bytes",
        "API request size",
        ["method", "path"],
        buckets=(100, 500, 1000, 5000, 10000, 50000, 100000),
    )
    API_RESPONSE_SIZE = Histogram(
        "aurum_api_response_size_bytes",
        "API response size",
        ["method", "path", "status"],
        buckets=(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000),
    )

    # Database Connection Pool Metrics (Enhanced)
    DB_POOL_CONNECTIONS_CREATED = Counter(
        "aurum_db_pool_connections_created_total",
        "Database connections created",
        ["pool_name"],
    )
    DB_POOL_CONNECTIONS_DESTROYED = Counter(
        "aurum_db_pool_connections_destroyed_total",
        "Database connections destroyed",
        ["pool_name"],
    )
    DB_POOL_CONNECTIONS_ACTIVE_CURRENT = Gauge(
        "aurum_db_pool_connections_active_current",
        "Current active database connections",
        ["pool_name"],
    )
    DB_POOL_CONNECTIONS_IDLE_CURRENT = Gauge(
        "aurum_db_pool_connections_idle_current",
        "Current idle database connections",
        ["pool_name"],
    )
    DB_POOL_CONNECTIONS_TOTAL_CURRENT = Gauge(
        "aurum_db_pool_connections_total_current",
        "Current total database connections",
        ["pool_name"],
    )
    DB_POOL_ACQUIRE_TIME = Histogram(
        "aurum_db_pool_acquire_time_seconds",
        "Database connection acquire time",
        ["pool_name"],
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    )
    DB_POOL_CONNECTION_TIMEOUTS = Counter(
        "aurum_db_pool_connection_timeouts_total",
        "Database connection timeouts",
        ["pool_name"],
    )
    DB_POOL_CONNECTION_ERRORS = Counter(
        "aurum_db_pool_connection_errors_total",
        "Database connection errors",
        ["pool_name", "error_type"],
    )

    # Trino-Specific Metrics
    TRINO_QUERY_TOTAL = Counter(
        "aurum_trino_queries_total",
        "Total Trino queries executed",
        ["tenant_id", "status"],
    )
    TRINO_QUERY_DURATION = Histogram(
        "aurum_trino_query_duration_seconds",
        "Trino query execution duration",
        ["tenant_id", "query_type"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0),
    )
    TRINO_CONNECTION_POOL_ACTIVE = Gauge(
        "aurum_trino_connection_pool_active",
        "Active Trino connections",
    )
    TRINO_CONNECTION_POOL_IDLE = Gauge(
        "aurum_trino_connection_pool_idle",
        "Idle Trino connections",
    )
    TRINO_CONNECTION_POOL_UTILIZATION = Gauge(
        "aurum_trino_connection_pool_utilization",
        "Trino connection pool utilization (0-1)",
    )
    TRINO_CONNECTION_ACQUIRE_TIME = Histogram(
        "aurum_trino_connection_acquire_time_seconds",
        "Time to acquire Trino connection",
        buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
    )
    TRINO_CIRCUIT_BREAKER_STATE = Gauge(
        "aurum_trino_circuit_breaker_state",
        "Trino circuit breaker state (0=closed, 1=open, 2=half_open)",
        ["state"],
    )
    TRINO_POOL_SATURATION = Gauge(
        "aurum_trino_pool_saturation",
        "Trino pool saturation level (0-1)",
    )
    TRINO_POOL_SATURATION_STATUS = Gauge(
        "aurum_trino_pool_saturation_status",
        "Trino pool saturation status (0=healthy, 1=warning, 2=critical)",
        ["status"],
    )

    # Cache Performance Metrics (Enhanced)
    CACHE_HIT_RATIO = Gauge(
        "aurum_cache_hit_ratio",
        "Cache hit ratio",
        ["cache_type"],
    )
    CACHE_EFFICIENCY = Gauge(
        "aurum_cache_efficiency",
        "Cache efficiency score",
        ["cache_type"],
    )
    CACHE_MEMORY_USAGE = Gauge(
        "aurum_cache_memory_usage_bytes",
        "Cache memory usage",
        ["cache_type"],
    )
    CACHE_INVALIDATION_COUNT = Counter(
        "aurum_cache_invalidation_total",
        "Cache invalidations",
        ["cache_type", "reason"],
    )
    CACHE_STALE_HITS = Counter(
        "aurum_cache_stale_hits_total",
        "Cache stale hits",
        ["cache_type"],
    )

    # Data Freshness Metrics
    DATA_FRESHNESS_HOURS = Gauge(
        "aurum_data_freshness_hours",
        "Data freshness in hours",
        ["dataset", "source"],
    )
    DATA_FRESHNESS_SCORE = Gauge(
        "aurum_data_freshness_score",
        "Data freshness score (0-1)",
        ["dataset", "source"],
    )
    DATA_STALENESS_VIOLATIONS = Counter(
        "aurum_data_staleness_violations_total",
        "Data staleness violations",
        ["dataset", "severity"],
    )

    # Great Expectations Validation Metrics
    GE_VALIDATION_RUNS = Counter(
        "aurum_ge_validation_runs_total",
        "Great Expectations validation runs",
        ["dataset", "suite", "status"],
    )
    GE_VALIDATION_DURATION = Histogram(
        "aurum_ge_validation_duration_seconds",
        "Great Expectations validation duration",
        ["dataset", "suite"],
        buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0),
    )
    GE_VALIDATION_EXPECTATIONS = Counter(
        "aurum_ge_validation_expectations_total",
        "Great Expectations validation expectations",
        ["dataset", "suite", "status"],
    )
    GE_VALIDATION_QUALITY_SCORE = Gauge(
        "aurum_ge_validation_quality_score",
        "Great Expectations validation quality score",
        ["dataset", "suite"],
    )

    # Canary Metrics
    CANARY_EXECUTION_STATUS = Counter(
        "aurum_canary_execution_status_total",
        "Canary execution status",
        ["canary_name", "status"],
    )
    CANARY_EXECUTION_DURATION = Histogram(
        "aurum_canary_execution_duration_seconds",
        "Canary execution duration",
        ["canary_name"],
        buckets=(0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 300.0),
    )
    CANARY_API_HEALTH_SCORE = Gauge(
        "aurum_canary_api_health_score",
        "Canary API health score",
        ["canary_name"],
    )
    CANARY_DATA_QUALITY_SCORE = Gauge(
        "aurum_canary_data_quality_score",
        "Canary data quality score",
        ["canary_name"],
    )

    # Staleness Monitoring Metrics
    STALENESS_MONITOR_CHECKS = Counter(
        "aurum_staleness_monitor_checks_total",
        "Staleness monitor checks",
        ["dataset", "status"],
    )
    STALENESS_MONITOR_DURATION = Histogram(
        "aurum_staleness_monitor_duration_seconds",
        "Staleness monitor check duration",
        ["dataset"],
        buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
    )
    STALENESS_LEVEL = Gauge(
        "aurum_staleness_level",
        "Current staleness level",
        ["dataset"],
    )
    STALENESS_AGE_HOURS = Gauge(
        "aurum_staleness_age_hours",
        "Hours since last update",
        ["dataset"],
    )

    # Alerting Metrics
    ALERTS_FIRED = Counter(
        "aurum_alerts_fired_total",
        "Alerts fired",
        ["alert_name", "severity", "channel"],
    )
    ALERTS_SUPPRESSED = Counter(
        "aurum_alerts_suppressed_total",
        "Alerts suppressed",
        ["alert_name", "reason"],
    )
    ALERT_PROCESSING_DURATION = Histogram(
        "aurum_alert_processing_duration_seconds",
        "Alert processing duration",
        ["alert_name"],
        buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0),
    )

    # System Health Metrics
    SYSTEM_HEALTH_SCORE = Gauge(
        "aurum_system_health_score",
        "Overall system health score",
    )
    COMPONENT_HEALTH_STATUS = Gauge(
        "aurum_component_health_status",
        "Component health status",
        ["component", "status"],
    )
    SYSTEM_UPTIME_SECONDS = Counter(
        "aurum_system_uptime_seconds_total",
        "System uptime in seconds",
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


# SLO Metrics Helper Functions
async def increment_slo_compliance(slo_name: str, status: str = "success") -> None:
    """Increment SLO compliance counter."""
    if SLO_COMPLIANCE is None:
        return
    try:
        SLO_COMPLIANCE.labels(slo_name=slo_name, status=status).inc()
    except Exception:
        pass


async def observe_slo_violation_duration(slo_name: str, duration_seconds: float) -> None:
    """Observe SLO violation duration."""
    if SLO_VIOLATION_DURATION is None:
        return
    try:
        SLO_VIOLATION_DURATION.labels(slo_name=slo_name).observe(duration_seconds)
    except Exception:
        pass


async def set_slo_availability(slo_name: str, availability: float) -> None:
    """Set SLO availability percentage."""
    if SLO_AVAILABILITY is None:
        return
    try:
        SLO_AVAILABILITY.labels(slo_name=slo_name).set(availability)
    except Exception:
        pass


async def set_slo_latency_p50(slo_name: str, latency_seconds: float) -> None:
    """Set SLO p50 latency."""
    if SLO_LATENCY_P50 is None:
        return
    try:
        SLO_LATENCY_P50.labels(slo_name=slo_name).set(latency_seconds)
    except Exception:
        pass


async def set_slo_latency_p95(slo_name: str, latency_seconds: float) -> None:
    """Set SLO p95 latency."""
    if SLO_LATENCY_P95 is None:
        return
    try:
        SLO_LATENCY_P95.labels(slo_name=slo_name).set(latency_seconds)
    except Exception:
        pass


async def set_slo_latency_p99(slo_name: str, latency_seconds: float) -> None:
    """Set SLO p99 latency."""
    if SLO_LATENCY_P99 is None:
        return
    try:
        SLO_LATENCY_P99.labels(slo_name=slo_name).set(latency_seconds)
    except Exception:
        pass


async def set_slo_error_rate(slo_name: str, error_rate: float) -> None:
    """Set SLO error rate percentage."""
    if SLO_ERROR_RATE is None:
        return
    try:
        SLO_ERROR_RATE.labels(slo_name=slo_name).set(error_rate)
    except Exception:
        pass


# Enhanced API Metrics Helper Functions
async def increment_api_requests(
    method: str,
    path: str,
    status_code: int,
    tenant_id: str = "",
    user_id: str = ""
) -> None:
    """Increment API requests counter with enhanced labels."""
    if API_REQUESTS_TOTAL is None:
        return
    try:
        API_REQUESTS_TOTAL.labels(
            method=method,
            path=path,
            status=str(status_code),
            tenant_id=tenant_id,
            user_id=user_id
        ).inc()
    except Exception:
        pass


async def observe_api_request_duration(
    method: str,
    path: str,
    status_code: int,
    duration_seconds: float,
    tenant_id: str = ""
) -> None:
    """Observe API request duration with enhanced labels."""
    if API_REQUEST_DURATION is None:
        return
    try:
        API_REQUEST_DURATION.labels(
            method=method,
            path=path,
            status=str(status_code),
            tenant_id=tenant_id
        ).observe(duration_seconds)
    except Exception:
        pass


async def observe_api_request_duration_percentile(
    method: str,
    path: str,
    percentile: str,
    duration_seconds: float
) -> None:
    """Observe API request duration percentiles."""
    if API_REQUEST_DURATION_PERCENTILES is None:
        return
    try:
        API_REQUEST_DURATION_PERCENTILES.labels(
            method=method,
            path=path,
            percentile=percentile
        ).observe(duration_seconds)
    except Exception:
        pass


async def observe_api_request_size(method: str, path: str, size_bytes: int) -> None:
    """Observe API request size."""
    if API_REQUEST_SIZE is None:
        return
    try:
        API_REQUEST_SIZE.labels(method=method, path=path).observe(size_bytes)
    except Exception:
        pass


async def observe_api_response_size(method: str, path: str, status_code: int, size_bytes: int) -> None:
    """Observe API response size."""
    if API_RESPONSE_SIZE is None:
        return
    try:
        API_RESPONSE_SIZE.labels(
            method=method,
            path=path,
            status=str(status_code)
        ).observe(size_bytes)
    except Exception:
        pass


# Enhanced Database Connection Pool Metrics Helper Functions
async def increment_db_pool_connections_created(pool_name: str) -> None:
    """Increment database pool connections created counter."""
    if DB_POOL_CONNECTIONS_CREATED is None:
        return
    try:
        DB_POOL_CONNECTIONS_CREATED.labels(pool_name=pool_name).inc()
    except Exception:
        pass


async def increment_db_pool_connections_destroyed(pool_name: str) -> None:
    """Increment database pool connections destroyed counter."""
    if DB_POOL_CONNECTIONS_DESTROYED is None:
        return
    try:
        DB_POOL_CONNECTIONS_DESTROYED.labels(pool_name=pool_name).inc()
    except Exception:
        pass


async def set_db_pool_connections_active(pool_name: str, count: int) -> None:
    """Set current active database connections."""
    if DB_POOL_CONNECTIONS_ACTIVE_CURRENT is None:
        return
    try:
        DB_POOL_CONNECTIONS_ACTIVE_CURRENT.labels(pool_name=pool_name).set(count)
    except Exception:
        pass


async def set_db_pool_connections_idle(pool_name: str, count: int) -> None:
    """Set current idle database connections."""
    if DB_POOL_CONNECTIONS_IDLE_CURRENT is None:
        return
    try:
        DB_POOL_CONNECTIONS_IDLE_CURRENT.labels(pool_name=pool_name).set(count)
    except Exception:
        pass


async def set_db_pool_connections_total(pool_name: str, count: int) -> None:
    """Set current total database connections."""
    if DB_POOL_CONNECTIONS_TOTAL_CURRENT is None:
        return
    try:
        DB_POOL_CONNECTIONS_TOTAL_CURRENT.labels(pool_name=pool_name).set(count)
    except Exception:
        pass


async def observe_db_pool_acquire_time(pool_name: str, duration_seconds: float) -> None:
    """Observe database connection pool acquire time."""
    if DB_POOL_ACQUIRE_TIME is None:
        return
    try:
        DB_POOL_ACQUIRE_TIME.labels(pool_name=pool_name).observe(duration_seconds)
    except Exception:
        pass


async def increment_db_pool_connection_timeouts(pool_name: str) -> None:
    """Increment database pool connection timeouts counter."""
    if DB_POOL_CONNECTION_TIMEOUTS is None:
        return
    try:
        DB_POOL_CONNECTION_TIMEOUTS.labels(pool_name=pool_name).inc()
    except Exception:
        pass


async def increment_db_pool_connection_errors(pool_name: str, error_type: str) -> None:
    """Increment database pool connection errors counter."""
    if DB_POOL_CONNECTION_ERRORS is None:
        return
    try:
        DB_POOL_CONNECTION_ERRORS.labels(pool_name=pool_name, error_type=error_type).inc()
    except Exception:
        pass


# Enhanced Cache Performance Metrics Helper Functions
async def set_cache_hit_ratio(cache_type: str, hit_ratio: float) -> None:
    """Set cache hit ratio gauge."""
    if CACHE_HIT_RATIO is None:
        return
    try:
        CACHE_HIT_RATIO.labels(cache_type=cache_type).set(hit_ratio)
    except Exception:
        pass


async def set_cache_efficiency(cache_type: str, efficiency: float) -> None:
    """Set cache efficiency score."""
    if CACHE_EFFICIENCY is None:
        return
    try:
        CACHE_EFFICIENCY.labels(cache_type=cache_type).set(efficiency)
    except Exception:
        pass


async def set_cache_memory_usage(cache_type: str, size_bytes: int) -> None:
    """Set cache memory usage."""
    if CACHE_MEMORY_USAGE is None:
        return
    try:
        CACHE_MEMORY_USAGE.labels(cache_type=cache_type).set(size_bytes)
    except Exception:
        pass


async def increment_cache_invalidation(cache_type: str, reason: str) -> None:
    """Increment cache invalidation counter."""
    if CACHE_INVALIDATION_COUNT is None:
        return
    try:
        CACHE_INVALIDATION_COUNT.labels(cache_type=cache_type, reason=reason).inc()
    except Exception:
        pass


async def increment_cache_stale_hits(cache_type: str) -> None:
    """Increment cache stale hits counter."""
    if CACHE_STALE_HITS is None:
        return
    try:
        CACHE_STALE_HITS.labels(cache_type=cache_type).inc()
    except Exception:
        pass


# Data Freshness Metrics Helper Functions
async def set_data_freshness_hours(dataset: str, source: str, hours: float) -> None:
    """Set data freshness in hours."""
    if DATA_FRESHNESS_HOURS is None:
        return
    try:
        DATA_FRESHNESS_HOURS.labels(dataset=dataset, source=source).set(hours)
    except Exception:
        pass


async def set_data_freshness_score(dataset: str, source: str, score: float) -> None:
    """Set data freshness score."""
    if DATA_FRESHNESS_SCORE is None:
        return
    try:
        DATA_FRESHNESS_SCORE.labels(dataset=dataset, source=source).set(score)
    except Exception:
        pass


async def increment_data_staleness_violations(dataset: str, severity: str) -> None:
    """Increment data staleness violations counter."""
    if DATA_STALENESS_VIOLATIONS is None:
        return
    try:
        DATA_STALENESS_VIOLATIONS.labels(dataset=dataset, severity=severity).inc()
    except Exception:
        pass


# Great Expectations Validation Metrics Helper Functions
async def increment_ge_validation_runs(dataset: str, suite: str, status: str = "success") -> None:
    """Increment Great Expectations validation runs counter."""
    if GE_VALIDATION_RUNS is None:
        return
    try:
        GE_VALIDATION_RUNS.labels(dataset=dataset, suite=suite, status=status).inc()
    except Exception:
        pass


async def observe_ge_validation_duration(dataset: str, suite: str, duration_seconds: float) -> None:
    """Observe Great Expectations validation duration."""
    if GE_VALIDATION_DURATION is None:
        return
    try:
        GE_VALIDATION_DURATION.labels(dataset=dataset, suite=suite).observe(duration_seconds)
    except Exception:
        pass


async def increment_ge_validation_expectations(dataset: str, suite: str, status: str) -> None:
    """Increment Great Expectations validation expectations counter."""
    if GE_VALIDATION_EXPECTATIONS is None:
        return
    try:
        GE_VALIDATION_EXPECTATIONS.labels(dataset=dataset, suite=suite, status=status).inc()
    except Exception:
        pass


async def set_ge_validation_quality_score(dataset: str, suite: str, score: float) -> None:
    """Set Great Expectations validation quality score."""
    if GE_VALIDATION_QUALITY_SCORE is None:
        return
    try:
        GE_VALIDATION_QUALITY_SCORE.labels(dataset=dataset, suite=suite).set(score)
    except Exception:
        pass


# Canary Metrics Helper Functions
async def increment_canary_execution_status(canary_name: str, status: str) -> None:
    """Increment canary execution status counter."""
    if CANARY_EXECUTION_STATUS is None:
        return
    try:
        CANARY_EXECUTION_STATUS.labels(canary_name=canary_name, status=status).inc()
    except Exception:
        pass


async def observe_canary_execution_duration(canary_name: str, duration_seconds: float) -> None:
    """Observe canary execution duration."""
    if CANARY_EXECUTION_DURATION is None:
        return
    try:
        CANARY_EXECUTION_DURATION.labels(canary_name=canary_name).observe(duration_seconds)
    except Exception:
        pass


async def set_canary_api_health_score(canary_name: str, score: float) -> None:
    """Set canary API health score."""
    if CANARY_API_HEALTH_SCORE is None:
        return
    try:
        CANARY_API_HEALTH_SCORE.labels(canary_name=canary_name).set(score)
    except Exception:
        pass


async def set_canary_data_quality_score(canary_name: str, score: float) -> None:
    """Set canary data quality score."""
    if CANARY_DATA_QUALITY_SCORE is None:
        return
    try:
        CANARY_DATA_QUALITY_SCORE.labels(canary_name=canary_name).set(score)
    except Exception:
        pass


# Staleness Monitoring Metrics Helper Functions
async def increment_staleness_monitor_checks(dataset: str, status: str = "success") -> None:
    """Increment staleness monitor checks counter."""
    if STALENESS_MONITOR_CHECKS is None:
        return
    try:
        STALENESS_MONITOR_CHECKS.labels(dataset=dataset, status=status).inc()
    except Exception:
        pass


async def observe_staleness_monitor_duration(dataset: str, duration_seconds: float) -> None:
    """Observe staleness monitor check duration."""
    if STALENESS_MONITOR_DURATION is None:
        return
    try:
        STALENESS_MONITOR_DURATION.labels(dataset=dataset).observe(duration_seconds)
    except Exception:
        pass


async def set_staleness_level(dataset: str, level: float) -> None:
    """Set current staleness level."""
    if STALENESS_LEVEL is None:
        return
    try:
        STALENESS_LEVEL.labels(dataset=dataset).set(level)
    except Exception:
        pass


async def set_staleness_age_hours(dataset: str, age_hours: float) -> None:
    """Set hours since last update."""
    if STALENESS_AGE_HOURS is None:
        return
    try:
        STALENESS_AGE_HOURS.labels(dataset=dataset).set(age_hours)
    except Exception:
        pass


# Alerting Metrics Helper Functions
async def increment_alerts_fired(alert_name: str, severity: str, channel: str) -> None:
    """Increment alerts fired counter."""
    if ALERTS_FIRED is None:
        return
    try:
        ALERTS_FIRED.labels(alert_name=alert_name, severity=severity, channel=channel).inc()
    except Exception:
        pass


async def increment_alerts_suppressed(alert_name: str, reason: str) -> None:
    """Increment alerts suppressed counter."""
    if ALERTS_SUPPRESSED is None:
        return
    try:
        ALERTS_SUPPRESSED.labels(alert_name=alert_name, reason=reason).inc()
    except Exception:
        pass


async def observe_alert_processing_duration(alert_name: str, duration_seconds: float) -> None:
    """Observe alert processing duration."""
    if ALERT_PROCESSING_DURATION is None:
        return
    try:
        ALERT_PROCESSING_DURATION.labels(alert_name=alert_name).observe(duration_seconds)
    except Exception:
        pass


# System Health Metrics Helper Functions
async def set_system_health_score(score: float) -> None:
    """Set overall system health score."""
    if SYSTEM_HEALTH_SCORE is None:
        return
    try:
        SYSTEM_HEALTH_SCORE.set(score)
    except Exception:
        pass


async def set_component_health_status(component: str, status: str) -> None:
    """Set component health status."""
    if COMPONENT_HEALTH_STATUS is None:
        return
    try:
        COMPONENT_HEALTH_STATUS.labels(component=component, status=status).set(1)
    except Exception:
        pass


async def increment_system_uptime_seconds() -> None:
    """Increment system uptime counter."""
    if SYSTEM_UPTIME_SECONDS is None:
        return
    try:
        SYSTEM_UPTIME_SECONDS.inc()
    except Exception:
        pass


# Trino Metrics Helper Functions
async def increment_trino_queries(tenant_id: str, status: str = "success") -> None:
    """Increment Trino query counter."""
    if TRINO_QUERY_TOTAL is None:
        return
    try:
        TRINO_QUERY_TOTAL.labels(tenant_id=tenant_id, status=status).inc()
    except Exception:
        pass


async def observe_trino_query_duration(tenant_id: str, query_type: str, duration_seconds: float) -> None:
    """Observe Trino query duration."""
    if TRINO_QUERY_DURATION is None:
        return
    try:
        TRINO_QUERY_DURATION.labels(tenant_id=tenant_id, query_type=query_type).observe(duration_seconds)
    except Exception:
        pass


async def set_trino_connection_pool_active(count: int) -> None:
    """Set active Trino connections."""
    if TRINO_CONNECTION_POOL_ACTIVE is None:
        return
    try:
        TRINO_CONNECTION_POOL_ACTIVE.set(count)
    except Exception:
        pass


async def set_trino_connection_pool_idle(count: int) -> None:
    """Set idle Trino connections."""
    if TRINO_CONNECTION_POOL_IDLE is None:
        return
    try:
        TRINO_CONNECTION_POOL_IDLE.set(count)
    except Exception:
        pass


async def set_trino_connection_pool_utilization(utilization: float) -> None:
    """Set Trino connection pool utilization."""
    if TRINO_CONNECTION_POOL_UTILIZATION is None:
        return
    try:
        TRINO_CONNECTION_POOL_UTILIZATION.set(utilization)
    except Exception:
        pass


async def observe_trino_connection_acquire_time(duration_seconds: float) -> None:
    """Observe Trino connection acquire time."""
    if TRINO_CONNECTION_ACQUIRE_TIME is None:
        return
    try:
        TRINO_CONNECTION_ACQUIRE_TIME.observe(duration_seconds)
    except Exception:
        pass


async def set_trino_circuit_breaker_state(state: str) -> None:
    """Set Trino circuit breaker state."""
    if TRINO_CIRCUIT_BREAKER_STATE is None:
        return
    try:
        # Convert state to numeric value
        state_map = {"closed": 0, "open": 1, "half_open": 2}
        numeric_state = state_map.get(state.lower(), 0)
        TRINO_CIRCUIT_BREAKER_STATE.labels(state=state).set(numeric_state)
    except Exception:
        pass


async def set_trino_pool_saturation(saturation: float) -> None:
    """Set Trino pool saturation level."""
    if TRINO_POOL_SATURATION is None:
        return
    try:
        TRINO_POOL_SATURATION.set(saturation)
    except Exception:
        pass


async def set_trino_pool_saturation_status(status: str) -> None:
    """Set Trino pool saturation status."""
    if TRINO_POOL_SATURATION_STATUS is None:
        return
    try:
        # Convert status to numeric value
        status_map = {"healthy": 0, "warning": 1, "critical": 2}
        numeric_status = status_map.get(status.lower(), 0)
        TRINO_POOL_SATURATION_STATUS.labels(status=status).set(numeric_status)
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
    # SLO Metrics
    "SLO_COMPLIANCE",
    "SLO_VIOLATION_DURATION",
    "SLO_AVAILABILITY",
    "SLO_LATENCY_P50",
    "SLO_LATENCY_P95",
    "SLO_LATENCY_P99",
    "SLO_ERROR_RATE",
    "increment_slo_compliance",
    "observe_slo_violation_duration",
    "set_slo_availability",
    "set_slo_latency_p50",
    "set_slo_latency_p95",
    "set_slo_latency_p99",
    "set_slo_error_rate",
    # Enhanced API Metrics
    "API_REQUESTS_TOTAL",
    "API_REQUEST_DURATION",
    "API_REQUEST_DURATION_PERCENTILES",
    "API_REQUEST_SIZE",
    "API_RESPONSE_SIZE",
    "increment_api_requests",
    "observe_api_request_duration",
    "observe_api_request_duration_percentile",
    "observe_api_request_size",
    "observe_api_response_size",
    # Enhanced Database Connection Pool Metrics
    "DB_POOL_CONNECTIONS_CREATED",
    "DB_POOL_CONNECTIONS_DESTROYED",
    "DB_POOL_CONNECTIONS_ACTIVE_CURRENT",
    "DB_POOL_CONNECTIONS_IDLE_CURRENT",
    "DB_POOL_CONNECTIONS_TOTAL_CURRENT",
    "DB_POOL_ACQUIRE_TIME",
    "DB_POOL_CONNECTION_TIMEOUTS",
    "DB_POOL_CONNECTION_ERRORS",
    "increment_db_pool_connections_created",
    "increment_db_pool_connections_destroyed",
    "set_db_pool_connections_active",
    "set_db_pool_connections_idle",
    "set_db_pool_connections_total",
    "observe_db_pool_acquire_time",
    "increment_db_pool_connection_timeouts",
    "increment_db_pool_connection_errors",
    # Enhanced Cache Performance Metrics
    "CACHE_HIT_RATIO",
    "CACHE_EFFICIENCY",
    "CACHE_MEMORY_USAGE",
    "CACHE_INVALIDATION_COUNT",
    "CACHE_STALE_HITS",
    "set_cache_hit_ratio",
    "set_cache_efficiency",
    "set_cache_memory_usage",
    "increment_cache_invalidation",
    "increment_cache_stale_hits",
    # Data Freshness Metrics
    "DATA_FRESHNESS_HOURS",
    "DATA_FRESHNESS_SCORE",
    "DATA_STALENESS_VIOLATIONS",
    "set_data_freshness_hours",
    "set_data_freshness_score",
    "increment_data_staleness_violations",
    # Great Expectations Validation Metrics
    "GE_VALIDATION_RUNS",
    "GE_VALIDATION_DURATION",
    "GE_VALIDATION_EXPECTATIONS",
    "GE_VALIDATION_QUALITY_SCORE",
    "increment_ge_validation_runs",
    "observe_ge_validation_duration",
    "increment_ge_validation_expectations",
    "set_ge_validation_quality_score",
    # Canary Metrics
    "CANARY_EXECUTION_STATUS",
    "CANARY_EXECUTION_DURATION",
    "CANARY_API_HEALTH_SCORE",
    "CANARY_DATA_QUALITY_SCORE",
    "increment_canary_execution_status",
    "observe_canary_execution_duration",
    "set_canary_api_health_score",
    "set_canary_data_quality_score",
    # Staleness Monitoring Metrics
    "STALENESS_MONITOR_CHECKS",
    "STALENESS_MONITOR_DURATION",
    "STALENESS_LEVEL",
    "STALENESS_AGE_HOURS",
    "increment_staleness_monitor_checks",
    "observe_staleness_monitor_duration",
    "set_staleness_level",
    "set_staleness_age_hours",
    # Alerting Metrics
    "ALERTS_FIRED",
    "ALERTS_SUPPRESSED",
    "ALERT_PROCESSING_DURATION",
    "increment_alerts_fired",
    "increment_alerts_suppressed",
    "observe_alert_processing_duration",
    # System Health Metrics
    "SYSTEM_HEALTH_SCORE",
    "COMPONENT_HEALTH_STATUS",
    "SYSTEM_UPTIME_SECONDS",
    "set_system_health_score",
    "set_component_health_status",
    "increment_system_uptime_seconds",
    # Trino metrics
    "TRINO_QUERY_TOTAL",
    "TRINO_QUERY_DURATION",
    "TRINO_CONNECTION_POOL_ACTIVE",
    "TRINO_CONNECTION_POOL_IDLE",
    "TRINO_CONNECTION_POOL_UTILIZATION",
    "TRINO_CONNECTION_ACQUIRE_TIME",
    "TRINO_CIRCUIT_BREAKER_STATE",
    "TRINO_POOL_SATURATION",
    "TRINO_POOL_SATURATION_STATUS",
    "increment_trino_queries",
    "observe_trino_query_duration",
    "set_trino_connection_pool_active",
    "set_trino_connection_pool_idle",
    "set_trino_connection_pool_utilization",
    "observe_trino_connection_acquire_time",
    "set_trino_circuit_breaker_state",
    "set_trino_pool_saturation",
    "set_trino_pool_saturation_status",
]
