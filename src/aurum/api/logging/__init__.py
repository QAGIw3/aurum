"""Standardized logging and observability utilities for the Aurum API.

This module provides consistent logging patterns, observability helpers,
and structured logging utilities for all API components.
"""

from __future__ import annotations

import time
from typing import Any, Dict, Optional

from ..logging import structured_logger as _structured_logger
from ..telemetry.context import log_structured, get_request_id, get_tenant_id, get_user_id

get_logger = getattr(_structured_logger, "get_logger", lambda *_, **__: None)


def log_api_operation(
    operation: str,
    level: str = "info",
    duration_ms: Optional[float] = None,
    status: str = "success",
    **context: Any
) -> None:
    """Log an API operation with standardized fields.

    Args:
        operation: Name of the operation (e.g., "curves_query", "scenario_create")
        level: Log level ("debug", "info", "warning", "error")
        duration_ms: Operation duration in milliseconds
        status: Operation status ("success", "error", "timeout", etc.)
        **context: Additional context fields
    """
    log_context = {
        "operation": operation,
        "status": status,
        **context
    }

    if duration_ms is not None:
        log_context["duration_ms"] = duration_ms

    # Standardized event naming and fields
    log_structured(level, "api_operation", **log_context)


def log_performance_metric(
    metric_name: str,
    value: float,
    unit: str = "ms",
    threshold: Optional[float] = None,
    **context: Any
) -> None:
    """Log a performance metric with optional threshold checking.

    Args:
        metric_name: Name of the metric (e.g., "query_duration", "cache_hit_rate")
        value: Metric value
        unit: Unit of measurement
        threshold: Warning threshold value
        **context: Additional context fields
    """
    log_context = {
        "metric_name": metric_name,
        "metric_value": value,
        "metric_unit": unit,
        **context
    }

    level = "warning" if threshold and value > threshold else "info"
    log_structured(level, f"performance_{metric_name}", **log_context)


def log_cache_operation(
    operation: str,
    cache_key: str,
    hit: bool = True,
    duration_ms: Optional[float] = None,
    **context: Any
) -> None:
    """Log a cache operation with standardized fields.

    Args:
        operation: Cache operation ("get", "set", "invalidate", "miss")
        cache_key: Cache key (will be truncated for privacy)
        hit: Whether cache hit occurred
        duration_ms: Operation duration in milliseconds
        **context: Additional context fields
    """
    # Truncate cache key for privacy while keeping essential info
    cache_key_short = cache_key[:32] + "..." if len(cache_key) > 32 else cache_key

    log_context = {
        "cache_operation": operation,
        "cache_key": cache_key_short,
        "cache_hit": hit,
        **context
    }

    if duration_ms is not None:
        log_context["duration_ms"] = duration_ms

    level = "debug" if hit else "info"
    log_structured(level, f"cache_{operation}", **log_context)


def log_database_operation(
    operation: str,
    table: str,
    duration_ms: Optional[float] = None,
    row_count: Optional[int] = None,
    **context: Any
) -> None:
    """Log a database operation with standardized fields.

    Args:
        operation: Database operation ("select", "insert", "update", "delete")
        table: Target table name
        duration_ms: Operation duration in milliseconds
        row_count: Number of rows affected
        **context: Additional context fields
    """
    log_context = {
        "db_operation": operation,
        "db_table": table,
        **context
    }

    if duration_ms is not None:
        log_context["duration_ms"] = duration_ms

    if row_count is not None:
        log_context["row_count"] = row_count

    log_structured("info", f"database_{operation}", **log_context)


def log_api_request(
    method: str,
    endpoint: str,
    status_code: int,
    duration_ms: Optional[float] = None,
    **context: Any
) -> None:
    """Log an API request with standardized fields.

    Args:
        method: HTTP method (GET, POST, PUT, DELETE)
        endpoint: Request endpoint/path
        status_code: HTTP status code
        duration_ms: Request duration in milliseconds
        **context: Additional context fields
    """
    # Standardized field names
    log_context = {
        "method": method,
        "endpoint": endpoint,
        "status_code": status_code,
        **context
    }

    if duration_ms is not None:
        log_context["duration_ms"] = duration_ms

    level = "error" if status_code >= 500 else "warning" if status_code >= 400 else "info"
    log_structured(level, "http_request_completed", **log_context)


__all__ = [
    "get_logger",
    "log_api_operation",
    "log_performance_metric",
    "log_cache_operation",
    "log_database_operation",
    "log_api_request",
]
