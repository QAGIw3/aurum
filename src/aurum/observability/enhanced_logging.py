"""Enhanced structured logging for better observability.

This module provides enhanced logging capabilities with:
- Structured logging with JSON format
- Request correlation across all log entries
- Performance metrics in logs
- Cache hit/miss tracking
- Database operation logging
- Error context and stack traces
"""

from __future__ import annotations

import json
import logging
import sys
import time
import traceback
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..telemetry.context import get_correlation_id, get_request_id

# Context variables for logging
log_context: ContextVar[Dict[str, Any]] = ContextVar("log_context", default={})


@dataclass
class LogEvent:
    """Structured log event with metadata."""
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    level: str = "INFO"
    message: str = ""
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    service_name: str = "aurum-api"
    component: str = ""
    operation: str = ""
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    error: Optional[str] = None
    error_type: Optional[str] = None
    stack_trace: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert log event to dictionary."""
        result = {
            "timestamp": self.timestamp,
            "level": self.level,
            "message": self.message,
            "service_name": self.service_name,
        }

        # Add optional fields if present
        for field in ["request_id", "correlation_id", "tenant_id", "user_id", "session_id",
                     "component", "operation", "duration_ms", "status_code",
                     "error", "error_type", "stack_trace"]:
            value = getattr(self, field)
            if value is not None:
                result[field] = value

        # Add metadata
        if self.metadata:
            result["metadata"] = self.metadata

        return result

    def to_json(self) -> str:
        """Convert log event to JSON string."""
        return json.dumps(self.to_dict(), default=str)


class StructuredLogger:
    """Enhanced structured logger with context awareness."""

    def __init__(self, name: str = "aurum"):
        self.name = name
        self.logger = logging.getLogger(name)
        self._setup_json_formatter()

    def _setup_json_formatter(self):
        """Set up JSON formatter for structured logging."""
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

    def _create_log_event(
        self,
        level: str,
        message: str,
        **kwargs
    ) -> LogEvent:
        """Create a structured log event."""
        return LogEvent(
            level=level,
            message=message,
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
            service_name=self.name,
            **kwargs
        )

    def _log(self, level: str, message: str, **kwargs):
        """Log a structured message."""
        event = self._create_log_event(level, message, **kwargs)
        log_message = event.to_json()
        self.logger.log(getattr(logging, level), log_message)

    def info(self, message: str, **kwargs):
        """Log info level message."""
        self._log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning level message."""
        self._log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error level message."""
        self._log("ERROR", message, **kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug level message."""
        self._log("DEBUG", message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical level message."""
        self._log("CRITICAL", message, **kwargs)


# Global logger instance
_logger = StructuredLogger()


def get_logger(name: str = "aurum") -> StructuredLogger:
    """Get a structured logger instance."""
    return StructuredLogger(name)




def log_api_request(
    logger: StructuredLogger,
    endpoint: str,
    method: str,
    status_code: int,
    duration_ms: float,
    **kwargs
):
    """Log API request with structured data."""
    logger.info(
        f"{method} {endpoint} completed",
        component="api",
        operation=f"{method}_{endpoint.replace('/', '_')}",
        status_code=status_code,
        duration_ms=round(duration_ms, 2),
        **kwargs
    )


def log_database_operation(
    logger: StructuredLogger,
    operation: str,
    table: str = "",
    duration_ms: float = 0,
    rows_affected: Optional[int] = None,
    **kwargs
):
    """Log database operation with structured data."""
    logger.info(
        f"Database operation: {operation}",
        component="database",
        operation=operation,
        duration_ms=round(duration_ms, 2),
        table=table,
        **kwargs
    )


def log_cache_operation(
    logger: StructuredLogger,
    operation: str,
    cache_type: str = "memory",
    key: str = "",
    hit: bool = False,
    duration_ms: float = 0,
    **kwargs
):
    """Log cache operation with structured data."""
    logger.info(
        f"Cache {operation}: {'hit' if hit else 'miss'}",
        component="cache",
        operation=operation,
        cache_type=cache_type,
        cache_hit=hit,
        duration_ms=round(duration_ms, 2),
        **kwargs
    )


def log_error(
    logger: StructuredLogger,
    error: Exception,
    operation: str = "",
    **kwargs
):
    """Log error with structured context."""
    logger.error(
        f"Error in {operation}: {str(error)}",
        component="error",
        operation=operation,
        error=str(error),
        error_type=error.__class__.__name__,
        stack_trace=traceback.format_exc(),
        **kwargs
    )


@contextmanager
def log_operation(
    logger: StructuredLogger,
    operation: str,
    component: str = "",
    **context
):
    """Context manager for logging operation start/end with duration."""
    start_time = time.perf_counter()

    # Log operation start
    logger.info(
        f"Starting {operation}",
        component=component,
        operation=operation,
        **context
    )

    try:
        yield
    except Exception as e:
        # Log operation failure
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.error(
            f"Failed {operation}",
            component=component,
            operation=operation,
            duration_ms=round(duration_ms, 2),
            error=str(e),
            error_type=e.__class__.__name__,
            **context
        )
        raise
    else:
        # Log operation success
        duration_ms = (time.perf_counter() - start_time) * 1000
        logger.info(
            f"Completed {operation}",
            component=component,
            operation=operation,
            duration_ms=round(duration_ms, 2),
            **context
        )


def log_performance_metric(
    logger: StructuredLogger,
    metric_name: str,
    value: float,
    unit: str = "ms",
    **context
):
    """Log a performance metric."""
    logger.info(
        f"Performance: {metric_name}",
        component="performance",
        operation="metric",
        metric_name=metric_name,
        metric_value=value,
        metric_unit=unit,
        **context
    )


# Enhanced middleware for automatic structured logging
class StructuredLoggingMiddleware:
    """Middleware for automatic structured logging of HTTP requests."""

    def __init__(self, app):
        self.app = app
        self.logger = get_logger("aurum.api.access")

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract request information
        request = scope["method"] + " " + scope["path"]

        # Start timing
        start_time = time.perf_counter()

        # Collect response info
        status_code = 500
        response_size = 0

        async def send_wrapper(message):
            nonlocal status_code, response_size
            if message["type"] == "http.response.start":
                status_code = message["status"]
            elif message["type"] == "http.response.body":
                response_size = len(message.get("body", b""))

            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception as e:
            # Log error with full context
            duration_ms = (time.perf_counter() - start_time) * 1000

            self.logger.error(
                "HTTP request failed",
                component="http",
                operation="request",
                method=scope["method"],
                path=scope["path"],
                status_code=500,
                duration_ms=round(duration_ms, 2),
                error=str(e),
                error_type=e.__class__.__name__,
            )
            raise
        else:
            # Log successful request
            duration_ms = (time.perf_counter() - start_time) * 1000

            self.logger.info(
                "HTTP request completed",
                component="http",
                operation="request",
                method=scope["method"],
                path=scope["path"],
                status_code=status_code,
                duration_ms=round(duration_ms, 2),
                response_size=response_size,
            )


# Convenience functions for common logging patterns
def log_api_performance(
    logger: StructuredLogger,
    endpoint: str,
    method: str,
    duration_ms: float,
    status_code: int,
    cache_hit: Optional[bool] = None,
    tenant_id: Optional[str] = None,
    **kwargs
):
    """Log API performance with structured data."""
    extra_data = {
        "component": "api",
        "operation": f"{method}_{endpoint.replace('/', '_')}",
        "endpoint": endpoint,
        "method": method,
        "status_code": status_code,
        "duration_ms": round(duration_ms, 2),
    }

    if cache_hit is not None:
        extra_data["cache_hit"] = cache_hit

    if tenant_id:
        extra_data["tenant_id"] = tenant_id

    extra_data.update(kwargs)

    logger.info(
        f"API request: {method} {endpoint}",
        **extra_data
    )


def log_scenario_operation(
    logger: StructuredLogger,
    operation: str,
    scenario_id: str,
    duration_ms: float,
    tenant_id: Optional[str] = None,
    run_id: Optional[str] = None,
    **kwargs
):
    """Log scenario-related operations."""
    logger.info(
        f"Scenario {operation}",
        component="scenario",
        operation=operation,
        scenario_id=scenario_id,
        duration_ms=round(duration_ms, 2),
        tenant_id=tenant_id,
        run_id=run_id,
        **kwargs
    )


def log_rate_limit_event(
    logger: StructuredLogger,
    tenant_id: str,
    endpoint: str,
    allowed: bool,
    retry_after: Optional[float] = None,
    **kwargs
):
    """Log rate limiting events."""
    logger.info(
        f"Rate limit {'allowed' if allowed else 'blocked'}",
        component="rate_limit",
        operation="rate_limit_check",
        tenant_id=tenant_id,
        endpoint=endpoint,
        rate_limit_allowed=allowed,
        retry_after=retry_after,
        **kwargs
    )


def log_circuit_breaker_event(
    logger: StructuredLogger,
    state: str,
    operation: str,
    **kwargs
):
    """Log circuit breaker state changes."""
    logger.warning(
        f"Circuit breaker {state}: {operation}",
        component="circuit_breaker",
        operation=operation,
        circuit_breaker_state=state,
        **kwargs
    )


def log_database_connection_event(
    logger: StructuredLogger,
    pool_name: str,
    event_type: str,
    **kwargs
):
    """Log database connection pool events."""
    logger.info(
        f"Database pool {event_type}",
        component="database",
        operation="connection_pool",
        pool_name=pool_name,
        event_type=event_type,
        **kwargs
    )
