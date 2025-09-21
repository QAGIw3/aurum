"""Structured logging system with context awareness and correlation."""

from __future__ import annotations

import asyncio
import logging
import sys
import traceback
from typing import Any, Dict, Optional

from ..telemetry.context import get_request_id


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_entry = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S.%fZ"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "request_id": getattr(record, "request_id", None),
            "trace_id": getattr(record, "trace_id", None),
            "span_id": getattr(record, "span_id", None),
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
            log_entry["stack_trace"] = traceback.format_exception(*record.exc_info)

        # Add extra fields
        for key, value in record.__dict__.items():
            if key not in {
                "name", "msg", "args", "levelname", "levelno", "pathname",
                "filename", "module", "exc_info", "exc_text", "stack_info",
                "lineno", "funcName", "created", "msecs", "relativeCreated",
                "thread", "threadName", "processName", "process", "message",
                "request_id", "trace_id", "span_id"
            } and not key.startswith("_"):
                log_entry[key] = value

        return json.dumps(log_entry, default=str)


class ContextFilter(logging.Filter):
    """Filter that adds context information to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Add context information to log record."""
        try:
            # Add request ID from context
            request_id = get_request_id()
            if request_id:
                record.request_id = request_id

            # Add trace information if available
            try:
                from .tracing import get_current_trace_id, get_current_span_id
                trace_id = asyncio.create_task(get_current_trace_id())
                span_id = asyncio.create_task(get_current_span_id())

                # Since these are async, we'll set them if available
                # In practice, this would be handled differently
                if hasattr(record, "trace_id"):
                    record.trace_id = trace_id
                if hasattr(record, "span_id"):
                    record.span_id = span_id

            except Exception:
                pass  # Ignore tracing errors in logging

        except Exception:
            pass  # Ignore context errors in logging

        return True


class ObservabilityLogger:
    """Enhanced logger with observability features."""

    def __init__(self, name: str, level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Avoid duplicate handlers
        if not self.logger.handlers:
            # Console handler for development
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)

            # JSON formatter for structured logs
            formatter = JsonFormatter()
            console_handler.setFormatter(formatter)

            # Context filter
            context_filter = ContextFilter()
            console_handler.addFilter(context_filter)

            self.logger.addHandler(console_handler)

    def debug(self, msg: str, *args, **kwargs) -> None:
        """Log debug message with context."""
        self.logger.debug(msg, *args, extra=self._extract_extra(kwargs))

    def info(self, msg: str, *args, **kwargs) -> None:
        """Log info message with context."""
        self.logger.info(msg, *args, extra=self._extract_extra(kwargs))

    def warning(self, msg: str, *args, **kwargs) -> None:
        """Log warning message with context."""
        self.logger.warning(msg, *args, extra=self._extract_extra(kwargs))

    def error(self, msg: str, *args, **kwargs) -> None:
        """Log error message with context."""
        self.logger.error(msg, *args, extra=self._extract_extra(kwargs))

    def exception(self, msg: str, *args, **kwargs) -> None:
        """Log exception message with context."""
        self.logger.exception(msg, *args, extra=self._extract_extra(kwargs))

    def _extract_extra(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract extra fields for logging."""
        extra = {}

        # Extract known fields
        for key in ["request_id", "trace_id", "span_id", "user_id", "tenant_id",
                   "component", "operation", "duration", "error_code", "metadata"]:
            if key in kwargs:
                extra[key] = kwargs[key]

        # Add any remaining kwargs as metadata
        metadata = kwargs.get("metadata", {})
        for key, value in kwargs.items():
            if key not in ["request_id", "trace_id", "span_id", "user_id", "tenant_id",
                          "component", "operation", "duration", "error_code", "metadata"]:
                metadata[key] = value

        if metadata:
            extra["metadata"] = metadata

        return extra


# Convenience functions for common logging scenarios
def log_api_request(
    logger: ObservabilityLogger,
    endpoint: str,
    method: str,
    status_code: int,
    duration: float,
    **kwargs
) -> None:
    """Log API request with structured data."""
    logger.info(
        f"API Request: {method} {endpoint}",
        component="api",
        operation="request",
        endpoint=endpoint,
        method=method,
        status_code=status_code,
        duration=duration,
        **kwargs
    )


def log_database_operation(
    logger: ObservabilityLogger,
    operation: str,
    table: str,
    duration: float,
    **kwargs
) -> None:
    """Log database operation with structured data."""
    logger.info(
        f"Database Operation: {operation} on {table}",
        component="database",
        operation=operation,
        table=table,
        duration=duration,
        **kwargs
    )


def log_cache_operation(
    logger: ObservabilityLogger,
    operation: str,
    cache_type: str,
    key: str,
    hit: bool,
    duration: float,
    **kwargs
) -> None:
    """Log cache operation with structured data."""
    logger.info(
        f"Cache {operation}: {key}",
        component="cache",
        operation=operation,
        cache_type=cache_type,
        cache_key=key,
        cache_hit=hit,
        duration=duration,
        **kwargs
    )


def log_external_call(
    logger: ObservabilityLogger,
    service: str,
    operation: str,
    duration: float,
    success: bool,
    **kwargs
) -> None:
    """Log external service call with structured data."""
    log_level = "info" if success else "warning"
    getattr(logger, log_level)(
        f"External call to {service}: {operation}",
        component="external",
        operation=operation,
        service=service,
        duration=duration,
        success=success,
        **kwargs
    )


def log_error(
    logger: ObservabilityLogger,
    error: Exception,
    context: str = "",
    **kwargs
) -> None:
    """Log error with context and structured data."""
    logger.error(
        f"Error in {context}: {str(error)}",
        component="error",
        operation=context,
        error_type=type(error).__name__,
        error_message=str(error),
        **kwargs
    )


def log_performance_issue(
    logger: ObservabilityLogger,
    operation: str,
    duration: float,
    threshold: float,
    **kwargs
) -> None:
    """Log performance issue when operation exceeds threshold."""
    logger.warning(
        f"Performance issue in {operation}: {duration".3f"}s > {threshold".3f"}s",
        component="performance",
        operation=operation,
        duration=duration,
        threshold=threshold,
        **kwargs
    )


def log_security_event(
    logger: ObservabilityLogger,
    event: str,
    user_id: str = "",
    tenant_id: str = "",
    **kwargs
) -> None:
    """Log security event with structured data."""
    logger.warning(
        f"Security Event: {event}",
        component="security",
        operation=event,
        user_id=user_id,
        tenant_id=tenant_id,
        **kwargs
    )


def log_business_metric(
    logger: ObservabilityLogger,
    metric: str,
    value: float,
    **kwargs
) -> None:
    """Log business metric with structured data."""
    logger.info(
        f"Business Metric: {metric} = {value}",
        component="business",
        operation="metric",
        metric_name=metric,
        metric_value=value,
        **kwargs
    )


# Global logger instances
_api_logger = ObservabilityLogger("aurum.api")
_database_logger = ObservabilityLogger("aurum.database")
_cache_logger = ObservabilityLogger("aurum.cache")
_performance_logger = ObservabilityLogger("aurum.performance")
_security_logger = ObservabilityLogger("aurum.security")
_business_logger = ObservabilityLogger("aurum.business")


def get_api_logger() -> ObservabilityLogger:
    """Get the API logger instance."""
    return _api_logger


def get_database_logger() -> ObservabilityLogger:
    """Get the database logger instance."""
    return _database_logger


def get_cache_logger() -> ObservabilityLogger:
    """Get the cache logger instance."""
    return _cache_logger


def get_performance_logger() -> ObservabilityLogger:
    """Get the performance logger instance."""
    return _performance_logger


def get_security_logger() -> ObservabilityLogger:
    """Get the security logger instance."""
    return _security_logger


def get_business_logger() -> ObservabilityLogger:
    """Get the business logger instance."""
    return _business_logger


def get_logger(name: str) -> ObservabilityLogger:
    """Get a logger instance by name."""
    return ObservabilityLogger(name)
