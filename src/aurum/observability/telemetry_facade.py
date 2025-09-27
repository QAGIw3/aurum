"""Comprehensive telemetry facade for unified observability.

This module provides a single, DRY interface for all telemetry operations:
- Structured logging with consistent context propagation
- Metrics collection with standardized naming and tagging
- Distributed tracing with automatic correlation
- Request ID and correlation ID management
- Performance profiling and monitoring
- Alert management and incident tracking

Features:
- Context-aware logging with automatic field population
- Hierarchical metrics with proper aggregation
- End-to-end tracing with correlation IDs
- Request lifecycle tracking
- Performance monitoring with SLO enforcement
- Alert correlation and deduplication
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, Awaitable, TypeVar, Generic
from functools import wraps

from .metrics import get_metrics_client, MetricType
from .logging import get_logger as get_base_logger
from .tracing import get_trace_collector
from .slo_monitor import SLOMonitor, SLOStatus, SLOType
from .alert_manager import AlertManager
from ..telemetry.context import (
    get_request_id,
    set_request_id,
    get_correlation_id,
    set_correlation_id,
    get_tenant_id,
    set_tenant_id,
    get_user_id,
    set_user_id,
    get_session_id,
    set_session_id,
    log_structured
)

T = TypeVar('T')
F = TypeVar('F', bound=Callable[..., Any])


class TelemetryLevel(str, Enum):
    """Telemetry collection levels."""
    MINIMAL = "minimal"      # Basic request/response logging
    STANDARD = "standard"    # Standard metrics and tracing
    DETAILED = "detailed"    # Detailed performance profiling
    DEBUG = "debug"         # Full debugging information


class MetricCategory(str, Enum):
    """Categories for metrics organization."""
    PERFORMANCE = "performance"
    BUSINESS = "business"
    RELIABILITY = "reliability"
    SECURITY = "security"
    RESOURCE = "resource"


@dataclass
class TelemetryContext:
    """Rich context for telemetry operations."""

    # Request identifiers
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    session_id: Optional[str] = None

    # Request metadata
    operation: Optional[str] = None
    endpoint: Optional[str] = None
    method: Optional[str] = None
    user_agent: Optional[str] = None
    client_ip: Optional[str] = None

    # Business context
    service: str = "aurum"
    component: Optional[str] = None
    feature: Optional[str] = None
    scenario_id: Optional[str] = None
    scenario_run_id: Optional[str] = None

    # Performance context
    start_time: Optional[datetime] = None
    duration_ms: Optional[float] = None

    # Custom fields
    custom_fields: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_request(cls, request) -> 'TelemetryContext':
        """Create context from FastAPI request."""
        # Extract headers
        headers = dict(request.headers) if hasattr(request, 'headers') else {}

        # Get identifiers from context or headers
        request_id = get_request_id() or headers.get('x-request-id')
        correlation_id = get_correlation_id() or headers.get('x-correlation-id')
        tenant_id = get_tenant_id() or headers.get('x-tenant-id')
        user_id = get_user_id() or headers.get('x-user-id')
        session_id = get_session_id() or headers.get('x-session-id')

        # Extract request info
        operation = getattr(request, 'url', None)
        if operation and hasattr(operation, 'path'):
            endpoint = operation.path
        else:
            endpoint = getattr(request, 'path', None)

        method = getattr(request, 'method', None)
        user_agent = headers.get('user-agent')
        client_ip = getattr(request, 'client', None)
        if client_ip and hasattr(client_ip, 'host'):
            client_ip = client_ip.host

        return cls(
            request_id=request_id,
            correlation_id=correlation_id,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            operation=str(operation) if operation else None,
            endpoint=endpoint,
            method=method,
            user_agent=user_agent,
            client_ip=client_ip,
            start_time=datetime.utcnow()
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for logging."""
        result = {
            "service": self.service,
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Add identifiers if present
        if self.request_id:
            result["request_id"] = self.request_id
        if self.correlation_id:
            result["correlation_id"] = self.correlation_id
        if self.tenant_id:
            result["tenant_id"] = self.tenant_id
        if self.user_id:
            result["user_id"] = self.user_id
        if self.session_id:
            result["session_id"] = self.session_id

        # Add request metadata
        if self.operation:
            result["operation"] = self.operation
        if self.endpoint:
            result["endpoint"] = self.endpoint
        if self.method:
            result["method"] = self.method
        if self.component:
            result["component"] = self.component
        if self.feature:
            result["feature"] = self.feature
        if self.scenario_id:
            result["scenario_id"] = self.scenario_id
        if self.scenario_run_id:
            result["scenario_run_id"] = self.scenario_run_id

        # Add performance data
        if self.duration_ms is not None:
            result["duration_ms"] = self.duration_ms

        # Add custom fields
        result.update(self.custom_fields)

        return result


class TelemetryEvent:
    """Structured telemetry event."""

    def __init__(
        self,
        level: str,
        message: str,
        context: TelemetryContext,
        category: str = "general",
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Initialize telemetry event.

        Args:
            level: Log level (debug, info, warning, error, critical)
            message: Event message
            context: Telemetry context
            category: Event category
            metadata: Additional metadata
        """
        self.level = level
        self.message = message
        self.context = context
        self.category = category
        self.metadata = metadata or {}
        self.timestamp = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary."""
        result = self.context.to_dict()
        result.update({
            "level": self.level,
            "message": self.message,
            "category": self.category,
            "event_timestamp": self.timestamp.isoformat()
        })
        result.update(self.metadata)
        return result


class TelemetryFacade:
    """Unified facade for all telemetry operations."""

    def __init__(
        self,
        service_name: str = "aurum",
        telemetry_level: TelemetryLevel = TelemetryLevel.STANDARD,
        enable_metrics: bool = True,
        enable_tracing: bool = True,
        enable_slo_monitoring: bool = True
    ):
        """Initialize telemetry facade.

        Args:
            service_name: Name of the service
            telemetry_level: Level of telemetry collection
            enable_metrics: Enable metrics collection
            enable_tracing: Enable distributed tracing
            enable_slo_monitoring: Enable SLO monitoring
        """
        self.service_name = service_name
        self.telemetry_level = telemetry_level
        self.enable_metrics = enable_metrics
        self.enable_tracing = enable_tracing
        self.enable_slo_monitoring = enable_slo_monitoring

        # Get underlying clients
        self.metrics_client = get_metrics_client() if enable_metrics else None
        self.logger = get_base_logger(f"{service_name}.telemetry")
        self.trace_collector = get_trace_collector() if enable_tracing else None

        # SLO monitoring
        self.slo_monitor = None
        if enable_slo_monitoring:
            try:
                self.slo_monitor = SLOMonitor()
            except Exception:
                self.logger.warning("SLO monitoring initialization failed")

        # Alert management
        self.alert_manager = AlertManager()

        # Request tracking
        self._active_requests: Dict[str, TelemetryContext] = {}
        self._context_stack: List[TelemetryContext] = []

        self.logger.info("Telemetry facade initialized",
                        service=service_name,
                        level=telemetry_level.value,
                        metrics=enable_metrics,
                        tracing=enable_tracing,
                        slo_monitoring=enable_slo_monitoring)

    def create_context(
        self,
        request_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        operation: Optional[str] = None,
        component: Optional[str] = None,
        **kwargs
    ) -> TelemetryContext:
        """Create a new telemetry context.

        Args:
            request_id: Request identifier
            correlation_id: Correlation identifier
            tenant_id: Tenant identifier
            user_id: User identifier
            session_id: Session identifier
            operation: Operation name
            component: Component name
            **kwargs: Additional context fields

        Returns:
            New telemetry context
        """
        # Generate IDs if not provided
        if not request_id:
            request_id = str(uuid.uuid4())
        if not correlation_id:
            correlation_id = request_id  # Use request_id as correlation_id by default

        return TelemetryContext(
            request_id=request_id,
            correlation_id=correlation_id,
            tenant_id=tenant_id,
            user_id=user_id,
            session_id=session_id,
            operation=operation,
            component=component,
            service=self.service_name,
            custom_fields=kwargs
        )

    @contextmanager
    def request_context(
        self,
        context: Optional[TelemetryContext] = None,
        **context_kwargs
    ):
        """Context manager for request-scoped telemetry.

        Args:
            context: Optional telemetry context
            **context_kwargs: Context creation arguments
        """
        if context is None:
            context = self.create_context(**context_kwargs)

        # Set context variables
        request_token = set_request_id(context.request_id)
        correlation_token = set_correlation_id(context.correlation_id)
        tenant_token = set_tenant_id(context.tenant_id) if context.tenant_id else None
        user_token = set_user_id(context.user_id) if context.user_id else None
        session_token = set_session_id(context.session_id) if context.session_id else None

        # Track active request
        self._active_requests[context.request_id] = context

        try:
            # Push to context stack
            self._context_stack.append(context)
            yield context
        finally:
            # Pop from context stack
            if self._context_stack:
                self._context_stack.pop()

            # Clean up active request
            self._active_requests.pop(context.request_id, None)

            # Reset context variables
            request_token.reset()
            correlation_token.reset()
            if tenant_token:
                tenant_token.reset()
            if user_token:
                user_token.reset()
            if session_token:
                session_token.reset()

    @asynccontextmanager
    async def async_request_context(
        self,
        context: Optional[TelemetryContext] = None,
        **context_kwargs
    ):
        """Async context manager for request-scoped telemetry."""
        if context is None:
            context = self.create_context(**context_kwargs)

        # Set context variables
        request_token = set_request_id(context.request_id)
        correlation_token = set_correlation_id(context.correlation_id)
        tenant_token = set_tenant_id(context.tenant_id) if context.tenant_id else None
        user_token = set_user_id(context.user_id) if context.user_id else None
        session_token = set_session_id(context.session_id) if context.session_id else None

        # Track active request
        self._active_requests[context.request_id] = context

        try:
            # Push to context stack
            self._context_stack.append(context)
            yield context
        finally:
            # Pop from context stack
            if self._context_stack:
                self._context_stack.pop()

            # Clean up active request
            self._active_requests.pop(context.request_id, None)

            # Reset context variables
            request_token.reset()
            correlation_token.reset()
            if tenant_token:
                tenant_token.reset()
            if user_token:
                user_token.reset()
            if session_token:
                session_token.reset()

    def get_current_context(self) -> Optional[TelemetryContext]:
        """Get the current telemetry context."""
        return self._context_stack[-1] if self._context_stack else None

    def log(
        self,
        level: str,
        message: str,
        category: str = "general",
        context: Optional[TelemetryContext] = None,
        **metadata
    ) -> None:
        """Log a structured event.

        Args:
            level: Log level
            message: Log message
            category: Event category
            context: Optional telemetry context
            **metadata: Additional metadata
        """
        if context is None:
            context = self.get_current_context() or self.create_context()

        event = TelemetryEvent(level, message, context, category, metadata)

        # Log to structured logger
        log_structured(level, message, **event.to_dict())

        # Log to standard logger for backward compatibility
        log_message = f"[{context.request_id}] {message}" if context.request_id else message
        if level == "debug":
            self.logger.debug(log_message, extra=event.to_dict())
        elif level == "info":
            self.logger.info(log_message, extra=event.to_dict())
        elif level == "warning":
            self.logger.warning(log_message, extra=event.to_dict())
        elif level == "error":
            self.logger.error(log_message, extra=event.to_dict())
        elif level == "critical":
            self.logger.critical(log_message, extra=event.to_dict())

    def debug(self, message: str, category: str = "general", **metadata) -> None:
        """Log debug message."""
        self.log("debug", message, category, **metadata)

    def info(self, message: str, category: str = "general", **metadata) -> None:
        """Log info message."""
        self.log("info", message, category, **metadata)

    def warning(self, message: str, category: str = "general", **metadata) -> None:
        """Log warning message."""
        self.log("warning", message, category, **metadata)

    def error(self, message: str, category: str = "general", **metadata) -> None:
        """Log error message."""
        self.log("error", message, category, **metadata)

    def critical(self, message: str, category: str = "general", **metadata) -> None:
        """Log critical message."""
        self.log("critical", message, category, **metadata)

    def increment_counter(
        self,
        name: str,
        value: int = 1,
        category: MetricCategory = MetricCategory.PERFORMANCE,
        context: Optional[TelemetryContext] = None,
        **labels
    ) -> None:
        """Increment a counter metric.

        Args:
            name: Metric name
            value: Value to increment by
            category: Metric category
            context: Optional telemetry context
            **labels: Metric labels
        """
        if not self.enable_metrics or not self.metrics_client:
            return

        if context is None:
            context = self.get_current_context()

        # Add context labels
        metric_labels = dict(labels)
        if context:
            if context.tenant_id:
                metric_labels["tenant"] = context.tenant_id
            if context.component:
                metric_labels["component"] = context.component
            if context.endpoint:
                metric_labels["endpoint"] = context.endpoint

        # Add category label
        metric_labels["category"] = category.value

        try:
            self.metrics_client.counter(f"{self.service_name}_{name}", labels=metric_labels, value=value)
        except Exception as e:
            self.warning("Failed to increment counter", metric=name, error=str(e))

    def record_gauge(
        self,
        name: str,
        value: float,
        category: MetricCategory = MetricCategory.PERFORMANCE,
        context: Optional[TelemetryContext] = None,
        **labels
    ) -> None:
        """Record a gauge metric.

        Args:
            name: Metric name
            value: Gauge value
            category: Metric category
            context: Optional telemetry context
            **labels: Metric labels
        """
        if not self.enable_metrics or not self.metrics_client:
            return

        if context is None:
            context = self.get_current_context()

        # Add context labels
        metric_labels = dict(labels)
        if context:
            if context.tenant_id:
                metric_labels["tenant"] = context.tenant_id
            if context.component:
                metric_labels["component"] = context.component
            if context.endpoint:
                metric_labels["endpoint"] = context.endpoint

        # Add category label
        metric_labels["category"] = category.value

        try:
            self.metrics_client.gauge(f"{self.service_name}_{name}", labels=metric_labels, value=value)
        except Exception as e:
            self.warning("Failed to record gauge", metric=name, error=str(e))

    def record_histogram(
        self,
        name: str,
        value: float,
        category: MetricCategory = MetricCategory.PERFORMANCE,
        context: Optional[TelemetryContext] = None,
        **labels
    ) -> None:
        """Record a histogram metric.

        Args:
            name: Metric name
            value: Histogram value
            category: Metric category
            context: Optional telemetry context
            **labels: Metric labels
        """
        if not self.enable_metrics or not self.metrics_client:
            return

        if context is None:
            context = self.get_current_context()

        # Add context labels
        metric_labels = dict(labels)
        if context:
            if context.tenant_id:
                metric_labels["tenant"] = context.tenant_id
            if context.component:
                metric_labels["component"] = context.component
            if context.endpoint:
                metric_labels["endpoint"] = context.endpoint

        # Add category label
        metric_labels["category"] = category.value

        try:
            self.metrics_client.histogram(f"{self.service_name}_{name}", labels=metric_labels, value=value)
        except Exception as e:
            self.warning("Failed to record histogram", metric=name, error=str(e))

    @contextmanager
    def measure_duration(
        self,
        operation: str,
        category: MetricCategory = MetricCategory.PERFORMANCE,
        context: Optional[TelemetryContext] = None
    ):
        """Context manager to measure operation duration.

        Args:
            operation: Operation name
            category: Metric category
            context: Optional telemetry context
        """
        if context is None:
            context = self.get_current_context()

        start_time = time.time()

        try:
            yield
        finally:
            duration_ms = (time.time() - start_time) * 1000

            # Record duration metric
            self.record_histogram(
                "operation_duration",
                duration_ms,
                category=category,
                context=context,
                operation=operation
            )

            # Log performance info
            if duration_ms > 1000:  # Log slow operations
                self.warning(
                    "Slow operation detected",
                    operation=operation,
                    duration_ms=duration_ms,
                    category="performance"
                )

    def start_span(
        self,
        name: str,
        context: Optional[TelemetryContext] = None,
        **attributes
    ) -> Any:
        """Start a tracing span.

        Args:
            name: Span name
            context: Optional telemetry context
            **attributes: Span attributes

        Returns:
            Span object (implementation dependent)
        """
        if not self.enable_tracing or not self.trace_collector:
            return None

        if context is None:
            context = self.get_current_context()

        # Prepare span attributes
        span_attributes = dict(attributes)
        if context:
            if context.request_id:
                span_attributes["request_id"] = context.request_id
            if context.correlation_id:
                span_attributes["correlation_id"] = context.correlation_id
            if context.tenant_id:
                span_attributes["tenant_id"] = context.tenant_id
            if context.user_id:
                span_attributes["user_id"] = context.user_id
            if context.operation:
                span_attributes["operation"] = context.operation
            if context.component:
                span_attributes["component"] = context.component

        try:
            return self.trace_collector.start_span(name, attributes=span_attributes)
        except Exception as e:
            self.warning("Failed to start span", span=name, error=str(e))
            return None

    def create_alert(
        self,
        title: str,
        message: str,
        severity: str = "medium",
        context: Optional[TelemetryContext] = None,
        **metadata
    ) -> None:
        """Create an alert.

        Args:
            title: Alert title
            message: Alert message
            severity: Alert severity
            context: Optional telemetry context
            **metadata: Additional alert metadata
        """
        if context is None:
            context = self.get_current_context() or self.create_context()

        try:
            self.alert_manager.create_alert(
                title=title,
                message=message,
                severity=severity,
                context=context.to_dict(),
                **metadata
            )
        except Exception as e:
            self.error("Failed to create alert", title=title, error=str(e))

    def check_slo(
        self,
        slo_name: str,
        value: float,
        context: Optional[TelemetryContext] = None
    ) -> Optional[SLOStatus]:
        """Check SLO compliance.

        Args:
            slo_name: SLO name
            value: Measured value
            context: Optional telemetry context

        Returns:
            SLO status if monitored, None otherwise
        """
        if not self.enable_slo_monitoring or not self.slo_monitor:
            return None

        if context is None:
            context = self.get_current_context()

        try:
            return self.slo_monitor.check_slo(slo_name, value, context.to_dict() if context else {})
        except Exception as e:
            self.warning("SLO check failed", slo=slo_name, error=str(e))
            return None

    def instrument_function(
        self,
        func: F,
        operation_name: Optional[str] = None,
        category: MetricCategory = MetricCategory.PERFORMANCE
    ) -> F:
        """Instrument a function with telemetry.

        Args:
            func: Function to instrument
            operation_name: Operation name (defaults to function name)
            category: Metric category

        Returns:
            Instrumented function
        """
        if operation_name is None:
            operation_name = getattr(func, '__name__', str(func))

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            context = self.get_current_context()

            with self.measure_duration(operation_name, category, context):
                try:
                    result = await func(*args, **kwargs)

                    # Record success metric
                    self.increment_counter(
                        "function_success",
                        category=category,
                        context=context,
                        function=operation_name
                    )

                    return result

                except Exception as e:
                    # Record error metric
                    self.increment_counter(
                        "function_error",
                        category=MetricCategory.RELIABILITY,
                        context=context,
                        function=operation_name,
                        error_type=type(e).__name__
                    )

                    # Log error with context
                    self.error(
                        "Function execution failed",
                        function=operation_name,
                        error=str(e),
                        error_type=type(e).__name__,
                        category="reliability"
                    )

                    raise

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            context = self.get_current_context()

            with self.measure_duration(operation_name, category, context):
                try:
                    result = func(*args, **kwargs)

                    # Record success metric
                    self.increment_counter(
                        "function_success",
                        category=category,
                        context=context,
                        function=operation_name
                    )

                    return result

                except Exception as e:
                    # Record error metric
                    self.increment_counter(
                        "function_error",
                        category=MetricCategory.RELIABILITY,
                        context=context,
                        function=operation_name,
                        error_type=type(e).__name__
                    )

                    # Log error with context
                    self.error(
                        "Function execution failed",
                        function=operation_name,
                        error=str(e),
                        error_type=type(e).__name__,
                        category="reliability"
                    )

                    raise

        # Return appropriate wrapper based on function type
        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore

    def get_request_summary(self, request_id: str) -> Optional[Dict[str, Any]]:
        """Get summary of request telemetry.

        Args:
            request_id: Request identifier

        Returns:
            Request summary or None if not found
        """
        context = self._active_requests.get(request_id)
        if not context:
            return None

        return {
            "request_id": context.request_id,
            "correlation_id": context.correlation_id,
            "tenant_id": context.tenant_id,
            "user_id": context.user_id,
            "operation": context.operation,
            "endpoint": context.endpoint,
            "method": context.method,
            "start_time": context.start_time.isoformat() if context.start_time else None,
            "duration_ms": context.duration_ms,
            "custom_fields": context.custom_fields
        }

    async def get_service_stats(self) -> Dict[str, Any]:
        """Get comprehensive service statistics.

        Returns:
            Service statistics
        """
        return {
            "service_name": self.service_name,
            "telemetry_level": self.telemetry_level.value,
            "active_requests": len(self._active_requests),
            "total_requests": len([c for c in self._active_requests.values()]),
            "features_enabled": {
                "metrics": self.enable_metrics,
                "tracing": self.enable_tracing,
                "slo_monitoring": self.enable_slo_monitoring
            }
        }


# Global telemetry facade instance
_telemetry_facade: Optional[TelemetryFacade] = None


def get_telemetry_facade(
    service_name: str = "aurum",
    telemetry_level: TelemetryLevel = TelemetryLevel.STANDARD
) -> TelemetryFacade:
    """Get the global telemetry facade instance.

    Args:
        service_name: Name of the service
        telemetry_level: Level of telemetry collection

    Returns:
        Telemetry facade instance
    """
    global _telemetry_facade

    if _telemetry_facade is None:
        _telemetry_facade = TelemetryFacade(
            service_name=service_name,
            telemetry_level=telemetry_level
        )

    return _telemetry_facade


# Convenience functions for common operations
def log_request_start(request, operation: Optional[str] = None) -> TelemetryContext:
    """Log the start of a request and return context."""
    telemetry = get_telemetry_facade()
    context = TelemetryContext.from_request(request)

    if operation:
        context.operation = operation

    telemetry.info(
        "Request started",
        operation=context.operation,
        endpoint=context.endpoint,
        method=context.method,
        category="request"
    )

    return context


def log_request_complete(context: TelemetryContext, status_code: int, duration_ms: float) -> None:
    """Log the completion of a request."""
    telemetry = get_telemetry_facade()

    context.duration_ms = duration_ms

    telemetry.info(
        "Request completed",
        status_code=status_code,
        duration_ms=duration_ms,
        operation=context.operation,
        category="request"
    )

    # Record metrics
    telemetry.increment_counter(
        "requests_total",
        category=MetricCategory.PERFORMANCE,
        status_code=str(status_code),
        method=context.method or "unknown"
    )

    telemetry.record_histogram(
        "request_duration",
        duration_ms,
        category=MetricCategory.PERFORMANCE,
        endpoint=context.endpoint or "unknown",
        method=context.method or "unknown"
    )


def log_error(
    message: str,
    error: Optional[Exception] = None,
    context: Optional[TelemetryContext] = None,
    **metadata
) -> None:
    """Log an error with full context."""
    telemetry = get_telemetry_facade()

    if error:
        metadata["error_type"] = type(error).__name__
        metadata["error_message"] = str(error)

    telemetry.error(message, category="reliability", **metadata)

    # Record error metric
    telemetry.increment_counter(
        "errors_total",
        category=MetricCategory.RELIABILITY,
        error_type=type(error).__name__ if error else "unknown"
    )
