"""Enhanced tracing for database operations and business logic.

This module provides enhanced tracing capabilities specifically for:
- Database operations with detailed query information
- Scenario storage operations with context
- Cache operations with hit/miss tracking
- Business logic operations with performance metrics
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from ..telemetry.context import get_request_id
from .tracing import trace_span, get_trace_collector


async def trace_database_query(
    operation: str,
    query: str = "",
    table: str = "",
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace a database query with detailed information."""
    async with trace_span(
        name=f"db:{operation}",
        span_type="database",
        attributes={
            "operation": operation,
            "query": query[:100] if query else "",  # Truncate long queries
            "table": table,
            "tenant_id": tenant_id or "",
            **attributes
        },
        tags={
            "component": "database",
            "operation": operation,
            "db.table": table,
        }
    ) as span:
        start_time = time.perf_counter()

        try:
            # This is a context manager - the actual query execution happens in the with block
            yield span
        except Exception as e:
            span.status = "error"
            span.error_message = str(e)
            span.set_attribute("error", True)
            span.set_attribute("error_message", str(e))
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            span.set_attribute("duration_ms", round(duration_ms, 2))


async def trace_scenario_operation(
    operation: str,
    scenario_id: str,
    tenant_id: Optional[str] = None,
    run_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace a scenario-related operation."""
    async with trace_span(
        name=f"scenario:{operation}",
        span_type="business_logic",
        attributes={
            "operation": operation,
            "scenario_id": scenario_id,
            "tenant_id": tenant_id or "",
            "run_id": run_id or "",
            **attributes
        },
        tags={
            "component": "scenario",
            "operation": operation,
            "business.scenario_id": scenario_id,
        }
    ) as span:
        start_time = time.perf_counter()

        try:
            yield span
        except Exception as e:
            span.status = "error"
            span.error_message = str(e)
            span.set_attribute("error", True)
            span.set_attribute("error_message", str(e))
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            span.set_attribute("duration_ms", round(duration_ms, 2))


async def trace_cache_operation(
    operation: str,
    cache_type: str = "memory",
    key: str = "",
    hit: Optional[bool] = None,
    **attributes
) -> Any:
    """Trace a cache operation."""
    async with trace_span(
        name=f"cache:{operation}",
        span_type="cache",
        attributes={
            "operation": operation,
            "cache_type": cache_type,
            "cache_key": key[:50] if key else "",  # Truncate long keys
            "cache_hit": hit,
            **attributes
        },
        tags={
            "component": "cache",
            "operation": operation,
            "cache.type": cache_type,
        }
    ) as span:
        start_time = time.perf_counter()

        try:
            yield span
        except Exception as e:
            span.status = "error"
            span.error_message = str(e)
            span.set_attribute("error", True)
            span.set_attribute("error_message", str(e))
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            span.set_attribute("duration_ms", round(duration_ms, 2))


async def trace_external_service_call(
    service: str,
    operation: str,
    endpoint: str = "",
    **attributes
) -> Any:
    """Trace an external service call."""
    async with trace_span(
        name=f"external:{service}:{operation}",
        span_type="external",
        attributes={
            "service": service,
            "operation": operation,
            "endpoint": endpoint,
            **attributes
        },
        tags={
            "component": "external",
            "service": service,
            "operation": operation,
        }
    ) as span:
        start_time = time.perf_counter()

        try:
            yield span
        except Exception as e:
            span.status = "error"
            span.error_message = str(e)
            span.set_attribute("error", True)
            span.set_attribute("error_message", str(e))
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            span.set_attribute("duration_ms", round(duration_ms, 2))


async def trace_business_operation(
    operation: str,
    entity_type: str = "",
    entity_id: str = "",
    **attributes
) -> Any:
    """Trace a business logic operation."""
    async with trace_span(
        name=f"business:{operation}",
        span_type="business_logic",
        attributes={
            "operation": operation,
            "entity_type": entity_type,
            "entity_id": entity_id,
            **attributes
        },
        tags={
            "component": "business",
            "operation": operation,
            "business.entity_type": entity_type,
        }
    ) as span:
        start_time = time.perf_counter()

        try:
            yield span
        except Exception as e:
            span.status = "error"
            span.error_message = str(e)
            span.set_attribute("error", True)
            span.set_attribute("error_message", str(e))
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            span.set_attribute("duration_ms", round(duration_ms, 2))


# Convenience functions for common operations
async def trace_scenario_creation(
    scenario_id: str,
    tenant_id: str,
    **attributes
) -> Any:
    """Trace scenario creation operation."""
    async with trace_scenario_operation(
        "create",
        scenario_id,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


async def trace_scenario_run_creation(
    scenario_id: str,
    run_id: str,
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace scenario run creation operation."""
    async with trace_scenario_operation(
        "run_create",
        scenario_id,
        tenant_id=tenant_id,
        run_id=run_id,
        **attributes
    ) as span:
        yield span


async def trace_scenario_query(
    scenario_id: str,
    operation: str = "get",
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace scenario query operation."""
    async with trace_scenario_operation(
        operation,
        scenario_id,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


# Database operation tracing functions
async def trace_db_insert(
    table: str,
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace database insert operation."""
    async with trace_database_query(
        "insert",
        table=table,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


async def trace_db_select(
    table: str,
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace database select operation."""
    async with trace_database_query(
        "select",
        table=table,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


async def trace_db_update(
    table: str,
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace database update operation."""
    async with trace_database_query(
        "update",
        table=table,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


async def trace_db_delete(
    table: str,
    tenant_id: Optional[str] = None,
    **attributes
) -> Any:
    """Trace database delete operation."""
    async with trace_database_query(
        "delete",
        table=table,
        tenant_id=tenant_id,
        **attributes
    ) as span:
        yield span


# Cache operation tracing functions
async def trace_cache_get(
    cache_type: str = "memory",
    key: str = "",
    **attributes
) -> Any:
    """Trace cache get operation."""
    async with trace_cache_operation(
        "get",
        cache_type=cache_type,
        key=key,
        **attributes
    ) as span:
        yield span


async def trace_cache_set(
    cache_type: str = "memory",
    key: str = "",
    **attributes
) -> Any:
    """Trace cache set operation."""
    async with trace_cache_operation(
        "set",
        cache_type=cache_type,
        key=key,
        **attributes
    ) as span:
        yield span


async def trace_cache_delete(
    cache_type: str = "memory",
    key: str = "",
    **attributes
) -> Any:
    """Trace cache delete operation."""
    async with trace_cache_operation(
        "delete",
        cache_type=cache_type,
        key=key,
        **attributes
    ) as span:
        yield span


# Performance tracking decorator
def trace_performance(operation_name: str, component: str = "performance"):
    """Decorator for tracing performance of async functions."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            async with trace_span(
                name=f"perf:{operation_name}",
                span_type="performance",
                attributes={
                    "operation": operation_name,
                    "function": func.__name__,
                },
                tags={
                    "component": component,
                    "operation": operation_name,
                }
            ) as span:
                start_time = time.perf_counter()

                try:
                    result = await func(*args, **kwargs)
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    span.set_attribute("duration_ms", round(duration_ms, 2))
                    span.set_attribute("success", True)
                    return result
                except Exception as e:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    span.set_attribute("duration_ms", round(duration_ms, 2))
                    span.set_attribute("success", False)
                    span.set_attribute("error", str(e))
                    span.status = "error"
                    span.error_message = str(e)
                    raise

        return wrapper
    return decorator


# Context manager for tracking operation metrics
class OperationTracker:
    """Track metrics for an operation with automatic span creation."""

    def __init__(
        self,
        operation: str,
        component: str,
        **attributes
    ):
        self.operation = operation
        self.component = component
        self.attributes = attributes
        self.start_time = 0
        self.span = None

    async def __aenter__(self):
        self.start_time = time.perf_counter()

        self.span = await get_trace_collector().start_span(
            name=f"{self.component}:{self.operation}",
            span_type=self.component,
            attributes=self.attributes,
            tags={"component": self.component, "operation": self.operation}
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration_ms = (time.perf_counter() - self.start_time) * 1000

        if self.span:
            self.span.set_attribute("duration_ms", round(duration_ms, 2))

            if exc_val:
                self.span.status = "error"
                self.span.error_message = str(exc_val)
                self.span.set_attribute("error", True)
                self.span.set_attribute("error_message", str(exc_val))

            await get_trace_collector().finish_span(self.span)

    def set_attribute(self, key: str, value: Any):
        """Set an attribute on the span."""
        if self.span:
            self.span.set_attribute(key, value)

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        """Add an event to the span."""
        if self.span:
            self.span.add_event(name, attributes)


async def track_operation(
    operation: str,
    component: str = "",
    **attributes
) -> OperationTracker:
    """Create an operation tracker context manager."""
    return OperationTracker(operation, component, **attributes)


# Enhanced error tracking with context
async def trace_error(
    error: Exception,
    operation: str = "",
    component: str = "",
    **context
):
    """Trace an error with full context."""
    span = await get_trace_collector().start_span(
        name=f"error:{operation}",
        span_type="error",
        attributes={
            "operation": operation,
            "component": component,
            "error": str(error),
            "error_type": error.__class__.__name__,
            **context
        },
        tags={
            "component": "error",
            "operation": operation,
            "error": "true"
        }
    )

    span.status = "error"
    span.error_message = str(error)
    span.set_attribute("stack_trace", str(error.__traceback__))

    await get_trace_collector().finish_span(span)
