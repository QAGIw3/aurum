"""Instrumentation helpers for automatically adding observability to services."""

from __future__ import annotations

import functools
import time
from collections.abc import Awaitable
from typing import Callable, TypeVar

from ..telemetry.context import get_request_id
from .metrics import (
    increment_api_requests,
    increment_business_errors,
    increment_business_transactions,
    increment_cache_operations,
    increment_ingestion_records,
    increment_rate_limit_requests,
    increment_worker_errors,
    observe_api_latency,
    observe_business_value,
    observe_ingestion_latency,
    observe_query_duration,
    observe_worker_task_duration,
    set_worker_active_tasks,
)
from .tracing import trace_span

T = TypeVar("T")


def instrument_api_endpoint(endpoint: str, method: str = "GET"):
    """Decorator to instrument API endpoints with metrics and tracing."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()
            request_id = get_request_id()

            # Start tracing
            async with trace_span(
                name=f"{method} {endpoint}",
                span_type="api_endpoint",
                attributes={
                    "endpoint": endpoint,
                    "method": method,
                    "request_id": request_id,
                }
            ) as span:
                try:
                    # Record rate limit check (assuming it's done before this point)
                    await increment_rate_limit_requests(endpoint)

                    result = await func(*args, **kwargs)

                    # Record success metrics
                    duration = time.perf_counter() - start_time
                    await observe_api_latency(endpoint, method, duration)
                    await increment_api_requests(endpoint, method, 200)

                    span.set_attribute("status", "success")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    # Record error metrics
                    duration = time.perf_counter() - start_time
                    await observe_api_latency(endpoint, method, duration)
                    await increment_api_requests(endpoint, method, 500)

                    span.set_attribute("status", "error")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise

        return wrapper
    return decorator


def instrument_database_operation(operation: str, table: str = ""):
    """Decorator to instrument database operations."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()

            async with trace_span(
                name=f"db:{operation}",
                span_type="database",
                attributes={
                    "operation": operation,
                    "table": table,
                }
            ) as span:
                try:
                    result = await func(*args, **kwargs)

                    duration = time.perf_counter() - start_time
                    await observe_query_duration(operation, duration)

                    span.set_attribute("status", "success")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    duration = time.perf_counter() - start_time
                    await observe_query_duration(operation, duration)

                    span.set_attribute("status", "error")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise

        return wrapper
    return decorator


def instrument_cache_operation(cache_type: str = "memory"):
    """Decorator to instrument cache operations."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()

            # Extract operation name from function name
            operation = func.__name__.replace("cache_", "").replace("_", "")

            async with trace_span(
                name=f"cache:{operation}",
                span_type="cache",
                attributes={
                    "operation": operation,
                    "cache_type": cache_type,
                }
            ) as span:
                try:
                    result = await func(*args, **kwargs)

                    duration = time.perf_counter() - start_time
                    await increment_cache_operations(cache_type, operation, "hit")

                    span.set_attribute("status", "hit")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    duration = time.perf_counter() - start_time
                    await increment_cache_operations(cache_type, operation, "miss")

                    span.set_attribute("status", "miss")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise

        return wrapper
    return decorator


def instrument_worker_task(worker_type: str, task_type: str):
    """Decorator to instrument worker tasks."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()

            # Increment active tasks counter
            await set_worker_active_tasks(worker_type, task_type, 1)

            async with trace_span(
                name=f"worker:{worker_type}:{task_type}",
                span_type="worker_task",
                attributes={
                    "worker_type": worker_type,
                    "task_type": task_type,
                }
            ) as span:
                try:
                    result = await func(*args, **kwargs)

                    duration = time.perf_counter() - start_time
                    await observe_worker_task_duration(worker_type, task_type, "success", duration)

                    span.set_attribute("status", "success")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    duration = time.perf_counter() - start_time
                    await observe_worker_task_duration(worker_type, task_type, "error", duration)
                    await increment_worker_errors(worker_type, type(exc).__name__)

                    span.set_attribute("status", "error")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise
                finally:
                    # Decrement active tasks counter
                    await set_worker_active_tasks(worker_type, task_type, 0)

        return wrapper
    return decorator


def instrument_ingestion(source: str, dataset: str):
    """Decorator to instrument ingestion operations."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()

            async with trace_span(
                name=f"ingestion:{source}:{dataset}",
                span_type="ingestion",
                attributes={
                    "source": source,
                    "dataset": dataset,
                }
            ) as span:
                try:
                    result = await func(*args, **kwargs)

                    duration = time.perf_counter() - start_time
                    await observe_ingestion_latency(source, dataset, duration)
                    await increment_ingestion_records(source, dataset, "success")

                    span.set_attribute("status", "success")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    duration = time.perf_counter() - start_time
                    await observe_ingestion_latency(source, dataset, duration)
                    await increment_ingestion_records(source, dataset, "error")

                    span.set_attribute("status", "error")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise

        return wrapper
    return decorator


def instrument_external_call(service: str, operation: str):
    """Decorator to instrument external service calls."""
    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            start_time = time.perf_counter()

            async with trace_span(
                name=f"external:{service}:{operation}",
                span_type="external_call",
                attributes={
                    "service": service,
                    "operation": operation,
                }
            ) as span:
                try:
                    result = await func(*args, **kwargs)

                    duration = time.perf_counter() - start_time
                    span.set_attribute("status", "success")
                    span.set_attribute("duration_seconds", duration)

                    return result

                except Exception as exc:
                    duration = time.perf_counter() - start_time
                    span.set_attribute("status", "error")
                    span.set_attribute("error", str(exc))
                    span.set_attribute("error_type", type(exc).__name__)

                    raise

        return wrapper
    return decorator


# Context managers for manual instrumentation
class ApiEndpointTracer:
    """Context manager for manual API endpoint tracing."""

    def __init__(self, endpoint: str, method: str = "GET"):
        self.endpoint = endpoint
        self.method = method
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.perf_counter()
        await increment_rate_limit_requests(self.endpoint)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.perf_counter() - self.start_time
        status_code = 500 if exc_type else 200
        await observe_api_latency(self.endpoint, self.method, duration)
        await increment_api_requests(self.endpoint, self.method, status_code)


class DatabaseOperationTracer:
    """Context manager for manual database operation tracing."""

    def __init__(self, operation: str, table: str = ""):
        self.operation = operation
        self.table = table
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.perf_counter() - self.start_time
        await observe_query_duration(self.operation, duration)


class CacheOperationTracer:
    """Context manager for manual cache operation tracing."""

    def __init__(self, cache_type: str, operation: str):
        self.cache_type = cache_type
        self.operation = operation
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.perf_counter() - self.start_time
        result = "miss" if exc_type else "hit"
        await increment_cache_operations(self.cache_type, self.operation, result)


# Business metrics helpers
def record_business_transaction(transaction_type: str, entity: str, status: str = "success"):
    """Record a business transaction."""
    return increment_business_transactions(transaction_type, entity, status)


def record_business_value(metric_type: str, entity: str, value: float):
    """Record a business value metric."""
    return observe_business_value(metric_type, entity, value)


def record_business_error(operation: str, error_type: str):
    """Record a business error."""
    return increment_business_errors(operation, error_type)


__all__ = [
    "instrument_api_endpoint",
    "instrument_database_operation",
    "instrument_cache_operation",
    "instrument_worker_task",
    "instrument_ingestion",
    "instrument_external_call",
    "ApiEndpointTracer",
    "DatabaseOperationTracer",
    "CacheOperationTracer",
    "record_business_transaction",
    "record_business_value",
    "record_business_error",
]
