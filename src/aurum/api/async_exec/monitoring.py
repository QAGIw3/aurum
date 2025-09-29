"""Monitoring helpers for async operations."""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

MetricReporter = Callable[[str, float, Dict[str, str]], Awaitable[None] | None]
LogEmitter = Callable[[str, Dict[str, Any]], None]
TraceFactory = Callable[[str, Dict[str, Any]], Any]


@dataclass(slots=True)
class MonitoringHooks:
    metric: Optional[MetricReporter] = None
    log: Optional[LogEmitter] = None
    trace: Optional[TraceFactory] = None


async def emit_metric(hooks: MonitoringHooks, name: str, value: float, labels: Dict[str, str]) -> None:
    if hooks.metric:
        maybe = hooks.metric(name, value, labels)
        if hasattr(maybe, "__await__"):
            await maybe  # pragma: no cover - depends on reporter implementation


def emit_log(hooks: MonitoringHooks, message: str, payload: Dict[str, Any]) -> None:
    if hooks.log:
        hooks.log(message, payload)


@asynccontextmanager
async def observe_async(
    *,
    hooks: MonitoringHooks,
    task_name: str,
    labels: Optional[Dict[str, str]] = None,
) -> Awaitable[None]:
    start = time.monotonic()
    labels_map = {"task": task_name, **(labels or {})}
    emit_log(hooks, "task.start", {"task": task_name, "labels": labels_map})
    await emit_metric(hooks, "async_task_inflight", 1.0, labels_map)
    if hooks.trace:
        span = hooks.trace(task_name, labels_map)
    else:  # pragma: no cover - trace optional
        span = None
    try:
        yield
    except Exception as exc:
        emit_log(hooks, "task.error", {"task": task_name, "error": str(exc)})
        await emit_metric(hooks, "async_task_failures_total", 1.0, labels_map)
        if span:
            span.finish(error=exc)
        raise
    else:
        duration = time.monotonic() - start
        await emit_metric(hooks, "async_task_duration_seconds", duration, labels_map)
        emit_log(hooks, "task.success", {"task": task_name, "duration": duration})
        if span:
            span.finish()
    finally:
        await emit_metric(hooks, "async_task_inflight", -1.0, labels_map)


__all__ = [
    "MonitoringHooks",
    "emit_log",
    "emit_metric",
    "observe_async",
]
