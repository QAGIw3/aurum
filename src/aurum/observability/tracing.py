"""Distributed tracing system for request correlation and performance analysis."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from ..telemetry.context import get_request_id


# Context variables for tracing
current_trace_id: ContextVar[Optional[str]] = ContextVar('current_trace_id', default=None)
current_span_id: ContextVar[Optional[str]] = ContextVar('current_span_id', default=None)
trace_attributes: ContextVar[Dict[str, Any]] = ContextVar('trace_attributes', default_factory=dict)


@dataclass
class Span:
    """Represents a single span in a trace."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    span_type: str = "internal"
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration: Optional[float] = None
    attributes: Dict[str, Any] = field(default_factory=dict)
    tags: Dict[str, str] = field(default_factory=dict)
    events: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "ok"  # ok, error, cancelled
    error_message: Optional[str] = None

    def finish(self, end_time: Optional[float] = None) -> None:
        """Mark span as finished and calculate duration."""
        if end_time is None:
            end_time = time.time()
        self.end_time = end_time
        self.duration = self.end_time - self.start_time

    def add_event(self, name: str, attributes: Optional[Dict[str, Any]] = None) -> None:
        """Add an event to the span."""
        self.events.append({
            "name": name,
            "timestamp": time.time(),
            "attributes": attributes or {}
        })

    def set_attribute(self, key: str, value: Any) -> None:
        """Set a span attribute."""
        self.attributes[key] = value

    def set_tag(self, key: str, value: str) -> None:
        """Set a span tag."""
        self.tags[key] = value


class TraceCollector:
    """Collects and manages distributed traces."""

    def __init__(self):
        self._active_traces: Dict[str, List[Span]] = {}
        self._completed_traces: Dict[str, List[Span]] = {}
        self._spans: Dict[str, Span] = {}
        self._lock = asyncio.Lock()

    async def start_span(
        self,
        name: str,
        span_type: str = "internal",
        attributes: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> Span:
        """Start a new span."""
        trace_id = current_trace_id.get()
        if trace_id is None:
            trace_id = str(uuid.uuid4())
            current_trace_id.set(trace_id)

        parent_span_id = current_span_id.get()
        span_id = str(uuid.uuid4())[:8]  # Short span ID for readability

        span = Span(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            name=name,
            span_type=span_type,
            attributes=attributes or {},
            tags=tags or {}
        )

        # Inherit attributes from parent context
        span.attributes.update(trace_attributes.get({}))
        span.attributes.update({
            "request_id": get_request_id(),
            "trace_id": trace_id,
            "span_id": span_id,
        })

        # Set as current span
        current_span_id.set(span_id)

        # Store span
        async with self._lock:
            if trace_id not in self._active_traces:
                self._active_traces[trace_id] = []
            self._active_traces[trace_id].append(span)
            self._spans[span_id] = span

        return span

    async def finish_span(self, span: Span) -> None:
        """Finish a span and record its duration."""
        span.finish()

        # Move from active to completed traces
        async with self._lock:
            if span.trace_id in self._active_traces:
                active_spans = self._active_traces[span.trace_id]
                if span in active_spans:
                    active_spans.remove(span)

                    if span.trace_id not in self._completed_traces:
                        self._completed_traces[span.trace_id] = []
                    self._completed_traces[span.trace_id].append(span)

                    # Clean up if trace is complete
                    if not active_spans:
                        del self._active_traces[span.trace_id]

    async def get_trace(self, trace_id: str) -> Optional[List[Span]]:
        """Get all spans for a trace."""
        async with self._lock:
            # Check both active and completed traces
            active_spans = self._active_traces.get(trace_id, [])
            completed_spans = self._completed_traces.get(trace_id, [])
            return active_spans + completed_spans

    async def get_active_traces(self) -> Dict[str, List[Span]]:
        """Get all active traces."""
        async with self._lock:
            return self._active_traces.copy()

    async def get_span(self, span_id: str) -> Optional[Span]:
        """Get a specific span."""
        async with self._lock:
            return self._spans.get(span_id)

    async def get_trace_summary(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Get a summary of a trace."""
        spans = await self.get_trace(trace_id)
        if not spans:
            return None

        # Sort spans by start time
        spans.sort(key=lambda x: x.start_time)

        total_duration = max(span.end_time or time.time() for span in spans) - min(span.start_time for span in spans)
        span_count = len(spans)
        error_count = sum(1 for span in spans if span.status == "error")

        return {
            "trace_id": trace_id,
            "span_count": span_count,
            "total_duration": total_duration,
            "error_count": error_count,
            "root_span": spans[0].name if spans else None,
            "status": "error" if error_count > 0 else "ok"
        }

    async def cleanup_old_traces(self, max_age_seconds: int = 3600) -> int:
        """Clean up old traces to prevent memory leaks."""
        current_time = time.time()
        traces_to_remove = []

        async with self._lock:
            # Find traces older than max_age_seconds
            for trace_id, spans in self._completed_traces.items():
                if spans and all(
                    (span.end_time or current_time) < (current_time - max_age_seconds)
                    for span in spans
                ):
                    traces_to_remove.append(trace_id)

            # Remove old traces
            for trace_id in traces_to_remove:
                del self._completed_traces[trace_id]

                # Also remove associated spans
                for span in list(self._spans.values()):
                    if span.trace_id == trace_id:
                        del self._spans[span.span_id]

        return len(traces_to_remove)


# Global trace collector
_trace_collector = TraceCollector()


def get_trace_collector() -> TraceCollector:
    """Get the global trace collector instance."""
    return _trace_collector


@asynccontextmanager
async def trace_span(
    name: str,
    span_type: str = "internal",
    attributes: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None
):
    """Context manager for creating and automatically finishing spans."""
    span = await _trace_collector.start_span(name, span_type, attributes, tags)
    try:
        yield span
    except Exception as exc:
        span.status = "error"
        span.error_message = str(exc)
        span.set_attribute("error", True)
        span.set_attribute("error_message", str(exc))
        raise
    finally:
        await _trace_collector.finish_span(span)


async def start_trace(
    name: str,
    span_type: str = "request",
    attributes: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None
) -> Span:
    """Start a new trace with a root span."""
    return await _trace_collector.start_span(name, span_type, attributes, tags)


async def get_current_trace_id() -> Optional[str]:
    """Get the current trace ID from context."""
    return current_trace_id.get()


async def get_current_span_id() -> Optional[str]:
    """Get the current span ID from context."""
    return current_span_id.get()


async def set_trace_attribute(key: str, value: Any) -> None:
    """Set a trace attribute that will be inherited by child spans."""
    attributes = trace_attributes.get({})
    attributes[key] = value
    trace_attributes.set(attributes)


async def get_trace_attributes() -> Dict[str, Any]:
    """Get all current trace attributes."""
    return trace_attributes.get({})


# Convenience functions for common tracing operations
async def trace_api_request(endpoint: str, method: str) -> Span:
    """Start tracing an API request."""
    return await _trace_collector.start_span(
        name=f"{method} {endpoint}",
        span_type="api_request",
        attributes={"endpoint": endpoint, "method": method},
        tags={"component": "api", "endpoint": endpoint}
    )


async def trace_database_operation(operation: str, table: str = "") -> Span:
    """Start tracing a database operation."""
    return await _trace_collector.start_span(
        name=f"db:{operation}",
        span_type="database",
        attributes={"operation": operation, "table": table},
        tags={"component": "database", "operation": operation}
    )


async def trace_cache_operation(operation: str, cache_type: str = "memory") -> Span:
    """Start tracing a cache operation."""
    return await _trace_collector.start_span(
        name=f"cache:{operation}",
        span_type="cache",
        attributes={"operation": operation, "cache_type": cache_type},
        tags={"component": "cache", "operation": operation}
    )


async def trace_external_call(service: str, operation: str) -> Span:
    """Start tracing an external service call."""
    return await _trace_collector.start_span(
        name=f"external:{service}:{operation}",
        span_type="external",
        attributes={"service": service, "operation": operation},
        tags={"component": "external", "service": service}
    )


async def trace_async_operation(operation: str, coroutine) -> Any:
    """Trace an async operation and return its result."""
    async with trace_span(f"async:{operation}", "async_operation") as span:
        try:
            result = await coroutine
            span.set_attribute("success", True)
            return result
        except Exception as exc:
            span.set_attribute("success", False)
            span.set_attribute("error", str(exc))
            raise


async def get_trace_report(trace_id: str) -> Optional[Dict[str, Any]]:
    """Generate a detailed report for a trace."""
    spans = await _trace_collector.get_trace(trace_id)
    if not spans:
        return None

    # Sort spans by start time
    spans.sort(key=lambda x: x.start_time)

    # Build trace tree
    root_spans = [span for span in spans if span.parent_span_id is None]
    child_spans = {span.parent_span_id: span for span in spans if span.parent_span_id}

    def build_span_tree(span: Span) -> Dict[str, Any]:
        children = []
        for child_span_id, child_span in child_spans.items():
            if child_span.parent_span_id == span.span_id:
                children.append(build_span_tree(child_span))

        return {
            "span_id": span.span_id,
            "name": span.name,
            "span_type": span.span_type,
            "start_time": span.start_time,
            "end_time": span.end_time,
            "duration": span.duration,
            "attributes": span.attributes,
            "tags": span.tags,
            "events": span.events,
            "status": span.status,
            "error_message": span.error_message,
            "children": children
        }

    trace_tree = [build_span_tree(root) for root in root_spans]

    return {
        "trace_id": trace_id,
        "span_count": len(spans),
        "root_spans": trace_tree,
        "total_duration": (
            max(span.end_time or time.time() for span in spans) -
            min(span.start_time for span in spans)
        ),
        "status": "error" if any(span.status == "error" for span in spans) else "ok"
    }
