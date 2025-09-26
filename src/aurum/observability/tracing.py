"""Distributed tracing system for request correlation and performance analysis."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..telemetry import OTEL_AVAILABLE, get_tracer
from ..telemetry.context import get_request_id

if OTEL_AVAILABLE:
    from opentelemetry.trace.status import Status, StatusCode


# Context variables for tracing
current_trace_id: ContextVar[Optional[str]] = ContextVar("current_trace_id", default=None)
current_span_id: ContextVar[Optional[str]] = ContextVar("current_span_id", default=None)
trace_attributes: ContextVar[Dict[str, Any]] = ContextVar("trace_attributes")


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
    _span_token: Token | None = field(default=None, init=False, repr=False, compare=False)
    _trace_token: Token | None = field(default=None, init=False, repr=False, compare=False)
    _attributes_token: Token | None = field(default=None, init=False, repr=False, compare=False)
    _otel_span: Optional[Any] = field(default=None, init=False, repr=False, compare=False)  # OpenTelemetry span

    def finish(self, end_time: Optional[float] = None) -> None:
        """Mark span as finished and calculate duration."""
        if end_time is None:
            end_time = time.time()
        self.end_time = end_time
        self.duration = self.end_time - self.start_time

        # Finish OpenTelemetry span if present
        if OTEL_AVAILABLE and self._otel_span is not None:
            try:
                # Set span attributes
                for key, value in self.attributes.items():
                    self._otel_span.set_attribute(key, str(value))

                # Set span tags
                for key, value in self.tags.items():
                    self._otel_span.set_attribute(f"tag.{key}", value)

                # Set span status based on our status
                if self.status == "error":
                    self._otel_span.set_status(Status(StatusCode.ERROR, self.error_message or "Unknown error"))
                elif self.status == "cancelled":
                    self._otel_span.set_status(Status(StatusCode.CANCELLED))
                else:
                    self._otel_span.set_status(Status(StatusCode.OK))

                # Add events
                for event in self.events:
                    self._otel_span.add_event(
                        event["name"],
                        attributes=event.get("attributes", {})
                    )

                self._otel_span.end()
            except Exception:
                # Silently handle OpenTelemetry errors to avoid breaking application
                pass

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
        trace_token: Token | None = None
        attrs_token: Token | None = None

        trace_id = current_trace_id.get()
        if trace_id is None:
            trace_id = str(uuid.uuid4())
            trace_token = current_trace_id.set(trace_id)
            inherited_attributes: Dict[str, Any] = {}
            attrs_token = trace_attributes.set(inherited_attributes)
        else:
            try:
                inherited_attributes = trace_attributes.get()
            except LookupError:
                inherited_attributes = {}

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
        span.attributes.update(inherited_attributes)
        span.attributes.update({
            "request_id": get_request_id(),
            "trace_id": trace_id,
            "span_id": span_id,
        })

        # Create OpenTelemetry span if available
        if OTEL_AVAILABLE:
            try:
                tracer = get_tracer("aurum")
                otel_span = tracer.start_span(
                    name=span.name,
                    attributes={
                        **span.attributes,
                        **span.tags,
                        "span.type": span.span_type,
                    }
                )
                span._otel_span = otel_span
            except Exception:
                # Silently handle OpenTelemetry errors
                pass

        # Track context tokens for restoration
        span._trace_token = trace_token
        span._attributes_token = attrs_token
        span._span_token = current_span_id.set(span_id)

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

        # Restore previous span context
        if span._span_token is not None:
            try:
                current_span_id.reset(span._span_token)
            except (LookupError, ValueError, RuntimeError):
                current_span_id.set(span.parent_span_id)
        else:
            current_span_id.set(span.parent_span_id)

        # Restore trace and attribute context for root spans
        if span.parent_span_id is None and span._trace_token is not None:
            try:
                current_trace_id.reset(span._trace_token)
            except (LookupError, ValueError, RuntimeError):
                current_trace_id.set(None)

        if span.parent_span_id is None and span._attributes_token is not None:
            try:
                trace_attributes.reset(span._attributes_token)
            except (LookupError, ValueError, RuntimeError):
                trace_attributes.set({})

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


def get_current_trace_id() -> Optional[str]:
    """Get the current trace ID from context."""
    return current_trace_id.get()


def get_current_span_id() -> Optional[str]:
    """Get the current span ID from context."""
    return current_span_id.get()


async def set_trace_attribute(key: str, value: Any) -> None:
    """Set a trace attribute that will be inherited by child spans."""
    try:
        attributes = trace_attributes.get()
        bound = True
    except LookupError:
        attributes = {}
        bound = False

    attributes[key] = value
    if not bound:
        trace_attributes.set(attributes)


async def get_trace_attributes() -> Dict[str, Any]:
    """Get all current trace attributes."""
    try:
        return trace_attributes.get()
    except LookupError:
        return {}


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
    root_spans.sort(key=lambda span: span.start_time)

    children_map: Dict[str, List[Span]] = defaultdict(list)
    for child in spans:
        if child.parent_span_id:
            children_map[child.parent_span_id].append(child)

    def build_span_tree(span: Span) -> Dict[str, Any]:
        children = [
            build_span_tree(child)
            for child in sorted(children_map.get(span.span_id, []), key=lambda c: c.start_time)
        ]

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
