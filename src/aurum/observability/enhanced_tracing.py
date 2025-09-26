"""Enhanced distributed tracing with W3C traceparent propagation and request correlation."""

from __future__ import annotations

import asyncio
import re
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from ..telemetry.context import get_request_id
from ..telemetry import get_tracer, OTEL_AVAILABLE

if OTEL_AVAILABLE:
    from opentelemetry import trace
    from opentelemetry.trace.status import Status, StatusCode
    from opentelemetry.trace.propagation.tracecontext import TraceContextPropagator
    from opentelemetry.propagate import inject, extract


# W3C TraceParent format: 00-{trace_id}-{span_id}-{trace_flags}
TRACEPARENT_REGEX = re.compile(
    r"^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$"
)

# Context variables for enhanced tracing
current_trace_id: ContextVar[Optional[str]] = ContextVar("current_trace_id", default=None)
current_span_id: ContextVar[Optional[str]] = ContextVar("current_span_id", default=None)
current_traceparent: ContextVar[Optional[str]] = ContextVar("current_traceparent", default=None)
trace_attributes: ContextVar[Dict[str, Any]] = ContextVar("trace_attributes", default_factory=dict)


@dataclass
class TraceContext:
    """W3C Trace Context for distributed tracing."""
    trace_id: str
    span_id: str
    trace_flags: str = "00"  # Default: sampled=0
    trace_state: Optional[str] = None

    @classmethod
    def from_traceparent(cls, traceparent: str) -> Optional['TraceContext']:
        """Parse W3C traceparent header."""
        match = TRACEPARENT_REGEX.match(traceparent)
        if match:
            version, trace_id, span_id, trace_flags = match.groups()
            return cls(trace_id=trace_id, span_id=span_id, trace_flags=trace_flags)
        return None

    def to_traceparent(self) -> str:
        """Generate W3C traceparent header."""
        return f"00-{self.trace_id}-{self.span_id}-{self.trace_flags}"

    def to_dict(self) -> Dict[str, str]:
        """Convert to dictionary for headers."""
        headers = {
            "traceparent": self.to_traceparent()
        }
        if self.trace_state:
            headers["tracestate"] = self.trace_state
        return headers

    @classmethod
    def generate_new(cls, parent_context: Optional['TraceContext'] = None) -> 'TraceContext':
        """Generate a new trace context."""
        if parent_context:
            # Child span - use same trace_id, new span_id
            return cls(
                trace_id=parent_context.trace_id,
                span_id=uuid.uuid4().hex[:16],
                trace_flags=parent_context.trace_flags
            )
        else:
            # Root span - new trace_id and span_id
            return cls(
                trace_id=uuid.uuid4().hex[:32],
                span_id=uuid.uuid4().hex[:16]
            )


@dataclass
class EnhancedSpan:
    """Enhanced span with correlation and propagation support."""
    trace_context: TraceContext
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

    # Context management
    _trace_token: Optional[Token] = None
    _span_token: Optional[Token] = None
    _traceparent_token: Optional[Token] = None
    _attributes_token: Optional[Token] = None

    # OpenTelemetry span
    _otel_span: Optional[Any] = None

    def finish(self, end_time: Optional[float] = None) -> None:
        """Mark span as finished."""
        if end_time is None:
            end_time = time.time()
        self.end_time = end_time
        self.duration = self.end_time - self.start_time

        # Finish OpenTelemetry span
        if OTEL_AVAILABLE and self._otel_span:
            try:
                # Set span attributes
                for key, value in self.attributes.items():
                    self._otel_span.set_attribute(key, str(value))
                for key, value in self.tags.items():
                    self._otel_span.set_attribute(f"tag.{key}", value)

                # Set span status
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

    def get_correlation_id(self) -> str:
        """Get correlation ID for this span."""
        return f"{self.trace_context.trace_id}:{self.trace_context.span_id}"


class EnhancedTraceCollector:
    """Enhanced trace collector with W3C propagation support."""

    def __init__(self):
        self._active_traces: Dict[str, List[EnhancedSpan]] = {}
        self._completed_traces: Dict[str, List[EnhancedSpan]] = {}
        self._spans: Dict[str, EnhancedSpan] = {}
        self._lock = asyncio.Lock()

        # W3C propagation
        self._propagator = TraceContextPropagator() if OTEL_AVAILABLE else None

    async def start_span(
        self,
        name: str,
        span_type: str = "internal",
        attributes: Optional[Dict[str, Any]] = None,
        tags: Optional[Dict[str, str]] = None,
        parent_context: Optional[TraceContext] = None
    ) -> EnhancedSpan:
        """Start a new span with W3C context propagation."""
        # Get or create trace context
        if parent_context:
            trace_context = TraceContext.generate_new(parent_context)
        else:
            # Try to get from current context
            current_traceparent_str = current_traceparent.get()
            if current_traceparent_str:
                current_context = TraceContext.from_traceparent(current_traceparent_str)
                if current_context:
                    trace_context = TraceContext.generate_new(current_context)
                else:
                    trace_context = TraceContext.generate_new()
            else:
                trace_context = TraceContext.generate_new()

        # Create span
        span = EnhancedSpan(
            trace_context=trace_context,
            name=name,
            span_type=span_type,
            attributes=attributes or {},
            tags=tags or {}
        )

        # Set context variables
        span._trace_token = current_trace_id.set(trace_context.trace_id)
        span._span_token = current_span_id.set(trace_context.span_id)
        span._traceparent_token = current_traceparent.set(trace_context.to_traceparent())

        # Inherit attributes from parent context
        try:
            inherited_attributes = trace_attributes.get()
            span.attributes.update(inherited_attributes)
        except LookupError:
            inherited_attributes = {}

        # Add standard attributes
        span.attributes.update({
            "request_id": get_request_id(),
            "trace_id": trace_context.trace_id,
            "span_id": trace_context.span_id,
            "traceparent": trace_context.to_traceparent()
        })

        # Create OpenTelemetry span if available
        if OTEL_AVAILABLE:
            try:
                tracer = get_tracer("aurum")
                context = trace.set_span_in_context(
                    trace.get_current_span()
                ) if trace.get_current_span() else None

                otel_span = tracer.start_span(
                    name=span.name,
                    context=context,
                    attributes={
                        **span.attributes,
                        **span.tags,
                        "span.type": span.span_type,
                    }
                )
                span._otel_span = otel_span
            except Exception:
                pass

        # Store span
        async with self._lock:
            if trace_context.trace_id not in self._active_traces:
                self._active_traces[trace_context.trace_id] = []
            self._active_traces[trace_context.trace_id].append(span)
            self._spans[trace_context.span_id] = span

        return span

    async def finish_span(self, span: EnhancedSpan) -> None:
        """Finish a span and restore context."""
        span.finish()

        # Move from active to completed traces
        async with self._lock:
            if span.trace_context.trace_id in self._active_traces:
                active_spans = self._active_traces[span.trace_context.trace_id]
                if span in active_spans:
                    active_spans.remove(span)

                    if span.trace_context.trace_id not in self._completed_traces:
                        self._completed_traces[span.trace_context.trace_id] = []
                    self._completed_traces[span.trace_context.trace_id].append(span)

                    # Clean up if trace is complete
                    if not active_spans:
                        del self._active_traces[span.trace_context.trace_id]

        # Restore previous context
        if span._span_token:
            try:
                current_span_id.reset(span._span_token)
            except (LookupError, ValueError, RuntimeError):
                current_span_id.set(span.trace_context.span_id if span.trace_context.span_id else None)

        if span._trace_token:
            try:
                current_trace_id.reset(span._trace_token)
            except (LookupError, ValueError, RuntimeError):
                current_trace_id.set(None)

        if span._traceparent_token:
            try:
                current_traceparent.reset(span._traceparent_token)
            except (LookupError, ValueError, RuntimeError):
                current_traceparent.set(None)

        if span._attributes_token:
            try:
                trace_attributes.reset(span._attributes_token)
            except (LookupError, ValueError, RuntimeError):
                trace_attributes.set({})

    def get_current_trace_context(self) -> Optional[TraceContext]:
        """Get current trace context from context variables."""
        traceparent_str = current_traceparent.get()
        if traceparent_str:
            return TraceContext.from_traceparent(traceparent_str)
        return None

    def extract_trace_context(self, headers: Dict[str, str]) -> Optional[TraceContext]:
        """Extract trace context from HTTP headers."""
        if OTEL_AVAILABLE and self._propagator:
            carrier = {}
            for key, value in headers.items():
                if key.lower().startswith('traceparent') or key.lower().startswith('tracestate'):
                    carrier[key] = value

            context = extract(carrier)
            if context:
                span = trace.get_current_span(context)
                if span:
                    span_context = span.get_span_context()
                    return TraceContext(
                        trace_id=span_context.trace_id,
                        span_id=span_context.span_id,
                        trace_flags=f"{span_context.trace_flags:02x}"
                    )

        # Fallback to manual extraction
        traceparent = headers.get('traceparent')
        if traceparent:
            return TraceContext.from_traceparent(traceparent)

        return None

    def inject_trace_context(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Inject trace context into HTTP headers."""
        context = self.get_current_trace_context()
        if context:
            headers.update(context.to_dict())
        return headers

    async def get_trace(self, trace_id: str) -> Optional[List[EnhancedSpan]]:
        """Get all spans for a trace."""
        async with self._lock:
            active_spans = self._active_traces.get(trace_id, [])
            completed_spans = self._completed_traces.get(trace_id, [])
            return active_spans + completed_spans

    async def get_trace_report(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Generate a detailed report for a trace."""
        spans = await self.get_trace(trace_id)
        if not spans:
            return None

        # Sort spans by start time
        spans.sort(key=lambda x: x.start_time)

        # Build trace tree
        root_spans = [span for span in spans if not span.trace_context.span_id]
        root_spans.sort(key=lambda span: span.start_time)

        children_map: Dict[str, List[EnhancedSpan]] = {}
        for child in spans:
            parent_id = getattr(child, 'parent_span_id', None)
            if parent_id:
                children_map.setdefault(parent_id, []).append(child)

        def build_span_tree(span: EnhancedSpan) -> Dict[str, Any]:
            children = [
                build_span_tree(child)
                for child in sorted(children_map.get(span.trace_context.span_id, []), key=lambda c: c.start_time)
            ]

            return {
                "span_id": span.trace_context.span_id,
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
                "trace_context": {
                    "trace_id": span.trace_context.trace_id,
                    "span_id": span.trace_context.span_id,
                    "traceparent": span.trace_context.to_traceparent()
                },
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


# Global trace collector
_enhanced_trace_collector = EnhancedTraceCollector()


def get_enhanced_trace_collector() -> EnhancedTraceCollector:
    """Get the global enhanced trace collector."""
    return _enhanced_trace_collector


@asynccontextmanager
async def enhanced_trace_span(
    name: str,
    span_type: str = "internal",
    attributes: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None
):
    """Enhanced context manager for creating and automatically finishing spans."""
    span = await _enhanced_trace_collector.start_span(name, span_type, attributes, tags)
    try:
        yield span
    except Exception as exc:
        span.status = "error"
        span.error_message = str(exc)
        span.set_attribute("error", True)
        span.set_attribute("error_message", str(exc))
        raise
    finally:
        await _enhanced_trace_collector.finish_span(span)


async def start_enhanced_trace(
    name: str,
    span_type: str = "request",
    attributes: Optional[Dict[str, Any]] = None,
    tags: Optional[Dict[str, str]] = None
) -> EnhancedSpan:
    """Start a new trace with a root span."""
    return await _enhanced_trace_collector.start_span(name, span_type, attributes, tags)


def get_current_trace_context() -> Optional[TraceContext]:
    """Get the current trace context."""
    return _enhanced_trace_collector.get_current_trace_context()


def extract_trace_context_from_headers(headers: Dict[str, str]) -> Optional[TraceContext]:
    """Extract trace context from HTTP headers."""
    return _enhanced_trace_collector.extract_trace_context(headers)


def inject_trace_context_into_headers(headers: Dict[str, str]) -> Dict[str, str]:
    """Inject trace context into HTTP headers."""
    return _enhanced_trace_collector.inject_trace_context(headers)


# Enhanced tracing for Kafka messages
async def propagate_trace_to_kafka_message(
    message: Dict[str, Any],
    headers: Optional[Dict[str, str]] = None
) -> Dict[str, Any]:
    """Add trace context to Kafka message."""
    trace_context = get_current_trace_context()
    if trace_context:
        message["trace_context"] = {
            "trace_id": trace_context.trace_id,
            "span_id": trace_context.span_id,
            "traceparent": trace_context.to_traceparent()
        }

        # Add to headers if provided
        if headers is None:
            headers = {}
        headers.update(trace_context.to_dict())

    return message, headers


async def extract_trace_from_kafka_message(message: Dict[str, Any]) -> Optional[TraceContext]:
    """Extract trace context from Kafka message."""
    trace_context_data = message.get("trace_context")
    if trace_context_data:
        return TraceContext(
            trace_id=trace_context_data["trace_id"],
            span_id=trace_context_data["span_id"]
        )

    # Fallback to headers
    headers = message.get("headers", {})
    return extract_trace_context_from_headers(headers)


# Convenience functions for common operations
async def trace_http_request(
    method: str,
    url: str,
    headers: Optional[Dict[str, str]] = None
) -> EnhancedSpan:
    """Start tracing an HTTP request."""
    if headers is None:
        headers = {}

    # Extract trace context from incoming headers
    trace_context = extract_trace_context_from_headers(headers)
    if not trace_context:
        trace_context = TraceContext.generate_new()

    span = await _enhanced_trace_collector.start_span(
        name=f"HTTP {method} {url}",
        span_type="http_request",
        attributes={
            "http.method": method,
            "http.url": url,
            "http.headers": dict(headers)
        },
        tags={"component": "http", "operation": "request"}
    )

    # Set trace context in headers for outgoing request
    headers.update(trace_context.to_dict())

    return span


async def trace_database_operation(
    operation: str,
    table: str = "",
    query: str = ""
) -> EnhancedSpan:
    """Start tracing a database operation."""
    span = await _enhanced_trace_collector.start_span(
        name=f"DB {operation}",
        span_type="database",
        attributes={
            "db.operation": operation,
            "db.table": table,
            "db.query": query[:1000]  # Limit query length
        },
        tags={"component": "database", "operation": operation}
    )
    return span


async def trace_kafka_operation(
    operation: str,
    topic: str,
    message_count: int = 1
) -> EnhancedSpan:
    """Start tracing a Kafka operation."""
    span = await _enhanced_trace_collector.start_span(
        name=f"Kafka {operation}",
        span_type="kafka",
        attributes={
            "kafka.operation": operation,
            "kafka.topic": topic,
            "kafka.message_count": message_count
        },
        tags={"component": "kafka", "operation": operation}
    )
    return span


__all__ = [
    "TraceContext",
    "EnhancedSpan",
    "EnhancedTraceCollector",
    "get_enhanced_trace_collector",
    "enhanced_trace_span",
    "start_enhanced_trace",
    "get_current_trace_context",
    "extract_trace_context_from_headers",
    "inject_trace_context_into_headers",
    "propagate_trace_to_kafka_message",
    "extract_trace_from_kafka_message",
    "trace_http_request",
    "trace_database_operation",
    "trace_kafka_operation",
]
