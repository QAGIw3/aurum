"""Unified logging and tracing patterns for consistent observability across services.

This module provides standardized patterns for structured logging, distributed tracing,
and correlation ID management across all Aurum services.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from contextlib import contextmanager, asynccontextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, AsyncGenerator, Generator
from datetime import datetime

from .facade import get_observability_facade, ObservabilityContext
from ..telemetry.context import (
    get_request_id, 
    get_tenant_id,
    get_correlation_id,
    set_correlation_id
)


class TraceEventType(str, Enum):
    """Types of trace events."""
    SPAN_START = "span_start"
    SPAN_END = "span_end"
    LOG = "log"
    ERROR = "error"
    METRIC = "metric"
    ANNOTATION = "annotation"


class LogPattern(str, Enum):
    """Standardized logging patterns."""
    REQUEST_START = "request_start"
    REQUEST_END = "request_end"
    OPERATION_START = "operation_start"
    OPERATION_END = "operation_end"
    ERROR_OCCURRED = "error_occurred"
    EXTERNAL_CALL = "external_call"
    DATABASE_QUERY = "database_query"
    CACHE_OPERATION = "cache_operation"
    BUSINESS_EVENT = "business_event"


@dataclass
class StructuredLogEntry:
    """Standardized structured log entry."""
    timestamp: str
    level: str
    pattern: LogPattern
    message: str
    
    # Context
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None
    tenant_id: Optional[str] = None
    user_id: Optional[str] = None
    
    # Service context
    service: str = "aurum"
    component: Optional[str] = None
    operation: Optional[str] = None
    version: Optional[str] = None
    
    # Performance
    duration_ms: Optional[float] = None
    status_code: Optional[int] = None
    
    # Business context
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    
    # Additional data
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "timestamp": self.timestamp,
            "level": self.level,
            "pattern": self.pattern.value,
            "message": self.message,
            "request_id": self.request_id,
            "correlation_id": self.correlation_id,
            "tenant_id": self.tenant_id,
            "user_id": self.user_id,
            "service": self.service,
            "component": self.component,
            "operation": self.operation,
            "version": self.version,
            "duration_ms": self.duration_ms,
            "status_code": self.status_code,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "tags": self.tags,
            "metadata": self.metadata,
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), default=str)


@dataclass
class TraceSpan:
    """Distributed tracing span."""
    trace_id: str
    span_id: str
    parent_span_id: Optional[str] = None
    operation_name: str = ""
    service_name: str = "aurum"
    component: Optional[str] = None
    
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    
    # Status
    status: str = "ok"  # ok, error, timeout
    status_message: Optional[str] = None
    
    # Tags and annotations
    tags: Dict[str, str] = field(default_factory=dict)
    annotations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Context
    request_id: Optional[str] = None
    tenant_id: Optional[str] = None
    
    def finish(self, status: str = "ok", status_message: Optional[str] = None):
        """Finish the span."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        self.status = status
        self.status_message = status_message
    
    def add_tag(self, key: str, value: str):
        """Add a tag to the span."""
        self.tags[key] = value
    
    def add_annotation(self, message: str, **metadata):
        """Add an annotation to the span."""
        self.annotations.append({
            "timestamp": time.time(),
            "message": message,
            "metadata": metadata
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "service_name": self.service_name,
            "component": self.component,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "status": self.status,
            "status_message": self.status_message,
            "tags": self.tags,
            "annotations": self.annotations,
            "request_id": self.request_id,
            "tenant_id": self.tenant_id,
        }


class UnifiedLogger:
    """Unified logger with standardized patterns."""
    
    def __init__(self, component: str = "unknown"):
        self.component = component
        self.facade = get_observability_facade()
        self._current_operation: Optional[str] = None
        self._operation_start_time: Optional[float] = None
    
    def _create_log_entry(
        self,
        level: str,
        pattern: LogPattern,
        message: str,
        **kwargs
    ) -> StructuredLogEntry:
        """Create standardized log entry."""
        return StructuredLogEntry(
            timestamp=datetime.utcnow().isoformat() + "Z",
            level=level,
            pattern=pattern,
            message=message,
            request_id=get_request_id(),
            correlation_id=get_correlation_id(),
            tenant_id=get_tenant_id(),
            component=self.component,
            operation=self._current_operation,
            duration_ms=(
                (time.time() - self._operation_start_time) * 1000
                if self._operation_start_time else None
            ),
            **kwargs
        )
    
    def log_request_start(self, method: str, path: str, **metadata):
        """Log request start with standardized pattern."""
        entry = self._create_log_entry(
            "INFO",
            LogPattern.REQUEST_START,
            f"{method} {path} - Request started",
            tags={"method": method, "path": path},
            metadata=metadata
        )
        
        self.facade.info(entry.message, **entry.to_dict())
    
    def log_request_end(
        self, 
        method: str, 
        path: str, 
        status_code: int, 
        duration_ms: float,
        **metadata
    ):
        """Log request end with standardized pattern."""
        entry = self._create_log_entry(
            "INFO",
            LogPattern.REQUEST_END,
            f"{method} {path} - Request completed",
            status_code=status_code,
            duration_ms=duration_ms,
            tags={"method": method, "path": path, "status_code": str(status_code)},
            metadata=metadata
        )
        
        self.facade.info(entry.message, **entry.to_dict())
    
    def log_operation_start(self, operation: str, **metadata):
        """Log operation start."""
        self._current_operation = operation
        self._operation_start_time = time.time()
        
        entry = self._create_log_entry(
            "DEBUG",
            LogPattern.OPERATION_START,
            f"Operation started: {operation}",
            operation=operation,
            tags={"operation": operation},
            metadata=metadata
        )
        
        self.facade.debug(entry.message, **entry.to_dict())
    
    def log_operation_end(self, operation: str, success: bool = True, **metadata):
        """Log operation end."""
        duration_ms = (
            (time.time() - self._operation_start_time) * 1000
            if self._operation_start_time else None
        )
        
        entry = self._create_log_entry(
            "DEBUG",
            LogPattern.OPERATION_END,
            f"Operation completed: {operation}",
            operation=operation,
            duration_ms=duration_ms,
            tags={"operation": operation, "success": str(success)},
            metadata=metadata
        )
        
        self.facade.debug(entry.message, **entry.to_dict())
        
        # Reset operation context
        self._current_operation = None
        self._operation_start_time = None
    
    def log_error(self, error: Union[str, Exception], operation: Optional[str] = None, **metadata):
        """Log error with standardized pattern."""
        error_message = str(error)
        error_type = type(error).__name__ if isinstance(error, Exception) else "Error"
        
        entry = self._create_log_entry(
            "ERROR",
            LogPattern.ERROR_OCCURRED,
            f"Error in {operation or self._current_operation or 'unknown'}: {error_message}",
            operation=operation or self._current_operation,
            tags={"error_type": error_type},
            metadata={"error_details": str(error), **metadata}
        )
        
        self.facade.error(entry.message, **entry.to_dict())
    
    def log_external_call(
        self,
        service: str,
        method: str,
        url: str,
        status_code: Optional[int] = None,
        duration_ms: Optional[float] = None,
        **metadata
    ):
        """Log external service call."""
        entry = self._create_log_entry(
            "INFO",
            LogPattern.EXTERNAL_CALL,
            f"External call to {service}: {method} {url}",
            status_code=status_code,
            duration_ms=duration_ms,
            tags={"external_service": service, "method": method},
            metadata={"url": url, **metadata}
        )
        
        self.facade.info(entry.message, **entry.to_dict())
    
    def log_database_query(
        self,
        query_type: str,
        table: Optional[str] = None,
        duration_ms: Optional[float] = None,
        rows_affected: Optional[int] = None,
        **metadata
    ):
        """Log database query."""
        entry = self._create_log_entry(
            "DEBUG",
            LogPattern.DATABASE_QUERY,
            f"Database {query_type}" + (f" on {table}" if table else ""),
            duration_ms=duration_ms,
            tags={"query_type": query_type, "table": table or "unknown"},
            metadata={"rows_affected": rows_affected, **metadata}
        )
        
        self.facade.debug(entry.message, **entry.to_dict())
    
    def log_cache_operation(
        self,
        operation: str,
        key: str,
        hit: Optional[bool] = None,
        duration_ms: Optional[float] = None,
        **metadata
    ):
        """Log cache operation."""
        entry = self._create_log_entry(
            "DEBUG",
            LogPattern.CACHE_OPERATION,
            f"Cache {operation}: {key}",
            duration_ms=duration_ms,
            tags={"cache_operation": operation, "cache_hit": str(hit) if hit is not None else "unknown"},
            metadata={"cache_key": key, **metadata}
        )
        
        self.facade.debug(entry.message, **entry.to_dict())
    
    def log_business_event(
        self,
        event_type: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        **metadata
    ):
        """Log business event."""
        entry = self._create_log_entry(
            "INFO",
            LogPattern.BUSINESS_EVENT,
            f"Business event: {event_type}",
            entity_type=entity_type,
            entity_id=entity_id,
            tags={"event_type": event_type},
            metadata=metadata
        )
        
        self.facade.info(entry.message, **entry.to_dict())


class UnifiedTracer:
    """Unified tracer for distributed tracing."""
    
    def __init__(self, service_name: str = "aurum"):
        self.service_name = service_name
        self.facade = get_observability_facade()
        self._active_spans: List[TraceSpan] = []
    
    def create_span(
        self,
        operation_name: str,
        component: Optional[str] = None,
        parent_span: Optional[TraceSpan] = None,
        **tags
    ) -> TraceSpan:
        """Create a new trace span."""
        # Generate trace and span IDs
        if parent_span:
            trace_id = parent_span.trace_id
            parent_span_id = parent_span.span_id
        else:
            trace_id = str(uuid.uuid4())
            parent_span_id = None
        
        span_id = str(uuid.uuid4())
        
        span = TraceSpan(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            operation_name=operation_name,
            service_name=self.service_name,
            component=component,
            request_id=get_request_id(),
            tenant_id=get_tenant_id(),
            tags=tags
        )
        
        return span
    
    @contextmanager
    def trace_operation(
        self,
        operation_name: str,
        component: Optional[str] = None,
        **tags
    ) -> Generator[TraceSpan, None, None]:
        """Context manager for tracing operations."""
        parent_span = self._active_spans[-1] if self._active_spans else None
        span = self.create_span(operation_name, component, parent_span, **tags)
        
        self._active_spans.append(span)
        
        # Log span start
        self.facade.debug(
            f"Trace span started: {operation_name}",
            trace_id=span.trace_id,
            span_id=span.span_id,
            operation=operation_name,
            component=component
        )
        
        try:
            yield span
        except Exception as e:
            span.add_annotation("error", error=str(e))
            span.finish("error", str(e))
            raise
        else:
            span.finish("ok")
        finally:
            self._active_spans.pop()
            
            # Log span end
            self.facade.debug(
                f"Trace span completed: {operation_name}",
                trace_id=span.trace_id,
                span_id=span.span_id,
                operation=operation_name,
                duration_ms=span.duration_ms,
                status=span.status
            )
    
    @asynccontextmanager
    async def async_trace_operation(
        self,
        operation_name: str,
        component: Optional[str] = None,
        **tags
    ) -> AsyncGenerator[TraceSpan, None]:
        """Async context manager for tracing operations."""
        parent_span = self._active_spans[-1] if self._active_spans else None
        span = self.create_span(operation_name, component, parent_span, **tags)
        
        self._active_spans.append(span)
        
        # Log span start
        self.facade.debug(
            f"Async trace span started: {operation_name}",
            trace_id=span.trace_id,
            span_id=span.span_id,
            operation=operation_name,
            component=component
        )
        
        try:
            yield span
        except Exception as e:
            span.add_annotation("error", error=str(e))
            span.finish("error", str(e))
            raise
        else:
            span.finish("ok")
        finally:
            self._active_spans.pop()
            
            # Log span end
            self.facade.debug(
                f"Async trace span completed: {operation_name}",
                trace_id=span.trace_id,
                span_id=span.span_id,
                operation=operation_name,
                duration_ms=span.duration_ms,
                status=span.status
            )
    
    def get_current_span(self) -> Optional[TraceSpan]:
        """Get the current active span."""
        return self._active_spans[-1] if self._active_spans else None


class CorrelationManager:
    """Manages correlation IDs across service boundaries."""
    
    @staticmethod
    def ensure_correlation_id() -> str:
        """Ensure a correlation ID exists and return it."""
        correlation_id = get_correlation_id()
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
            set_correlation_id(correlation_id)
        return correlation_id
    
    @staticmethod
    @contextmanager
    def correlation_context(correlation_id: Optional[str] = None) -> Generator[str, None, None]:
        """Context manager for correlation ID."""
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        # Set correlation ID for this context
        original_id = get_correlation_id()
        set_correlation_id(correlation_id)
        
        try:
            yield correlation_id
        finally:
            # Restore original correlation ID
            if original_id:
                set_correlation_id(original_id)


# Global instances
def get_unified_logger(component: str) -> UnifiedLogger:
    """Get a unified logger for a component."""
    return UnifiedLogger(component)

def get_unified_tracer(service_name: str = "aurum") -> UnifiedTracer:
    """Get a unified tracer."""
    return UnifiedTracer(service_name)


__all__ = [
    "TraceEventType",
    "LogPattern",
    "StructuredLogEntry",
    "TraceSpan",
    "UnifiedLogger",
    "UnifiedTracer",
    "CorrelationManager",
    "get_unified_logger",
    "get_unified_tracer",
]