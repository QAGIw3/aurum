"""OpenTelemetry instrumentation and observability utilities."""
from __future__ import annotations

import logging
import time
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

from .config import ObservabilitySettings

logger = logging.getLogger(__name__)


class ObservabilityManager:
    """Manages OpenTelemetry tracing and metrics."""
    
    def __init__(self, settings: ObservabilitySettings):
        self.settings = settings
        self.tracer = None
        self.meter = None
        self._initialized = False
        
        # Metrics
        self.request_counter = None
        self.request_duration = None
        self.cache_operations = None
        self.db_operations = None
    
    def initialize(self):
        """Initialize OpenTelemetry providers and exporters."""
        if self._initialized:
            return
        
        if not self.settings.enable_tracing and not self.settings.enable_metrics:
            logger.info("OpenTelemetry disabled")
            return
        
        # Configure tracing
        if self.settings.enable_tracing:
            self._setup_tracing()
        
        # Configure metrics
        if self.settings.enable_metrics:
            self._setup_metrics()
        
        # Auto-instrument libraries
        self._setup_auto_instrumentation()
        
        self._initialized = True
        logger.info(f"OpenTelemetry initialized for service: {self.settings.service_name}")
    
    def _setup_tracing(self):
        """Setup tracing with OTLP exporter."""
        provider = TracerProvider()
        
        if self.settings.otel_endpoint:
            # OTLP exporter for production
            otlp_exporter = OTLPSpanExporter(
                endpoint=self.settings.otel_endpoint,
                insecure=True,  # Use secure=True in production with TLS
            )
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
        else:
            # Console exporter for development
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter
            console_exporter = ConsoleSpanExporter()
            provider.add_span_processor(BatchSpanProcessor(console_exporter))
        
        trace.set_tracer_provider(provider)
        self.tracer = trace.get_tracer(__name__)
    
    def _setup_metrics(self):
        """Setup metrics with OTLP exporter."""
        if self.settings.otel_endpoint:
            # OTLP exporter for production
            otlp_metric_exporter = OTLPMetricExporter(
                endpoint=self.settings.otel_endpoint,
                insecure=True,  # Use secure=True in production with TLS
            )
            metric_reader = PeriodicExportingMetricReader(otlp_metric_exporter, export_interval_millis=10000)
        else:
            # Console exporter for development
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter
            console_metric_exporter = ConsoleMetricExporter()
            metric_reader = PeriodicExportingMetricReader(console_metric_exporter, export_interval_millis=10000)
        
        provider = MeterProvider(metric_readers=[metric_reader])
        metrics.set_meter_provider(provider)
        self.meter = metrics.get_meter(__name__)
        
        # Create common metrics
        self.request_counter = self.meter.create_counter(
            "aurum_requests_total",
            description="Total number of requests",
        )
        
        self.request_duration = self.meter.create_histogram(
            "aurum_request_duration_seconds",
            description="Request duration in seconds",
        )
        
        self.cache_operations = self.meter.create_counter(
            "aurum_cache_operations_total", 
            description="Total cache operations",
        )
        
        self.db_operations = self.meter.create_counter(
            "aurum_db_operations_total",
            description="Total database operations",
        )
    
    def _setup_auto_instrumentation(self):
        """Setup automatic instrumentation for common libraries."""
        try:
            # Instrument SQLAlchemy
            SQLAlchemyInstrumentor().instrument()
            
            # Instrument HTTPX
            HTTPXClientInstrumentor().instrument()
            
            logger.info("Auto-instrumentation enabled for SQLAlchemy and HTTPX")
            
        except Exception as e:
            logger.warning(f"Failed to setup auto-instrumentation: {e}")
    
    def instrument_fastapi(self, app):
        """Instrument FastAPI application."""
        if not self._initialized:
            self.initialize()
        
        if self.settings.enable_tracing:
            FastAPIInstrumentor.instrument_app(
                app,
                tracer_provider=trace.get_tracer_provider(),
                meter_provider=metrics.get_meter_provider() if self.settings.enable_metrics else None,
            )
            logger.info("FastAPI instrumentation enabled")
    
    @asynccontextmanager
    async def trace_operation(self, operation_name: str, attributes: Optional[Dict[str, Any]] = None):
        """Context manager for tracing operations."""
        if not self.tracer:
            yield
            return
        
        with self.tracer.start_as_current_span(operation_name) as span:
            if attributes:
                span.set_attributes(attributes)
            
            start_time = time.time()
            
            try:
                yield span
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                raise
            finally:
                # Record duration metric
                if self.request_duration:
                    duration = time.time() - start_time
                    self.request_duration.record(
                        duration,
                        attributes={"operation": operation_name}
                    )
    
    def record_cache_operation(self, operation: str, hit: bool = False):
        """Record cache operation metrics."""
        if self.cache_operations:
            self.cache_operations.add(
                1,
                attributes={
                    "operation": operation,
                    "result": "hit" if hit else "miss"
                }
            )
    
    def record_db_operation(self, operation: str, repository: str, success: bool = True):
        """Record database operation metrics."""
        if self.db_operations:
            self.db_operations.add(
                1,
                attributes={
                    "operation": operation,
                    "repository": repository,
                    "status": "success" if success else "error"
                }
            )
    
    def record_request(self, method: str, path: str, status_code: int):
        """Record HTTP request metrics."""
        if self.request_counter:
            self.request_counter.add(
                1,
                attributes={
                    "method": method,
                    "path": path,
                    "status_code": str(status_code)
                }
            )


# Global observability manager instance
_observability: Optional[ObservabilityManager] = None


def get_observability() -> Optional[ObservabilityManager]:
    """Get the global observability manager."""
    return _observability


def configure_observability(settings: ObservabilitySettings) -> ObservabilityManager:
    """Configure global observability manager."""
    global _observability
    _observability = ObservabilityManager(settings)
    _observability.initialize()
    return _observability


def trace_operation(operation_name: str, attributes: Optional[Dict[str, Any]] = None):
    """Decorator for tracing operations."""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            obs = get_observability()
            if obs:
                async with obs.trace_operation(operation_name, attributes):
                    return await func(*args, **kwargs)
            else:
                return await func(*args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            obs = get_observability()
            if obs and obs.tracer:
                with obs.tracer.start_as_current_span(operation_name) as span:
                    if attributes:
                        span.set_attributes(attributes)
                    return func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        
        if hasattr(func, '__call__') and hasattr(func, '__code__'):
            if func.__code__.co_flags & 0x80:  # CO_COROUTINE
                return async_wrapper
        
        return sync_wrapper
    
    return decorator