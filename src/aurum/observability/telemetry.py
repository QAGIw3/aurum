"""Enhanced OpenTelemetry instrumentation for data pipeline observability."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, Optional, Union

from opentelemetry import trace
from opentelemetry.exporter.console import ConsoleSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode
from opentelemetry.trace.span import Span

from ..config import settings


class DataPipelineTracer:
    """Enhanced OpenTelemetry tracer for data pipeline components."""

    def __init__(
        self,
        service_name: str = "aurum-data-pipeline",
        otlp_endpoint: Optional[str] = None,
        enable_console_export: bool = False
    ):
        """Initialize the tracer."""
        self.service_name = service_name
        self.resource = Resource.create({"service.name": service_name})

        # Initialize tracer provider
        self.tracer_provider = TracerProvider(resource=self.resource)
        trace.set_tracer_provider(self.tracer_provider)

        # Add span processors
        if otlp_endpoint:
            otlp_exporter = OTLPSpanExporter(
                endpoint=otlp_endpoint,
                headers={"service.name": service_name}
            )
            span_processor = BatchSpanProcessor(otlp_exporter)
            self.tracer_provider.add_span_processor(span_processor)

        if enable_console_export or not otlp_endpoint:
            console_exporter = ConsoleSpanExporter()
            span_processor = SimpleSpanProcessor(console_exporter)
            self.tracer_provider.add_span_processor(span_processor)

        self.tracer = trace.get_tracer(__name__)

    def get_tracer(self):
        """Get the configured tracer."""
        return self.tracer

    @contextmanager
    def trace_operation(
        self,
        operation_name: str,
        attributes: Optional[Dict[str, Any]] = None,
        record_exception: bool = True
    ):
        """Context manager for tracing operations."""
        span = self.tracer.start_span(operation_name, attributes=attributes or {})

        try:
            yield span
            span.set_status(Status(StatusCode.OK))
        except Exception as e:
            if record_exception:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
            raise
        finally:
            span.end()

    def trace_async_operation(
        self,
        operation_name: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Decorator for tracing async operations."""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                span = self.tracer.start_span(operation_name, attributes=attributes or {})

                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
                finally:
                    span.end()

            return wrapper
        return decorator

    def trace_sync_operation(
        self,
        operation_name: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Decorator for tracing sync operations."""
        def decorator(func):
            def wrapper(*args, **kwargs):
                span = self.tracer.start_span(operation_name, attributes=attributes or {})

                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise
                finally:
                    span.end()

            return wrapper
        return decorator


class KafkaTelemetry:
    """OpenTelemetry instrumentation for Kafka operations."""

    def __init__(self, tracer: trace.Tracer):
        """Initialize Kafka telemetry."""
        self.tracer = tracer

    @contextmanager
    def trace_producer_operation(
        self,
        topic: str,
        operation: str = "produce",
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace Kafka producer operations."""
        span_attributes = {
            "messaging.system": "kafka",
            "messaging.destination": topic,
            "messaging.operation": operation,
            "messaging.destination_kind": "topic",
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation(f"kafka.{operation}", span_attributes) as span:
            start_time = time.time()
            yield span
            duration = time.time() - start_time
            span.set_attribute("messaging.duration_ms", duration * 1000)

    @contextmanager
    def trace_consumer_operation(
        self,
        topic: str,
        consumer_group: str,
        operation: str = "consume",
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace Kafka consumer operations."""
        span_attributes = {
            "messaging.system": "kafka",
            "messaging.source": topic,
            "messaging.consumer_group": consumer_group,
            "messaging.operation": operation,
            "messaging.source_kind": "topic",
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation(f"kafka.{operation}", span_attributes) as span:
            yield span

    def record_producer_metrics(
        self,
        span: Span,
        message_count: int,
        batch_size: int,
        compression_type: str,
        success: bool = True
    ):
        """Record producer-specific metrics."""
        span.set_attribute("kafka.message_count", message_count)
        span.set_attribute("kafka.batch_size", batch_size)
        span.set_attribute("kafka.compression_type", compression_type)
        span.set_attribute("kafka.producer_success", success)

        if success:
            span.add_event("kafka.produce_batch_success", {
                "message_count": message_count,
                "batch_size": batch_size
            })
        else:
            span.add_event("kafka.produce_batch_failed", {
                "message_count": message_count,
                "batch_size": batch_size
            })


class SeaTunnelTelemetry:
    """OpenTelemetry instrumentation for SeaTunnel operations."""

    def __init__(self, tracer: trace.Tracer):
        """Initialize SeaTunnel telemetry."""
        self.tracer = tracer

    @contextmanager
    def trace_seatunnel_job(
        self,
        job_name: str,
        job_config_path: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace SeaTunnel job execution."""
        span_attributes = {
            "seatunnel.job.name": job_name,
            "seatunnel.job.config_path": job_config_path,
            "batch.job.name": job_name,
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation("seatunnel.job_execution", span_attributes) as span:
            yield span

    @contextmanager
    def trace_seatunnel_stage(
        self,
        stage_name: str,
        stage_type: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace individual SeaTunnel processing stages."""
        span_attributes = {
            "seatunnel.stage.name": stage_name,
            "seatunnel.stage.type": stage_type,
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation(f"seatunnel.{stage_type}", span_attributes) as span:
            yield span

    def record_stage_metrics(
        self,
        span: Span,
        records_processed: int,
        records_written: int,
        processing_time_ms: float,
        stage_success: bool = True
    ):
        """Record SeaTunnel stage metrics."""
        span.set_attribute("seatunnel.records_processed", records_processed)
        span.set_attribute("seatunnel.records_written", records_written)
        span.set_attribute("seatunnel.processing_time_ms", processing_time_ms)
        span.set_attribute("seatunnel.stage_success", stage_success)

        if stage_success:
            span.add_event("seatunnel.stage_completed", {
                "records_processed": records_processed,
                "records_written": records_written,
                "processing_time_ms": processing_time_ms
            })
        else:
            span.add_event("seatunnel.stage_failed", {
                "records_processed": records_processed,
                "processing_time_ms": processing_time_ms
            })


class APITelemetry:
    """OpenTelemetry instrumentation for external API calls."""

    def __init__(self, tracer: trace.Tracer):
        """Initialize API telemetry."""
        self.tracer = tracer

    @contextmanager
    def trace_api_call(
        self,
        api_name: str,
        endpoint: str,
        method: str = "GET",
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace external API calls."""
        span_attributes = {
            "http.method": method,
            "http.url": endpoint,
            "http.user_agent": "Aurum-Data-Platform/1.0",
            "api.name": api_name,
            "api.endpoint": endpoint,
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation(f"api.{api_name}", span_attributes) as span:
            start_time = time.time()
            yield span
            duration = time.time() - start_time
            span.set_attribute("http.duration_ms", duration * 1000)

    @contextmanager
    def trace_rate_limited_call(
        self,
        api_name: str,
        endpoint: str,
        rate_limit_remaining: int,
        rate_limit_reset: Optional[float] = None,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace rate-limited API calls with quota information."""
        span_attributes = {
            "api.rate_limit_remaining": rate_limit_remaining,
        }
        if rate_limit_reset:
            span_attributes["api.rate_limit_reset"] = rate_limit_reset
        span_attributes.update(attributes or {})

        with self.trace_api_call(api_name, endpoint, attributes=span_attributes) as span:
            yield span

    def record_api_metrics(
        self,
        span: Span,
        response_status: int,
        response_size: Optional[int] = None,
        api_key_used: bool = False,
        quota_exceeded: bool = False
    ):
        """Record API-specific metrics."""
        span.set_attribute("http.status_code", response_status)
        if response_size:
            span.set_attribute("http.response_size_bytes", response_size)
        span.set_attribute("api.key_used", api_key_used)
        span.set_attribute("api.quota_exceeded", quota_exceeded)

        if response_status >= 400:
            span.add_event("api_error", {
                "status_code": response_status,
                "quota_exceeded": quota_exceeded
            })


class DataQualityTelemetry:
    """OpenTelemetry instrumentation for data quality operations."""

    def __init__(self, tracer: trace.Tracer):
        """Initialize data quality telemetry."""
        self.tracer = tracer

    @contextmanager
    def trace_validation_run(
        self,
        table_name: str,
        expectation_suite: str,
        attributes: Optional[Dict[str, Any]] = None
    ):
        """Trace data validation runs."""
        span_attributes = {
            "data_quality.table": table_name,
            "data_quality.expectation_suite": expectation_suite,
            "validation.run.table": table_name,
        }
        span_attributes.update(attributes or {})

        with self.tracer.trace_operation("data_quality.validation", span_attributes) as span:
            yield span

    def record_validation_results(
        self,
        span: Span,
        total_expectations: int,
        successful_expectations: int,
        failed_expectations: int,
        evaluated_records: int,
        validation_success: bool = True
    ):
        """Record data validation results."""
        span.set_attribute("data_quality.expectations_total", total_expectations)
        span.set_attribute("data_quality.expectations_successful", successful_expectations)
        span.set_attribute("data_quality.expectations_failed", failed_expectations)
        span.set_attribute("data_quality.records_evaluated", evaluated_records)
        span.set_attribute("data_quality.validation_success", validation_success)

        success_rate = successful_expectations / total_expectations if total_expectations > 0 else 0
        span.set_attribute("data_quality.success_rate", success_rate)

        if validation_success:
            span.add_event("validation_passed", {
                "expectations_successful": successful_expectations,
                "expectations_failed": failed_expectations,
                "records_evaluated": evaluated_records,
                "success_rate": success_rate
            })
        else:
            span.add_event("validation_failed", {
                "expectations_successful": successful_expectations,
                "expectations_failed": failed_expectations,
                "records_evaluated": evaluated_records,
                "success_rate": success_rate
            })


# Global telemetry instance
_data_pipeline_tracer = None


def get_data_pipeline_tracer() -> DataPipelineTracer:
    """Get or create the global data pipeline tracer."""
    global _data_pipeline_tracer

    if _data_pipeline_tracer is None:
        _data_pipeline_tracer = DataPipelineTracer(
            service_name="aurum-data-pipeline",
            otlp_endpoint=getattr(settings, 'OTLP_ENDPOINT', None),
            enable_console_export=getattr(settings, 'OTLP_CONSOLE_EXPORT', False)
        )

    return _data_pipeline_tracer


def get_kafka_telemetry() -> KafkaTelemetry:
    """Get Kafka telemetry instance."""
    tracer = get_data_pipeline_tracer().get_tracer()
    return KafkaTelemetry(tracer)


def get_seatunnel_telemetry() -> SeaTunnelTelemetry:
    """Get SeaTunnel telemetry instance."""
    tracer = get_data_pipeline_tracer().get_tracer()
    return SeaTunnelTelemetry(tracer)


def get_api_telemetry() -> APITelemetry:
    """Get API telemetry instance."""
    tracer = get_data_pipeline_tracer().get_tracer()
    return APITelemetry(tracer)


def get_data_quality_telemetry() -> DataQualityTelemetry:
    """Get data quality telemetry instance."""
    tracer = get_data_pipeline_tracer().get_tracer()
    return DataQualityTelemetry(tracer)


# Convenience decorators for common operations
def trace_kafka_producer_operation(topic: str, operation: str = "produce"):
    """Decorator for tracing Kafka producer operations."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            telemetry = get_kafka_telemetry()
            with telemetry.trace_producer_operation(topic, operation):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def trace_seatunnel_job_execution(job_name: str, job_config_path: str):
    """Decorator for tracing SeaTunnel job execution."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            telemetry = get_seatunnel_telemetry()
            with telemetry.trace_seatunnel_job(job_name, job_config_path):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def trace_api_call(api_name: str, endpoint: str, method: str = "GET"):
    """Decorator for tracing API calls."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            telemetry = get_api_telemetry()
            with telemetry.trace_api_call(api_name, endpoint, method):
                return func(*args, **kwargs)
        return wrapper
    return decorator
