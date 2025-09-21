"""OpenTelemetry configuration helpers for Aurum services."""
from __future__ import annotations

import logging
import os
import socket
from typing import Optional, Any

try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry._logs import set_logger_provider
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
    from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
    from opentelemetry.instrumentation.logging import LoggingInstrumentor
    from opentelemetry.instrumentation.requests import RequestsInstrumentor
    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover - telemetry optional
    trace = None  # type: ignore[assignment]

    class _NoOpSpan:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def set_attribute(self, *args, **kwargs):
            return None

        def is_recording(self) -> bool:
            return False

    class _NoOpTracer:
        def start_as_current_span(self, _name: str):
            return _NoOpSpan()

    class _NoOpTraceModule:
        @staticmethod
        def get_tracer(_name: str) -> _NoOpTracer:
            return _NoOpTracer()

        @staticmethod
        def get_tracer_provider() -> None:
            return None

        @staticmethod
        def set_tracer_provider(_provider) -> None:
            return None

    trace = _NoOpTraceModule()  # type: ignore[assignment]
    Resource = TracerProvider = BatchSpanProcessor = TraceIdRatioBased = OTLPSpanExporter = None  # type: ignore
    set_logger_provider = lambda *args, **kwargs: None  # type: ignore
    LoggerProvider = BatchLogRecordProcessor = OTLPLogExporter = None  # type: ignore

    class _NoOpInstrumentor:
        def instrument(self, *args, **kwargs):
            return None

    LoggingInstrumentor = RequestsInstrumentor = _NoOpInstrumentor  # type: ignore
    OTEL_AVAILABLE = False

try:  # Optional instrumentation depending on extras installed
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor  # type: ignore
except Exception:  # pragma: no cover - optional dep
    FastAPIInstrumentor = None  # type: ignore

try:  # Optional instrumentation
    from opentelemetry.instrumentation.psycopg import PsycopgInstrumentation  # type: ignore
except Exception:  # pragma: no cover - optional dep
    PsycopgInstrumentation = None  # type: ignore

_logger = logging.getLogger(__name__)
_INITIALISED = False


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _sampler() -> TraceIdRatioBased:
    ratio_raw = os.getenv("AURUM_OTEL_SAMPLER_RATIO", "1.0")
    try:
        ratio = float(ratio_raw)
    except ValueError:
        ratio = 1.0
    ratio = max(0.0, min(1.0, ratio))
    return TraceIdRatioBased(ratio)


def configure_telemetry(
    service_name: str,
    *,
    fastapi_app=None,
    enable_requests: bool = True,
    enable_psycopg: bool = False,
) -> None:
    """Initialise tracing and logging exporters if configured via environment."""
    global _INITIALISED
    if not OTEL_AVAILABLE:
        return None
    if not _INITIALISED:
        resolved_service_name = os.getenv("AURUM_OTEL_SERVICE_NAME", service_name)
        namespace = os.getenv("AURUM_OTEL_SERVICE_NAMESPACE", "aurum")
        instance_id = os.getenv("AURUM_OTEL_SERVICE_INSTANCE_ID", socket.gethostname())
        resource = Resource.create(
            {
                "service.name": resolved_service_name,
                "service.namespace": namespace,
                "service.instance.id": instance_id,
            }
        )
        provider = TracerProvider(resource=resource, sampler=_sampler())

        endpoint = os.getenv("AURUM_OTEL_EXPORTER_ENDPOINT")
        if endpoint:
            insecure = _bool_env("AURUM_OTEL_EXPORTER_INSECURE", True)
            try:
                span_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=insecure)
                provider.add_span_processor(BatchSpanProcessor(span_exporter))
            except Exception as exc:  # pragma: no cover - exporter misconfig
                _logger.warning("Failed to initialise OTLP span exporter: %s", exc)
            try:
                log_exporter = OTLPLogExporter(endpoint=endpoint, insecure=insecure)
                logger_provider = LoggerProvider(resource=resource)
                logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
                set_logger_provider(logger_provider)
            except Exception as exc:  # pragma: no cover - exporter misconfig
                _logger.warning("Failed to initialise OTLP log exporter: %s", exc)

        trace.set_tracer_provider(provider)

        try:
            LoggingInstrumentor().instrument(set_logging_format=True)
        except Exception as exc:  # pragma: no cover - instrumentation failure
            _logger.warning("Failed to instrument logging: %s", exc)
        if enable_requests:
            try:
                RequestsInstrumentor().instrument()
            except Exception as exc:  # pragma: no cover - instrumentation failure
                _logger.debug("requests instrumentation not available: %s", exc)
        if enable_psycopg and PsycopgInstrumentation:
            try:
                PsycopgInstrumentation().instrument()
            except Exception as exc:  # pragma: no cover - psycopg instrumentation missing
                _logger.debug("psycopg instrumentation not available: %s", exc)
        _INITIALISED = True

    if fastapi_app is not None and FastAPIInstrumentor is not None:
        try:
            FastAPIInstrumentor.instrument_app(fastapi_app, tracer_provider=trace.get_tracer_provider())
        except Exception as exc:  # pragma: no cover - instrumentation failure
            _logger.warning("Failed to instrument FastAPI app: %s", exc)


def get_tracer(name: str) -> Any:
    """Return a tracer bound to the configured provider."""
    return trace.get_tracer(name)


__all__ = ["configure_telemetry", "get_tracer"]
