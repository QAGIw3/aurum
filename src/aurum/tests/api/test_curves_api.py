"""Compatibility shim for test utilities expected under ``aurum.tests``."""
from __future__ import annotations

import sys
import types

__all__ = ["_ensure_opentelemetry"]


def _ensure_opentelemetry(monkeypatch):
    """Patch opentelemetry modules for test environments without the SDK."""

    stub_span = types.SimpleNamespace(
        set_attribute=lambda *args, **kwargs: None,
        is_recording=lambda: False,
    )

    trace_module = types.ModuleType("opentelemetry.trace")
    trace_module.get_current_span = lambda: stub_span
    trace_module.get_tracer_provider = lambda: None
    trace_module.get_tracer = lambda *_args, **_kwargs: None
    trace_module.set_tracer_provider = lambda *_args, **_kwargs: None

    propagate_module = types.ModuleType("opentelemetry.propagate")
    propagate_module.inject = lambda *_args, **_kwargs: None

    resources_module = types.ModuleType("opentelemetry.sdk.resources")

    class _Resource:
        @staticmethod
        def create(attrs):
            return attrs

    resources_module.Resource = _Resource

    sdk_trace_module = types.ModuleType("opentelemetry.sdk.trace")

    class _TracerProvider:
        def __init__(self, resource=None, sampler=None):
            self.resource = resource
            self.sampler = sampler

        def add_span_processor(self, _processor):
            return None

    sdk_trace_module.TracerProvider = _TracerProvider

    trace_export_module = types.ModuleType("opentelemetry.sdk.trace.export")

    class _BatchSpanProcessor:
        def __init__(self, _exporter):
            pass

    trace_export_module.BatchSpanProcessor = _BatchSpanProcessor

    sampling_module = types.ModuleType("opentelemetry.sdk.trace.sampling")

    class _TraceIdRatioBased:
        def __init__(self, _ratio):
            self.ratio = _ratio

    sampling_module.TraceIdRatioBased = _TraceIdRatioBased

    otlp_trace_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc.trace_exporter")

    class _OTLPSpanExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_trace_module.OTLPSpanExporter = _OTLPSpanExporter

    logs_module = types.ModuleType("opentelemetry._logs")
    logs_module.set_logger_provider = lambda *_args, **_kwargs: None

    sdk_logs_module = types.ModuleType("opentelemetry.sdk._logs")

    class _LoggerProvider:
        def __init__(self, **_kwargs):
            pass

        def add_log_record_processor(self, _processor):
            return None

    sdk_logs_module.LoggerProvider = _LoggerProvider

    log_export_module = types.ModuleType("opentelemetry.sdk._logs.export")

    class _BatchLogRecordProcessor:
        def __init__(self, _exporter):
            pass

    log_export_module.BatchLogRecordProcessor = _BatchLogRecordProcessor

    otlp_log_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc._log_exporter")

    class _OTLPLogExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_log_module.OTLPLogExporter = _OTLPLogExporter

    monkeypatch.setitem(sys.modules, "opentelemetry.trace", trace_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.propagate", propagate_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.resources", resources_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace", sdk_trace_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace.export", trace_export_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk.trace.sampling", sampling_module)
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter",
        otlp_trace_module,
    )
    monkeypatch.setitem(sys.modules, "opentelemetry._logs", logs_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk._logs", sdk_logs_module)
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk._logs.export", log_export_module)
    monkeypatch.setitem(
        sys.modules,
        "opentelemetry.exporter.otlp.proto.grpc._log_exporter",
        otlp_log_module,
    )
