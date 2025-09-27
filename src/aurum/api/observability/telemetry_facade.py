"""Compatibility wrapper for telemetry facade helpers."""

from __future__ import annotations

from ..observability import telemetry_facade as _telemetry_facade

__all__ = getattr(_telemetry_facade, "__all__", [])  # type: ignore[var-annotated]

globals().update({name: getattr(_telemetry_facade, name) for name in __all__})

# Ensure the commonly used entry points are available even if ``__all__`` is
# not defined upstream.
get_telemetry_facade = getattr(_telemetry_facade, "get_telemetry_facade", lambda *_, **__: None)
TelemetryFacade = getattr(_telemetry_facade, "TelemetryFacade", None)
MetricCategory = getattr(_telemetry_facade, "MetricCategory", None)
