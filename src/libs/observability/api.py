"""Central observability API (tracing/metrics/logging) for API and workers.

This module re-exports the observability manager and helpers to provide a
single, stable import path: `libs.observability.api`.
"""

from __future__ import annotations

from .common.observability import (
    ObservabilityManager,
    configure_observability,
    get_observability,
    trace_operation,
)

__all__ = [
    "ObservabilityManager",
    "configure_observability",
    "get_observability",
    "trace_operation",
]


