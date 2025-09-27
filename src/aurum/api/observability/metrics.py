"""Compatibility wrapper for observability metrics.

Historically ``aurum.api`` re-exported metrics helpers even though the
implementations reside in ``aurum.observability``.  This thin wrapper keeps the
older import path operational while delegating to the canonical module.
"""

from __future__ import annotations

from ..observability import metrics as _core_metrics

__all__ = getattr(_core_metrics, "__all__", [])  # type: ignore[var-annotated]

globals().update({name: getattr(_core_metrics, name) for name in __all__})

# Some callers rely on additional attributes not listed in ``__all__``.  To be
# safe, expose the common entry points explicitly.
get_metrics_client = getattr(_core_metrics, "get_metrics_client", lambda *_, **__: None)
MetricCategory = getattr(_core_metrics, "MetricCategory", None)
MetricType = getattr(_core_metrics, "MetricType", None)
