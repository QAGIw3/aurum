"""Light-weight helpers for Prometheus metric collectors.

These helpers centralize collector lookup and creation so that multiple
components (middleware, database clients, etc.) can safely import the same
metrics without tripping Prometheus' duplicate registration safeguards. The
helpers fall back to no-op collectors when ``prometheus_client`` is absent so
modules can still be imported in constrained environments (docs builds, tests
without metrics, ...).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter, Gauge, Histogram, REGISTRY

    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in environments without prometheus
    PROMETHEUS_AVAILABLE = False

    class _NoOpCollector:
        """Minimal collector stub that mirrors Prometheus API."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401 - intentional no-op
            self._kwargs = kwargs

        def labels(self, **_labels: Any) -> "_NoOpCollector":
            return self

    class Counter(_NoOpCollector):  # type: ignore[misc]
        def inc(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class Histogram(_NoOpCollector):  # type: ignore[misc]
        def observe(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class Gauge(_NoOpCollector):  # type: ignore[misc]
        def set(self, *_args: Any, **_kwargs: Any) -> None:
            return None

    class _NoOpRegistry:
        _names_to_collectors: Dict[str, Any] = {}

    REGISTRY = _NoOpRegistry()  # type: ignore[assignment]


_MetricKey = Tuple[str, str, Tuple[str, ...], Tuple[Tuple[str, Any], ...], int]


def _normalize_labels(labelnames: Optional[Iterable[str]]) -> Tuple[str, ...]:
    return tuple(labelnames or ())


def _normalize_kwargs(kwargs: Dict[str, Any]) -> Tuple[Tuple[str, Any], ...]:
    if not kwargs:
        return ()

    normalized: Dict[str, Any] = {}
    for key, value in kwargs.items():
        if isinstance(value, (list, tuple)):
            normalized[key] = tuple(value)
        elif isinstance(value, dict):
            normalized[key] = tuple(sorted(value.items()))
        else:
            normalized[key] = value
    return tuple(sorted(normalized.items()))


def _metric_key(
    metric_type: str,
    name: str,
    labelnames: Optional[Iterable[str]],
    kwargs: Dict[str, Any],
    registry: Optional[Any],
) -> _MetricKey:
    labels = _normalize_labels(labelnames)
    kw_items = _normalize_kwargs(kwargs)
    registry_id = id(registry) if registry is not None else id(REGISTRY)
    return (metric_type, name, labels, kw_items, registry_id)


_METRIC_CACHE: Dict[_MetricKey, Any] = {}


def _get_existing_collector(name: str, registry: Optional[Any]) -> Any:
    if not PROMETHEUS_AVAILABLE:
        return None

    target_registry = registry or REGISTRY
    names_to_collectors = getattr(target_registry, "_names_to_collectors", None)
    if not names_to_collectors:
        return None
    return names_to_collectors.get(name)


def _get_collector(
    metric_type: str,
    name: str,
    documentation: str,
    labelnames: Optional[Iterable[str]],
    constructor,
    registry: Optional[Any] = None,
    **kwargs: Any,
) -> Any:
    key = _metric_key(metric_type, name, labelnames, kwargs, registry)
    cached = _METRIC_CACHE.get(key)
    if cached is not None:
        return cached

    collector = _get_existing_collector(name, registry)
    if collector is None:
        ctor_kwargs = dict(kwargs)
        labels = list(labelnames or [])
        collector = constructor(
            name,
            documentation,
            labels,
            registry=registry,
            **ctor_kwargs,
        )

    _METRIC_CACHE[key] = collector
    return collector


def get_counter(
    name: str,
    documentation: str,
    labelnames: Optional[Iterable[str]] = None,
    *,
    registry: Optional[Any] = None,
    **kwargs: Any,
) -> Counter:
    """Return a cached Counter collector (no duplicate registration)."""

    return _get_collector("counter", name, documentation, labelnames, Counter, registry, **kwargs)


def get_histogram(
    name: str,
    documentation: str,
    labelnames: Optional[Iterable[str]] = None,
    *,
    registry: Optional[Any] = None,
    **kwargs: Any,
) -> Histogram:
    """Return a cached Histogram collector."""

    return _get_collector("histogram", name, documentation, labelnames, Histogram, registry, **kwargs)


def get_gauge(
    name: str,
    documentation: str,
    labelnames: Optional[Iterable[str]] = None,
    *,
    registry: Optional[Any] = None,
    **kwargs: Any,
) -> Gauge:
    """Return a cached Gauge collector."""

    return _get_collector("gauge", name, documentation, labelnames, Gauge, registry, **kwargs)


def clear_metric_cache() -> None:
    """Clear the helper cache (useful for isolated tests)."""

    _METRIC_CACHE.clear()

