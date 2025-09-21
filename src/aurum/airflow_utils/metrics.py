"""Lightweight StatsD helpers for Airflow instrumentation."""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Dict, Mapping, Optional

try:  # Optional dependency
    from statsd import StatsClient  # type: ignore
except Exception:  # pragma: no cover - statsd optional
    StatsClient = None  # type: ignore

_CLIENT: Optional[StatsClient] = None


def _get_client() -> Optional[StatsClient]:
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    if StatsClient is None:
        return None
    host = os.getenv("STATSD_HOST")
    if not host:
        return None
    port = int(os.getenv("STATSD_PORT", "8125"))
    prefix = os.getenv("STATSD_PREFIX", "aurum")
    try:
        _CLIENT = StatsClient(host=host, port=port, prefix=prefix)
    except Exception:
        _CLIENT = None
    return _CLIENT


def _format_metric(metric: str, tags: Optional[Mapping[str, str]]) -> str:
    if not tags:
        return metric
    suffix = ".".join(f"{key}.{value}" for key, value in tags.items())
    return f"{metric}.{suffix}" if suffix else metric


def increment(metric: str, *, value: int = 1, tags: Optional[Mapping[str, str]] = None) -> None:
    client = _get_client()
    name = _format_metric(metric, tags)
    if client is None:
        print(f"[metrics] incr {name}={value}")
        return
    try:  # pragma: no cover - best effort
        client.incr(name, value)
    except Exception as exc:
        print(f"[metrics] incr failed for {name}: {exc}")


def gauge(metric: str, value: float, *, tags: Optional[Mapping[str, str]] = None) -> None:
    client = _get_client()
    name = _format_metric(metric, tags)
    if client is None:
        print(f"[metrics] gauge {name}={value}")
        return
    try:  # pragma: no cover - best effort
        client.gauge(name, value)
    except Exception as exc:
        print(f"[metrics] gauge failed for {name}: {exc}")


def timing(metric: str, millis: float, *, tags: Optional[Mapping[str, str]] = None) -> None:
    client = _get_client()
    name = _format_metric(metric, tags)
    if client is None:
        print(f"[metrics] timing {name}={millis}ms")
        return
    try:  # pragma: no cover - best effort
        client.timing(name, millis)
    except Exception as exc:
        print(f"[metrics] timing failed for {name}: {exc}")


def record_watermark_success(source: str, watermark: datetime) -> None:
    if watermark.tzinfo is None:
        watermark = watermark.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    latency_ms = max(0.0, (now - watermark).total_seconds() * 1000.0)
    tags: Dict[str, str] = {"source": source}
    increment("ingest.watermark_updates", tags=tags)
    timing("ingest.watermark_latency_ms", latency_ms, tags=tags)


__all__ = ["increment", "gauge", "timing", "record_watermark_success"]
