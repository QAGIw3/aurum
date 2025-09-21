"""Stub metrics helpers."""
from __future__ import annotations

from datetime import datetime
from typing import Mapping, Optional


def increment(metric: str, *, value: int = 1, tags: Optional[Mapping[str, str]] = None) -> None:  # pragma: no cover
    print(f"[stub metrics] incr {metric} value={value} tags={dict(tags or {})}")


def gauge(metric: str, value: float, *, tags: Optional[Mapping[str, str]] = None) -> None:  # pragma: no cover
    print(f"[stub metrics] gauge {metric} value={value} tags={dict(tags or {})}")


def timing(metric: str, millis: float, *, tags: Optional[Mapping[str, str]] = None) -> None:  # pragma: no cover
    print(f"[stub metrics] timing {metric} value={millis} tags={dict(tags or {})}")


def record_watermark_success(source: str, watermark: datetime) -> None:  # pragma: no cover
    print(f"[stub metrics] watermark source={source} watermark={watermark}")


__all__ = [
    "increment",
    "gauge",
    "timing",
    "record_watermark_success",
]
