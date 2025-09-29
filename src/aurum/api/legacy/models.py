"""Minimal legacy model shims for test imports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class CurvePoint:
    curve_key: str = ""
    tenor_label: str = ""
    asof_date: Any = None
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    currency: Optional[str] = None
    per_unit: Optional[str] = None
    iso: Optional[str] = None
    market: Optional[str] = None
    location: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None


@dataclass
class CurveDiffPoint:
    curve_key: str = ""
    tenor_label: str = ""
    asof_date_a: Any = None
    asof_date_b: Any = None
    mid_a: Optional[float] = None
    mid_b: Optional[float] = None
    mid_diff: Optional[float] = None


@dataclass
class Meta:
    request_id: str = ""
    query_time_ms: int = 0
    extra: Dict[str, Any] = None


