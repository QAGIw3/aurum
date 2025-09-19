from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field


class Meta(BaseModel):
    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: str | None = None


class CurvePoint(BaseModel):
    curve_key: str
    tenor_label: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    asof_date: date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = None


class CurveResponse(BaseModel):
    meta: Meta
    data: List[CurvePoint]


class CurveDiffPoint(BaseModel):
    curve_key: str
    tenor_label: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    asof_a: date
    mid_a: Optional[float] = None
    asof_b: date
    mid_b: Optional[float] = None
    diff_abs: Optional[float] = None
    diff_pct: Optional[float] = None


class CurveDiffResponse(BaseModel):
    meta: Meta
    data: List[CurveDiffPoint]


class DimensionsData(BaseModel):
    asset_class: list[str] | None = None
    iso: list[str] | None = None
    location: list[str] | None = None
    market: list[str] | None = None
    product: list[str] | None = None
    block: list[str] | None = None
    tenor_type: list[str] | None = None


class DimensionsResponse(BaseModel):
    meta: Meta
    data: DimensionsData


__all__ = [
    "Meta",
    "CurvePoint",
    "CurveResponse",
    "CurveDiffPoint",
    "CurveDiffResponse",
    "DimensionsData",
    "DimensionsResponse",
]
