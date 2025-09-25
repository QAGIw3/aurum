from __future__ import annotations

"""Drought data response models."""

from datetime import date, datetime
from typing import Any

from .base import AurumBaseModel
from .common import Meta


class DroughtIndexPoint(AurumBaseModel):
    series_id: str
    dataset: str
    index: str
    timescale: str
    valid_date: date
    as_of: datetime | None = None
    value: float | None = None
    unit: str | None = None
    poc: str | None = None
    region_type: str
    region_id: str
    region_name: str | None = None
    parent_region_id: str | None = None
    source_url: str | None = None
    metadata: dict[str, Any] | None = None


class DroughtIndexResponse(AurumBaseModel):
    meta: Meta
    data: list[DroughtIndexPoint]


class DroughtUsdmPoint(AurumBaseModel):
    region_type: str
    region_id: str
    region_name: str | None = None
    parent_region_id: str | None = None
    valid_date: date
    as_of: datetime | None = None
    d0_frac: float | None = None
    d1_frac: float | None = None
    d2_frac: float | None = None
    d3_frac: float | None = None
    d4_frac: float | None = None
    source_url: str | None = None
    metadata: dict[str, Any] | None = None


class DroughtUsdmResponse(AurumBaseModel):
    meta: Meta
    data: list[DroughtUsdmPoint]


class DroughtVectorEventPoint(AurumBaseModel):
    layer: str
    event_id: str
    region_type: str | None = None
    region_id: str | None = None
    region_name: str | None = None
    parent_region_id: str | None = None
    valid_start: datetime | None = None
    valid_end: datetime | None = None
    value: float | None = None
    unit: str | None = None
    category: str | None = None
    severity: str | None = None
    source_url: str | None = None
    geometry_wkt: str | None = None
    properties: dict[str, Any] | None = None


class DroughtVectorResponse(AurumBaseModel):
    meta: Meta
    data: list[DroughtVectorEventPoint]


class DroughtDimensions(AurumBaseModel):
    datasets: list[str]
    indices: list[str]
    timescales: list[str]
    layers: list[str]
    region_types: list[str]


class DroughtDimensionsResponse(AurumBaseModel):
    meta: Meta
    data: DroughtDimensions


class DroughtInfoResponse(AurumBaseModel):
    meta: Meta
    data: dict[str, Any]


__all__ = [
    "DroughtIndexPoint",
    "DroughtIndexResponse",
    "DroughtUsdmPoint",
    "DroughtUsdmResponse",
    "DroughtVectorEventPoint",
    "DroughtVectorResponse",
    "DroughtDimensions",
    "DroughtDimensionsResponse",
    "DroughtInfoResponse",
]
