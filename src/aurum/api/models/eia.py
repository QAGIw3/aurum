from __future__ import annotations

"""EIA catalog and series response models."""

from datetime import datetime
from typing import Optional

from pydantic import Field

from .base import AurumBaseModel
from .common import Meta


class EiaDatasetBriefOut(AurumBaseModel):
    path: str
    name: str | None = None
    description: str | None = None
    default_frequency: str | None = None
    start_period: str | None = None
    end_period: str | None = None


class EiaDatasetsResponse(AurumBaseModel):
    meta: Meta
    data: list[EiaDatasetBriefOut]


class EiaDatasetDetailOut(AurumBaseModel):
    path: str
    name: str | None = None
    description: str | None = None
    frequencies: list[dict]
    facets: list[dict]
    data_columns: list[str]
    start_period: str | None = None
    end_period: str | None = None
    default_frequency: str | None = None
    default_date_format: str | None = None


class EiaDatasetResponse(AurumBaseModel):
    meta: Meta
    data: EiaDatasetDetailOut


class EiaSeriesPoint(AurumBaseModel):
    series_id: str
    period: str
    period_start: datetime
    period_end: datetime | None = None
    frequency: str | None = None
    value: float | None = None
    raw_value: str | None = None
    unit: str | None = None
    canonical_unit: str | None = None
    canonical_currency: str | None = None
    canonical_value: float | None = None
    conversion_factor: float | None = None
    area: str | None = None
    sector: str | None = None
    seasonal_adjustment: str | None = None
    description: str | None = None
    source: str | None = None
    dataset: str | None = None
    metadata: dict[str, str] | None = None
    ingest_ts: datetime | None = None


class EiaSeriesResponse(AurumBaseModel):
    meta: Meta
    data: list[EiaSeriesPoint]


class EiaSeriesDimensionsData(AurumBaseModel):
    dataset: list[str] | None = None
    area: list[str] | None = None
    sector: list[str] | None = None
    unit: list[str] | None = None
    canonical_unit: list[str] | None = None
    canonical_currency: list[str] | None = None
    frequency: list[str] | None = None
    source: list[str] | None = None


class EiaSeriesDimensionsResponse(AurumBaseModel):
    meta: Meta
    data: EiaSeriesDimensionsData


__all__ = [
    "EiaDatasetBriefOut",
    "EiaDatasetsResponse",
    "EiaDatasetDetailOut",
    "EiaDatasetResponse",
    "EiaSeriesPoint",
    "EiaSeriesResponse",
    "EiaSeriesDimensionsData",
    "EiaSeriesDimensionsResponse",
]
