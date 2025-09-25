from __future__ import annotations

"""Administrative metadata schemas (calendars, units, series mappings)."""

from datetime import datetime
from typing import Any

from pydantic import Field

from .base import AurumBaseModel
from .common import Meta


class UnitsCanonical(AurumBaseModel):
    currencies: list[str]
    units: list[str]


class UnitsCanonicalResponse(AurumBaseModel):
    meta: Meta
    data: UnitsCanonical


class UnitMappingOut(AurumBaseModel):
    units_raw: str
    currency: str | None = None
    per_unit: str | None = None


class UnitsMappingResponse(AurumBaseModel):
    meta: Meta
    data: list[UnitMappingOut]


class SeriesCurveMappingCreate(AurumBaseModel):
    external_provider: str
    external_series_id: str
    curve_key: str
    mapping_confidence: float = Field(1.0, ge=0.0, le=1.0)
    mapping_method: str = Field("manual")
    mapping_notes: str | None = None
    is_active: bool = True


class SeriesCurveMappingUpdate(AurumBaseModel):
    mapping_confidence: float | None = Field(None, ge=0.0, le=1.0)
    mapping_method: str | None = None
    mapping_notes: str | None = None
    is_active: bool | None = None


class SeriesCurveMappingOut(AurumBaseModel):
    id: str
    external_provider: str
    external_series_id: str
    curve_key: str
    mapping_confidence: float
    mapping_method: str
    mapping_notes: str | None
    is_active: bool
    created_by: str
    created_at: datetime
    updated_by: str | None
    updated_at: datetime


class SeriesCurveMappingListResponse(AurumBaseModel):
    meta: Meta
    data: list[SeriesCurveMappingOut]


class SeriesCurveMappingSearchResponse(AurumBaseModel):
    meta: Meta
    data: list[dict[str, Any]]


class CalendarOut(AurumBaseModel):
    name: str
    timezone: str


class CalendarsResponse(AurumBaseModel):
    meta: Meta
    data: list[CalendarOut]


class CalendarBlocksResponse(AurumBaseModel):
    meta: Meta
    data: list[str]


class CalendarHoursResponse(AurumBaseModel):
    meta: Meta
    data: list[str]


__all__ = [
    "UnitsCanonical",
    "UnitsCanonicalResponse",
    "UnitMappingOut",
    "UnitsMappingResponse",
    "SeriesCurveMappingCreate",
    "SeriesCurveMappingUpdate",
    "SeriesCurveMappingOut",
    "SeriesCurveMappingListResponse",
    "SeriesCurveMappingSearchResponse",
    "CalendarOut",
    "CalendarsResponse",
    "CalendarBlocksResponse",
    "CalendarHoursResponse",
]
