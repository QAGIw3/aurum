from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field
from aurum.scenarios.models import ScenarioAssumption


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
    counts: Optional["DimensionsCountData"] = None


class DimensionCount(BaseModel):
    value: str
    count: int = Field(ge=0)


class DimensionsCountData(BaseModel):
    asset_class: list[DimensionCount] | None = None
    iso: list[DimensionCount] | None = None
    location: list[DimensionCount] | None = None
    market: list[DimensionCount] | None = None
    product: list[DimensionCount] | None = None
    block: list[DimensionCount] | None = None
    tenor_type: list[DimensionCount] | None = None


class IsoLocationOut(BaseModel):
    iso: str
    location_id: str
    location_name: Optional[str] = None
    location_type: Optional[str] = None
    zone: Optional[str] = None
    hub: Optional[str] = None
    timezone: Optional[str] = None


class IsoLocationsResponse(BaseModel):
    meta: Meta
    data: list[IsoLocationOut]


class IsoLocationResponse(BaseModel):
    meta: Meta
    data: IsoLocationOut


class UnitsCanonical(BaseModel):
    currencies: list[str]
    units: list[str]


class UnitsCanonicalResponse(BaseModel):
    meta: Meta
    data: UnitsCanonical


class UnitMappingOut(BaseModel):
    units_raw: str
    currency: str | None = None
    per_unit: str | None = None


class UnitsMappingResponse(BaseModel):
    meta: Meta
    data: list[UnitMappingOut]


class CalendarOut(BaseModel):
    name: str
    timezone: str


class CalendarsResponse(BaseModel):
    meta: Meta
    data: list[CalendarOut]


class CalendarBlocksResponse(BaseModel):
    meta: Meta
    data: list[str]


class CalendarHoursResponse(BaseModel):
    meta: Meta
    data: list[str]


# EIA catalog models


class EiaDatasetBriefOut(BaseModel):
    path: str
    name: str | None = None
    description: str | None = None
    default_frequency: str | None = None
    start_period: str | None = None
    end_period: str | None = None


class EiaDatasetsResponse(BaseModel):
    meta: Meta
    data: list[EiaDatasetBriefOut]


class EiaDatasetDetailOut(BaseModel):
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


class EiaDatasetResponse(BaseModel):
    meta: Meta
    data: EiaDatasetDetailOut

__all__ = [
    "Meta",
    "CurvePoint",
    "CurveResponse",
    "CurveDiffPoint",
    "CurveDiffResponse",
    "DimensionsData",
    "DimensionsResponse",
    "DimensionCount",
    "DimensionsCountData",
    "IsoLocationOut",
    "IsoLocationsResponse",
    "IsoLocationResponse",
    "UnitsCanonical",
    "UnitsCanonicalResponse",
    "UnitMappingOut",
    "UnitsMappingResponse",
    "CalendarOut",
    "CalendarsResponse",
    "CalendarBlocksResponse",
    "CalendarHoursResponse",
    "EiaDatasetBriefOut",
    "EiaDatasetsResponse",
    "EiaDatasetDetailOut",
    "EiaDatasetResponse",
]

# Scenario models


class CreateScenarioRequest(BaseModel):
    tenant_id: str
    name: str
    description: Optional[str] = None
    assumptions: list[ScenarioAssumption]


class ScenarioData(BaseModel):
    scenario_id: str
    tenant_id: str
    name: str
    description: Optional[str] = None
    status: str
    assumptions: list[ScenarioAssumption]
    created_at: Optional[str] = None


class ScenarioResponse(BaseModel):
    meta: Meta
    data: ScenarioData


class ScenarioListResponse(BaseModel):
    meta: Meta
    data: List[ScenarioData]


class ScenarioRunOptions(BaseModel):
    code_version: Optional[str] = None
    seed: Optional[int] = None


class ScenarioRunData(BaseModel):
    run_id: str
    scenario_id: str
    state: str
    code_version: Optional[str] = None
    seed: Optional[int] = None
    created_at: Optional[str] = None


class ScenarioRunResponse(BaseModel):
    meta: Meta
    data: ScenarioRunData


class ScenarioRunListResponse(BaseModel):
    meta: Meta
    data: List[ScenarioRunData]


class ScenarioOutputPoint(BaseModel):
    scenario_id: str
    asof_date: Optional[date] = None
    curve_key: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    tenor_label: Optional[str] = None
    metric: str
    value: Optional[float] = None
    band_lower: Optional[float] = None
    band_upper: Optional[float] = None
    attribution: Optional[dict[str, float]] = None
    version_hash: Optional[str] = None


class ScenarioOutputResponse(BaseModel):
    meta: Meta
    data: list[ScenarioOutputPoint]


# PPA valuation models


class PpaValuationRequest(BaseModel):
    ppa_contract_id: str
    scenario_id: Optional[str] = None
    asof_date: Optional[date] = None
    options: Optional[dict] = None


class PpaMetric(BaseModel):
    period_start: date
    period_end: date
    metric: str
    value: float
    currency: Optional[str] = None


class PpaValuationResponse(BaseModel):
    meta: Meta
    data: list[PpaMetric]


__all__ += [
    "CreateScenarioRequest",
    "ScenarioData",
    "ScenarioResponse",
    "ScenarioRunOptions",
    "ScenarioRunData",
    "ScenarioRunResponse",
    "ScenarioOutputPoint",
    "ScenarioOutputResponse",
    "PpaValuationRequest",
    "PpaMetric",
    "PpaValuationResponse",
]
