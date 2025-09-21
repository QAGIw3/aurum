from __future__ import annotations

from datetime import date, datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field
from aurum.scenarios.models import ScenarioAssumption


class Meta(BaseModel):
    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: str | None = None
    prev_cursor: str | None = None


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


# ISO LMP models


class IsoLmpPoint(BaseModel):
    iso_code: str
    market: str
    delivery_date: date
    interval_start: datetime
    interval_end: datetime | None = None
    interval_minutes: int | None = None
    location_id: str
    location_name: str | None = None
    location_type: str | None = None
    price_total: float
    price_energy: float | None = None
    price_congestion: float | None = None
    price_loss: float | None = None
    currency: str
    uom: str
    settlement_point: str | None = None
    source_run_id: str | None = None
    ingest_ts: datetime | None = None
    record_hash: str
    metadata: dict[str, str] | None = None


class IsoLmpResponse(BaseModel):
    meta: Meta
    data: list[IsoLmpPoint]


class IsoLmpAggregatePoint(BaseModel):
    iso_code: str
    market: str
    interval_start: datetime
    location_id: str
    currency: str
    uom: str
    price_avg: float | None = None
    price_min: float | None = None
    price_max: float | None = None
    price_stddev: float | None = None
    sample_count: int


class IsoLmpAggregateResponse(BaseModel):
    meta: Meta
    data: list[IsoLmpAggregatePoint]


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


class EiaSeriesPoint(BaseModel):
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


class EiaSeriesResponse(BaseModel):
    meta: Meta
    data: list[EiaSeriesPoint]


class EiaSeriesDimensionsData(BaseModel):
    dataset: list[str] | None = None
    area: list[str] | None = None
    sector: list[str] | None = None
    unit: list[str] | None = None
    canonical_unit: list[str] | None = None
    canonical_currency: list[str] | None = None
    frequency: list[str] | None = None
    source: list[str] | None = None


class EiaSeriesDimensionsResponse(BaseModel):
    meta: Meta
    data: EiaSeriesDimensionsData


class DroughtIndexPoint(BaseModel):
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

class DroughtIndexResponse(BaseModel):
    meta: Meta
    data: list[DroughtIndexPoint]


class DroughtUsdmPoint(BaseModel):
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


class DroughtUsdmResponse(BaseModel):
    meta: Meta
    data: list[DroughtUsdmPoint]


class DroughtVectorEventPoint(BaseModel):
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


class DroughtVectorResponse(BaseModel):
    meta: Meta
    data: list[DroughtVectorEventPoint]


class DroughtDimensions(BaseModel):
    datasets: list[str]
    indices: list[str]
    timescales: list[str]
    layers: list[str]
    region_types: list[str]


class DroughtDimensionsResponse(BaseModel):
    meta: Meta
    data: DroughtDimensions


class DroughtInfoResponse(BaseModel):
    meta: Meta
    data: dict[str, Any]

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
    "EiaSeriesPoint",
    "EiaSeriesResponse",
    "EiaSeriesDimensionsData",
    "EiaSeriesDimensionsResponse",
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
    run_id: Optional[str] = None
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


class ScenarioMetricLatest(BaseModel):
    scenario_id: str
    metric: str
    curve_key: Optional[str] = None
    tenor_label: Optional[str] = None
    latest_value: Optional[float] = None
    latest_band_lower: Optional[float] = None
    latest_band_upper: Optional[float] = None
    latest_asof_date: Optional[date] = None


class ScenarioMetricLatestResponse(BaseModel):
    meta: Meta
    data: list[ScenarioMetricLatest]


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
    unit: Optional[str] = None
    run_id: Optional[str] = None
    curve_key: Optional[str] = None
    tenor_type: Optional[str] = None


class PpaValuationResponse(BaseModel):
    meta: Meta
    data: list[PpaMetric]


class PpaContractBase(BaseModel):
    instrument_id: str | None = None
    terms: dict[str, Any] = Field(default_factory=dict)


class PpaContractCreate(PpaContractBase):
    pass


class PpaContractUpdate(BaseModel):
    instrument_id: str | None = None
    terms: dict[str, Any] | None = None


class PpaContractOut(PpaContractBase):
    ppa_contract_id: str
    tenant_id: str
    created_at: datetime
    updated_at: datetime


class PpaContractResponse(BaseModel):
    meta: Meta
    data: PpaContractOut


class PpaContractListResponse(BaseModel):
    meta: Meta
    data: list[PpaContractOut]


class PpaValuationRecord(BaseModel):
    asof_date: date | None = None
    scenario_id: str | None = None
    period_start: date | None = None
    period_end: date | None = None
    metric: str | None = None
    value: float | None = None
    cashflow: float | None = None
    npv: float | None = None
    irr: float | None = None
    curve_key: str | None = None
    version_hash: str | None = None
    ingested_at: datetime | None = None


class PpaValuationListResponse(BaseModel):
    meta: Meta
    data: list[PpaValuationRecord]


class CachePurgeDetail(BaseModel):
    scope: str
    redis_keys_removed: int
    local_entries_removed: int


class CachePurgeResponse(BaseModel):
    meta: Meta
    data: list[CachePurgeDetail]


__all__ += [
    "CreateScenarioRequest",
    "ScenarioData",
    "ScenarioResponse",
    "ScenarioRunOptions",
    "ScenarioRunData",
    "ScenarioRunResponse",
    "ScenarioOutputPoint",
    "ScenarioOutputResponse",
    "ScenarioMetricLatest",
    "ScenarioMetricLatestResponse",
    "PpaValuationRequest",
    "PpaMetric",
    "PpaValuationResponse",
    "PpaContractCreate",
    "PpaContractUpdate",
    "PpaContractOut",
    "PpaContractResponse",
    "PpaContractListResponse",
    "PpaValuationRecord",
    "PpaValuationListResponse",
    "CachePurgeDetail",
    "CachePurgeResponse",
]
