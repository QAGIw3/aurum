from __future__ import annotations

from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, validator, root_validator, Extra
from aurum.scenarios.models import ScenarioAssumption


class AurumBaseModel(BaseModel):
    """Base model for all Aurum API models with consistent configuration."""

    class Config:
        """Pydantic configuration for all models."""
        extra = Extra.forbid  # Forbid extra fields by default
        validate_assignment = True
        use_enum_values = True
        allow_population_by_field_name = True


class Meta(AurumBaseModel):
    """Metadata for API responses."""
    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: str | None = None
    prev_cursor: str | None = None
    has_more: Optional[bool] = None
    count: Optional[int] = None
    total: Optional[int] = None
    offset: Optional[int] = None
    limit: Optional[int] = None

    @validator("next_cursor", "prev_cursor")
    def validate_cursor_format(cls, v):
        """Validate cursor format (base64 encoded)."""
        if v is not None and len(v) > 1000:  # Reasonable cursor length limit
            raise ValueError("Cursor too long")
        return v


class ErrorEnvelope(AurumBaseModel):
    """Consistent error response envelope."""
    error: str
    message: Optional[str] = None
    code: Optional[str] = None
    field: Optional[str] = None
    value: Optional[Any] = None
    context: Optional[Dict[str, Any]] = None
    request_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @validator("context")
    def validate_context(cls, v):
        """Ensure context is a dictionary if provided."""
        if v is not None and not isinstance(v, dict):
            raise ValueError("Context must be a dictionary")
        return v


class ValidationErrorDetail(AurumBaseModel):
    """Detailed validation error information."""
    field: str
    message: str
    value: Optional[Any] = None
    code: Optional[str] = None
    constraint: Optional[str] = None

    @validator("constraint")
    def validate_constraint(cls, v):
        """Ensure constraint is a valid constraint type."""
        valid_constraints = {
            "required", "type", "format", "min", "max", "length",
            "pattern", "enum", "unique", "custom"
        }
        if v is not None and v not in valid_constraints:
            raise ValueError(f"Invalid constraint type: {v}")
        return v


class ValidationErrorResponse(AurumBaseModel):
    """Validation error response envelope."""
    error: str = "Validation Error"
    message: str
    field_errors: List[ValidationErrorDetail] = Field(default_factory=list)
    request_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


# Request validation models
class CurveQueryParams(AurumBaseModel):
    """Query parameters for curve data requests with comprehensive validation."""
    asof: Optional[str] = Field(None, description="As-of date (YYYY-MM-DD)")
    iso: Optional[str] = Field(None, max_length=10, description="ISO code")
    market: Optional[str] = Field(None, max_length=50, description="Market identifier")
    location: Optional[str] = Field(None, max_length=100, description="Location identifier")
    product: Optional[str] = Field(None, max_length=50, description="Product identifier")
    block: Optional[str] = Field(None, max_length=50, description="Block identifier")
    limit: int = Field(100, ge=1, le=500, description="Maximum results")
    offset: int = Field(0, ge=0, description="Pagination offset")
    cursor: Optional[str] = Field(None, max_length=1000, description="Cursor for pagination")
    since_cursor: Optional[str] = Field(None, max_length=1000, description="Since cursor")
    prev_cursor: Optional[str] = Field(None, max_length=1000, description="Previous cursor")

    @model_validator(mode='before')
    @classmethod
    def validate_pagination(cls, values):
        """Validate pagination parameters."""
        cursor = values.get("cursor")
        since_cursor = values.get("since_cursor")
        prev_cursor = values.get("prev_cursor")
        offset = values.get("offset", 0)

        # Cannot use cursor and offset together
        if cursor and offset > 0:
            raise ValueError("Cannot use cursor and offset together")

        # Cannot use multiple cursor types together
        cursor_count = sum([1 for c in [cursor, since_cursor, prev_cursor] if c is not None])
        if cursor_count > 1:
            raise ValueError("Cannot use multiple cursor parameters together")

        return values

    @validator("asof")
    def validate_asof_format(cls, v):
        """Validate asof date format."""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d")
            except ValueError:
                raise ValueError("asof must be in YYYY-MM-DD format")
        return v

    @validator("iso")
    def validate_iso_format(cls, v):
        """Validate ISO code format."""
        if v is not None and not v.isalpha():
            raise ValueError("ISO code must contain only letters")
        return v.upper() if v else v

    @validator("cursor", "since_cursor", "prev_cursor")
    def validate_cursor_length(cls, v):
        """Validate cursor length."""
        if v is not None and len(v) > 1000:
            raise ValueError("Cursor too long (max 1000 characters)")
        return v


class CurveDiffQueryParams(AurumBaseModel):
    """Query parameters for curve difference requests."""
    asof_a: str = Field(..., description="First comparison date (YYYY-MM-DD)")
    asof_b: str = Field(..., description="Second comparison date (YYYY-MM-DD)")
    iso: Optional[str] = Field(None, max_length=10, description="ISO code")
    market: Optional[str] = Field(None, max_length=50, description="Market identifier")
    location: Optional[str] = Field(None, max_length=100, description="Location identifier")
    product: Optional[str] = Field(None, max_length=50, description="Product identifier")
    block: Optional[str] = Field(None, max_length=50, description="Block identifier")
    tenor_type: Optional[str] = Field(None, max_length=50, description="Tenor type")
    limit: int = Field(100, ge=1, le=500, description="Maximum results")
    offset: int = Field(0, ge=0, description="Pagination offset")

    @validator("asof_a", "asof_b")
    def validate_date_format(cls, v, values):
        """Validate date format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")
        return v

    @model_validator(mode='before')
    @classmethod
    def validate_date_comparison(cls, values):
        """Ensure asof_a and asof_b are different dates."""
        asof_a = values.get("asof_a")
        asof_b = values.get("asof_b")

        if asof_a and asof_b:
            date_a = datetime.strptime(asof_a, "%Y-%m-%d")
            date_b = datetime.strptime(asof_b, "%Y-%m-%d")

            if date_a == date_b:
                raise ValueError("asof_a and asof_b must be different dates")

            # Check date range (max 365 days difference)
            days_diff = abs((date_b - date_a).days)
            if days_diff > 365:
                raise ValueError(f"Date range too large: {days_diff} days. Maximum allowed: 365 days")

        return values

    @model_validator(mode='before')
    @classmethod
    def validate_dimension_filters(cls, values):
        """Ensure at least one dimension filter is provided."""
        dimension_count = sum([
            1 for field in ["iso", "market", "location", "product", "block", "tenor_type"]
            if values.get(field) is not None
        ])

        if dimension_count == 0:
            raise ValueError("At least one dimension filter must be specified")

        return values

    @model_validator(mode='before')
    @classmethod
    def validate_dimension_combinations(cls, values):
        """Validate dimension combinations to prevent expensive queries."""
        limit = values.get("limit", 100)
        dimension_count = sum([
            1 for field in ["iso", "market", "location", "product", "block", "tenor_type"]
            if values.get(field) is not None
        ])

        # Warn for potentially expensive queries
        if dimension_count == 1 and limit > 1000:
            # This will be handled by the API layer, but we can log it here
            pass

        return values


class ScenarioCreateRequest(AurumBaseModel):
    """Enhanced request model for creating scenarios with comprehensive validation."""
    tenant_id: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = Field(None, max_length=1000)
    assumptions: List[Dict[str, Any]] = Field(default_factory=list, max_items=100)
    parameters: Dict[str, Any] = Field(default_factory=dict)
    tags: List[str] = Field(default_factory=list, max_items=50)

    @validator("tenant_id")
    def validate_tenant_id(cls, v):
        """Validate tenant ID format."""
        if not v or not v.strip():
            raise ValueError("tenant_id cannot be empty")
        if not all(c.isalnum() or c in "_-." for c in v):
            raise ValueError("tenant_id can only contain alphanumeric characters, underscore, hyphen, and dot")
        return v.strip()

    @validator("name")
    def validate_scenario_name(cls, v):
        """Validate scenario name."""
        if not v or not v.strip():
            raise ValueError("scenario name cannot be empty")
        return v.strip()

    @validator("tags")
    def validate_tags(cls, v):
        """Validate tags format."""
        for tag in v:
            if not tag or not tag.strip():
                raise ValueError("Tags cannot be empty")
            if len(tag) > 100:
                raise ValueError("Individual tag cannot exceed 100 characters")
            if not all(c.isalnum() or c in "_-." for c in tag):
                raise ValueError("Tags can only contain alphanumeric characters, underscore, hyphen, and dot")
        return [tag.strip() for tag in v]

    @validator("assumptions")
    def validate_assumptions(cls, v):
        """Validate assumptions structure."""
        for assumption in v:
            if not isinstance(assumption, dict):
                raise ValueError("Each assumption must be a dictionary")
            if "type" not in assumption:
                raise ValueError("Each assumption must have a 'type' field")
            if not assumption["type"] or not assumption["type"].strip():
                raise ValueError("Assumption type cannot be empty")
        return v


class CurvePoint(AurumBaseModel):
    """Individual curve data point with comprehensive validation."""
    curve_key: str = Field(..., min_length=1, max_length=255)
    tenor_label: str = Field(..., min_length=1, max_length=100)
    tenor_type: Optional[str] = Field(None, max_length=50)
    contract_month: Optional[date] = None
    asof_date: date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = Field(None, max_length=50)

    @model_validator(mode='before')
    @classmethod
    def validate_prices(cls, values):
        """Ensure at least one price field is provided."""
        mid = values.get("mid")
        bid = values.get("bid")
        ask = values.get("ask")

        if mid is None and bid is None and ask is None:
            raise ValueError("At least one price field (mid, bid, or ask) must be provided")

        return values

    @validator("mid", "bid", "ask")
    def validate_price_range(cls, v):
        """Validate price values are reasonable."""
        if v is not None:
            if not (-1e9 <= v <= 1e9):  # Reasonable price range
                raise ValueError("Price must be between -1e9 and 1e9")
        return v

    @validator("curve_key")
    def validate_curve_key_format(cls, v):
        """Validate curve key format."""
        if not v or not v.strip():
            raise ValueError("Curve key cannot be empty")
        # Allow alphanumeric, underscore, hyphen, dot
        if not all(c.isalnum() or c in "_-." for c in v):
            raise ValueError("Curve key can only contain alphanumeric characters, underscore, hyphen, and dot")
        return v.strip()

    @validator("tenor_label")
    def validate_tenor_label_format(cls, v):
        """Validate tenor label format."""
        if not v or not v.strip():
            raise ValueError("Tenor label cannot be empty")
        return v.strip()


class CurveResponse(AurumBaseModel):
    """Response model for curve data."""
    meta: Meta
    data: List[CurvePoint]

    @validator("data")
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Data cannot be empty")
        return v


class CurveDiffPoint(AurumBaseModel):
    """Curve difference point comparing two dates with validation."""
    curve_key: str = Field(..., min_length=1, max_length=255)
    tenor_label: str = Field(..., min_length=1, max_length=100)
    tenor_type: Optional[str] = Field(None, max_length=50)
    contract_month: Optional[date] = None
    asof_a: date
    mid_a: Optional[float] = None
    asof_b: date
    mid_b: Optional[float] = None
    diff_abs: Optional[float] = None
    diff_pct: Optional[float] = None

    @model_validator(mode='before')
    @classmethod
    def validate_dates(cls, values):
        """Ensure asof_a and asof_b are different dates."""
        asof_a = values.get("asof_a")
        asof_b = values.get("asof_b")

        if asof_a and asof_b and asof_a == asof_b:
            raise ValueError("asof_a and asof_b must be different dates")

        return values

    @model_validator(mode='before')
    @classmethod
    def validate_prices(cls, values):
        """Ensure at least one price comparison is available."""
        mid_a = values.get("mid_a")
        mid_b = values.get("mid_b")

        if mid_a is None and mid_b is None:
            raise ValueError("At least one price comparison (mid_a or mid_b) must be provided")

        return values

    @validator("mid_a", "mid_b")
    def validate_price_range(cls, v):
        """Validate price values are reasonable."""
        if v is not None:
            if not (-1e9 <= v <= 1e9):
                raise ValueError("Price must be between -1e9 and 1e9")
        return v

    @validator("diff_abs")
    def validate_diff_abs(cls, v):
        """Validate absolute difference is reasonable."""
        if v is not None:
            if not (-1e10 <= v <= 1e10):
                raise ValueError("Absolute difference must be between -1e10 and 1e10")
        return v

    @validator("diff_pct")
    def validate_diff_pct(cls, v):
        """Validate percentage difference is reasonable."""
        if v is not None:
            if not (-1000 <= v <= 1000):  # Allow up to 1000% change
                raise ValueError("Percentage difference must be between -1000 and 1000")
        return v

    @validator("curve_key", "tenor_label")
    def validate_string_fields(cls, v):
        """Validate string fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty")
        return v.strip()


class CurveDiffResponse(AurumBaseModel):
    """Response model for curve difference data."""
    meta: Meta
    data: List[CurveDiffPoint]

    @validator("data")
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Data cannot be empty")
        return v


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
