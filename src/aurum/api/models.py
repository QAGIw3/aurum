from __future__ import annotations

"""API data models and validation helpers using Pydantic v2.

Defines canonical response envelopes, error payloads, and request parameter
schemas used across the HTTP surface. The models enforce strict validation and
offer clear error messages while remaining serialization‑friendly for clients.
"""

from datetime import datetime, timedelta
import datetime as dt
from enum import Enum
from typing import Any, Dict, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .http.pagination import DEFAULT_PAGE_SIZE, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE
from aurum.scenarios.models import ScenarioAssumption


class AurumBaseModel(BaseModel):
    """Base model for all Aurum API models with consistent configuration."""

    model_config = ConfigDict(
        extra="forbid",
        validate_assignment=True,
        use_enum_values=True,
        populate_by_name=True,
    )


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

    @field_validator("next_cursor", "prev_cursor)
    def validate_cursor_format(cls, v):
        """Validate cursor format (base64 encoded)."""
        if v is not None and len(v) > 1000:  # Reasonable cursor length limit
            raise ValueError("Cursor too long)
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

    @field_validator("context)
    def validate_context(cls, v):
        """Ensure context is a dictionary if provided."""
        if v is not None and not isinstance(v, dict):
            raise ValueError("Context must be a dictionary)
        return v


class ValidationErrorDetail(AurumBaseModel):
    """Detailed validation error information."""
    field: str
    message: str
    value: Optional[Any] = None
    code: Optional[str] = None
    constraint: Optional[str] = None

    @field_validator("constraint)
    def validate_constraint(cls, v):
        """Ensure constraint is a valid constraint type."""
        valid_constraints = {
            "required", "type", "format", "min", "max", "length",
            "pattern", "enum", "unique", "custom"
        }
        if v is not None and v not in valid_constraints:
            raise ValueError(f"Invalid constraint type: {v})
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
    asof: Optional[str] = Field(None, )
    iso: Optional[str] = Field(None, max_length=10, )
    market: Optional[str] = Field(None, max_length=50, )
    location: Optional[str] = Field(None, max_length=100, )
    product: Optional[str] = Field(None, max_length=50, )
    block: Optional[str] = Field(None, max_length=50, )
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, )
    offset: int = Field(0, ge=0, )
    cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH, )
    since_cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH, )
    prev_cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH, )

    @model_validator(mode="before)
    @classmethod
    def validate_pagination(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Validate pagination parameters."""
        cursor = values.get("cursor)
        since_cursor = values.get("since_cursor)
        prev_cursor = values.get("prev_cursor)
        offset = values.get("offset", 0)

        # Cannot use cursor and offset together
        if cursor and offset > 0:
            raise ValueError("Cannot use cursor and offset together)

        # Cannot use multiple cursor types together
        cursor_count = sum([1 for c in [cursor, since_cursor, prev_cursor] if c is not None])
        if cursor_count > 1:
            raise ValueError("Cannot use multiple cursor parameters together)

        return values

    @field_validator("asof)
    def validate_asof_format(cls, v):
        """Validate asof date format."""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d)
            except ValueError:
                raise ValueError("asof must be in YYYY-MM-DD format)
        return v

    @field_validator("iso)
    def validate_iso_format(cls, v):
        """Validate  format."""
        if v is not None and not v.isalpha():
            raise ValueError(" must contain only letters)
        return v.upper() if v else v

    @field_validator("cursor", "since_cursor", "prev_cursor)
    def validate_cursor_length(cls, v):
        """Validate cursor length."""
        if v is not None and len(v) > 1000:
            raise ValueError("Cursor too long (max 1000 characters))
        return v


class CurveDiffQueryParams(AurumBaseModel):
    """Query parameters for curve difference requests."""
    asof_a: str = Field(..., )
    asof_b: str = Field(..., )
    iso: Optional[str] = Field(None, max_length=10, )
    market: Optional[str] = Field(None, max_length=50, )
    location: Optional[str] = Field(None, max_length=100, )
    product: Optional[str] = Field(None, max_length=50, )
    block: Optional[str] = Field(None, max_length=50, )
    tenor_type: Optional[str] = Field(None, max_length=50, )
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, )
    offset: int = Field(0, ge=0, )

    @field_validator("asof_a", "asof_b)
    def validate_date_format(cls, v, values):
        """Validate date format."""
        try:
            datetime.strptime(v, "%Y-%m-%d)
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format)
        return v

    @model_validator(mode="before)
    @classmethod
    def validate_date_comparison(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure asof_a and asof_b are different dates."""
        asof_a = values.get("asof_a)
        asof_b = values.get("asof_b)

        if asof_a and asof_b:
            date_a = datetime.strptime(asof_a, "%Y-%m-%d)
            date_b = datetime.strptime(asof_b, "%Y-%m-%d)

            if date_a == date_b:
                raise ValueError("asof_a and asof_b must be different dates)

            # Check date range (max 365 days difference)
            days_diff = abs((date_b - date_a).days)
            if days_diff > 365:
                raise ValueError(f"Date range too large: {days_diff} days. Maximum allowed: 365 days)

        return values

    @model_validator(mode="before)
    @classmethod
    def validate_dimension_filters(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure at least one dimension filter is provided."""
        dimension_count = sum([
            1 for field in ["iso", "market", "location", "product", "block", "tenor_type"]
            if values.get(field) is not None
        ])

        if dimension_count == 0:
            raise ValueError("At least one dimension filter must be specified)

        return values

    @model_validator(mode="before)
    @classmethod
    def validate_dimension_combinations(cls, values: dict[str, Any]) -> dict[str, Any]:
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

    @field_validator("tenant_id)
    def validate_tenant_id(cls, v):
        """Validate tenant ID format."""
        if not v or not v.strip():
            raise ValueError("tenant_id cannot be empty)
        if not all(c.isalnum() or c in "_-." for c in v):
            raise ValueError("tenant_id can only contain alphanumeric characters, underscore, hyphen, and dot)
        return v.strip()

    @field_validator("name)
    def validate_scenario_name(cls, v):
        """Validate scenario name."""
        if not v or not v.strip():
            raise ValueError("scenario name cannot be empty)
        return v.strip()

    @field_validator("tags)
    def validate_tags(cls, v):
        """Validate tags format."""
        for tag in v:
            if not tag or not tag.strip():
                raise ValueError("Tags cannot be empty)
            if len(tag) > 100:
                raise ValueError("Individual tag cannot exceed 100 characters)
            if not all(c.isalnum() or c in "_-." for c in tag):
                raise ValueError("Tags can only contain alphanumeric characters, underscore, hyphen, and dot)
        return [tag.strip() for tag in v]

    @field_validator("assumptions)
    def validate_assumptions(cls, v):
        """Validate assumptions structure."""
        for assumption in v:
            if not isinstance(assumption, dict):
                raise ValueError("Each assumption must be a dictionary)
            if "type" not in assumption:
                raise ValueError("Each assumption must have a 'type' field)
            if not assumption["type"] or not assumption["type"].strip():
                raise ValueError("Assumption type cannot be empty)
        return v


class CurvePoint(AurumBaseModel):
    """Individual curve data point with comprehensive validation."""
    curve_key: str = Field(..., min_length=1, max_length=255)
    tenor_label: str = Field(..., min_length=1, max_length=100)
    tenor_type: Optional[str] = Field(None, max_length=50)
    contract_month: Optional[dt.date] = None
    asof_date: dt.date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = Field(None, max_length=50)

    @model_validator(mode="before)
    @classmethod
    def validate_prices(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure at least one price field is provided."""
        mid = values.get("mid)
        bid = values.get("bid)
        ask = values.get("ask)

        if mid is None and bid is None and ask is None:
            raise ValueError("At least one price field (mid, bid, or ask) must be provided)

        return values

    @field_validator("mid", "bid", "ask)
    def validate_price_range(cls, v):
        """Validate price values are reasonable."""
        if v is not None:
            if not (-1e9 <= v <= 1e9):  # Reasonable price range
                raise ValueError("Price must be between -1e9 and 1e9)
        return v

    @field_validator("curve_key)
    def validate_curve_key_format(cls, v):
        """Validate curve key format."""
        if not v or not v.strip():
            raise ValueError("Curve key cannot be empty)
        # Allow alphanumeric, underscore, hyphen, dot
        if not all(c.isalnum() or c in "_-." for c in v):
            raise ValueError("Curve key can only contain alphanumeric characters, underscore, hyphen, and dot)
        return v.strip()

    @field_validator("tenor_label)
    def validate_tenor_label_format(cls, v):
        """Validate tenor label format."""
        if not v or not v.strip():
            raise ValueError("Tenor label cannot be empty)
        return v.strip()


class CurveResponse(AurumBaseModel):
    """Response model for curve data."""
    meta: Meta
    data: List[CurvePoint]

    @field_validator("data)
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Data cannot be empty)
        return v


class CurveDiffPoint(AurumBaseModel):
    """Curve difference point comparing two dates with validation."""
    curve_key: str = Field(..., min_length=1, max_length=255)
    tenor_label: str = Field(..., min_length=1, max_length=100)
    tenor_type: Optional[str] = Field(None, max_length=50)
    contract_month: Optional[dt.date] = None
    asof_a: dt.date
    mid_a: Optional[float] = None
    asof_b: dt.date
    mid_b: Optional[float] = None
    diff_abs: Optional[float] = None
    diff_pct: Optional[float] = None

    @model_validator(mode="before)
    @classmethod
    def validate_dates(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure asof_a and asof_b are different dates."""
        asof_a = values.get("asof_a)
        asof_b = values.get("asof_b)

        if asof_a and asof_b and asof_a == asof_b:
            raise ValueError("asof_a and asof_b must be different dates)

        return values

    @model_validator(mode="before)
    @classmethod
    def validate_prices(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Ensure at least one price comparison is available."""
        mid_a = values.get("mid_a)
        mid_b = values.get("mid_b)

        if mid_a is None and mid_b is None:
            raise ValueError("At least one price comparison (mid_a or mid_b) must be provided)

        return values

    @field_validator("mid_a", "mid_b)
    def validate_price_range(cls, v):
        """Validate price values are reasonable."""
        if v is not None:
            if not (-1e9 <= v <= 1e9):
                raise ValueError("Price must be between -1e9 and 1e9)
        return v

    @field_validator("diff_abs)
    def validate_diff_abs(cls, v):
        """Validate absolute difference is reasonable."""
        if v is not None:
            if not (-1e10 <= v <= 1e10):
                raise ValueError("Absolute difference must be between -1e10 and 1e10)
        return v

    @field_validator("diff_pct)
    def validate_diff_pct(cls, v):
        """Validate percentage difference is reasonable."""
        if v is not None:
            if not (-1000 <= v <= 1000):  # Allow up to 1000% change
                raise ValueError("Percentage difference must be between -1000 and 1000)
        return v

    @field_validator("curve_key", "tenor_label)
    def validate_string_fields(cls, v):
        """Validate string fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty)
        return v.strip()


class CurveDiffResponse(AurumBaseModel):
    """Response model for curve difference data."""
    meta: Meta
    data: List[CurveDiffPoint]

    @field_validator("data)
    def validate_data_not_empty(cls, v):
        """Ensure data is not empty."""
        if not v:
            raise ValueError("Data cannot be empty)
        return v


class DimensionsData(AurumBaseModel):
    asset_class: list[str] | None = None
    iso: list[str] | None = None
    location: list[str] | None = None
    market: list[str] | None = None
    product: list[str] | None = None
    block: list[str] | None = None
    tenor_type: list[str] | None = None


class DimensionsResponse(AurumBaseModel):
    meta: Meta
    data: DimensionsData
    counts: Optional["DimensionsCountData"] = None


class DimensionCount(AurumBaseModel):
    value: str
    count: int = Field(ge=0)


class DimensionsCountData(AurumBaseModel):
    asset_class: list[DimensionCount] | None = None
    iso: list[DimensionCount] | None = None
    location: list[DimensionCount] | None = None
    market: list[DimensionCount] | None = None
    product: list[DimensionCount] | None = None
    block: list[DimensionCount] | None = None
    tenor_type: list[DimensionCount] | None = None


class IsoLocationOut(AurumBaseModel):
    iso: str
    location_id: str
    location_name: Optional[str] = None
    location_type: Optional[str] = None
    zone: Optional[str] = None
    hub: Optional[str] = None
    timezone: Optional[str] = None


class IsoLocationsResponse(AurumBaseModel):
    meta: Meta
    data: list[IsoLocationOut]


class IsoLocationResponse(AurumBaseModel):
    meta: Meta
    data: IsoLocationOut


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


# Series-Curve Mapping models
class SeriesCurveMappingCreate(AurumBaseModel):
    """Model for creating series-curve mappings."""
    external_provider: str = Field(..., )
    external_series_id: str = Field(..., )
    curve_key: str = Field(..., )
    mapping_confidence: float = Field(1.0, ge=0.0, le=1.0, )
    mapping_method: str = Field("manual", )
    mapping_notes: str | None = Field(None, )
    is_active: bool = Field(True, )


class SeriesCurveMappingUpdate(AurumBaseModel):
    """Model for updating series-curve mappings."""
    mapping_confidence: float | None = Field(None, ge=0.0, le=1.0, )
    mapping_method: str | None = Field(None, )
    mapping_notes: str | None = Field(None, )
    is_active: bool | None = Field(None, )


class SeriesCurveMappingOut(AurumBaseModel):
    """Model for series-curve mapping responses."""
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
    """Response model for listing series-curve mappings."""
    meta: Meta
    data: list[SeriesCurveMappingOut]


class SeriesCurveMappingSearchResponse(AurumBaseModel):
    """Response model for potential mapping suggestions."""
    meta: Meta
    data: list[dict]  # curve_key, similarity_score, matching_criteria


# ISO LMP models


class IsoLmpPoint(AurumBaseModel):
    iso_code: str
    market: str
    delivery_date: dt.date
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


class IsoLmpResponse(AurumBaseModel):
    meta: Meta
    data: list[IsoLmpPoint]


class IsoLmpAggregatePoint(AurumBaseModel):
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


class IsoLmpAggregateResponse(AurumBaseModel):
    meta: Meta
    data: list[IsoLmpAggregatePoint]


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


# EIA catalog models


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


class DroughtIndexPoint(AurumBaseModel):
    series_id: str
    dataset: str
    index: str
    timescale: str
    valid_date: dt.date
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
    valid_date: dt.date
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
    "SeriesCurveMappingCreate",
    "SeriesCurveMappingUpdate",
    "SeriesCurveMappingOut",
    "SeriesCurveMappingListResponse",
    "SeriesCurveMappingSearchResponse",
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

from . import scenario_models as _scenario_models

ScenarioData = _scenario_models.ScenarioData
ScenarioResponse = _scenario_models.ScenarioResponse
ScenarioListResponse = _scenario_models.ScenarioListResponse
ScenarioRunOptions = _scenario_models.ScenarioRunOptions
ScenarioRunData = _scenario_models.ScenarioRunData
ScenarioRunResponse = _scenario_models.ScenarioRunResponse
ScenarioRunListResponse = _scenario_models.ScenarioRunListResponse
ScenarioOutputPoint = _scenario_models.ScenarioOutputPoint
ScenarioOutputResponse = _scenario_models.ScenarioOutputResponse
ScenarioMetricLatest = _scenario_models.ScenarioMetricLatest
ScenarioMetricLatestResponse = _scenario_models.ScenarioMetricLatestResponse
ScenarioOutputListResponse = _scenario_models.ScenarioOutputListResponse
CreateScenarioRequest = _scenario_models.CreateScenarioRequest
ScenarioStatus = _scenario_models.ScenarioStatus
ScenarioRunStatus = _scenario_models.ScenarioRunStatus
ScenarioRunPriority = _scenario_models.ScenarioRunPriority
BulkScenarioRunRequest = _scenario_models.BulkScenarioRunRequest
BulkScenarioRunResponse = _scenario_models.BulkScenarioRunResponse
BulkScenarioRunResult = _scenario_models.BulkScenarioRunResult
BulkScenarioRunDuplicate = _scenario_models.BulkScenarioRunDuplicate
ScenarioRunBulkResponse = _scenario_models.ScenarioRunBulkResponse


# PPA valuation models


class PpaValuationRequest(AurumBaseModel):
    ppa_contract_id: str
    scenario_id: Optional[str] = None
    asof_date: Optional[dt.date] = None
    options: Optional[dict] = None


class PpaMetric(AurumBaseModel):
    period_start: dt.date
    period_end: dt.date
    metric: str
    value: float
    currency: Optional[str] = None
    unit: Optional[str] = None
    run_id: Optional[str] = None
    curve_key: Optional[str] = None
    tenor_type: Optional[str] = None


class PpaValuationResponse(AurumBaseModel):
    meta: Meta
    data: list[PpaMetric]


class PpaContractBase(AurumBaseModel):
    instrument_id: str | None = None
    terms: dict[str, Any] = Field(default_factory=dict)


class PpaContractCreate(PpaContractBase):
    pass


class PpaContractUpdate(AurumBaseModel):
    instrument_id: str | None = None
    terms: dict[str, Any] | None = None


class PpaContractOut(PpaContractBase):
    ppa_contract_id: str
    tenant_id: str
    created_at: datetime
    updated_at: datetime


class PpaContractResponse(AurumBaseModel):
    meta: Meta
    data: PpaContractOut


class PpaContractListResponse(AurumBaseModel):
    meta: Meta
    data: list[PpaContractOut]


class PpaValuationRecord(AurumBaseModel):
    asof_date: dt.date | None = None
    scenario_id: str | None = None
    period_start: dt.date | None = None
    period_end: dt.date | None = None
    metric: str | None = None
    value: float | None = None
    cashflow: float | None = None
    npv: float | None = None
    irr: float | None = None
    curve_key: str | None = None
    version_hash: str | None = None
    ingested_at: datetime | None = None


class PpaValuationListResponse(AurumBaseModel):
    meta: Meta
    data: list[PpaValuationRecord]


class CachePurgeDetail(AurumBaseModel):
    scope: str
    redis_keys_removed: int
    local_entries_removed: int


class CachePurgeResponse(AurumBaseModel):
    meta: Meta
    data: list[CachePurgeDetail]


# External data models

class ExternalProvider(AurumBaseModel):
    """External data provider."""
    id: str = Field(..., )
    name: str = Field(..., )
    description: str = Field(...)
    base_url: Optional[str] = Field(None)
    last_updated: Optional[datetime] = Field(None, )
    series_count: Optional[int] = Field(None, )


class ExternalSeries(AurumBaseModel):
    """External data series."""
    id: str = Field(..., )
    provider_id: str = Field(..., )
    name: str = Field(..., )
    frequency: str = Field(..., )
    description: str = Field(...)
    units: Optional[str] = Field(None, )
    last_updated: Optional[datetime] = Field(None, )
    observation_count: Optional[int] = Field(None, )


class ExternalObservation(AurumBaseModel):
    """External data observation."""
    series_id: str = Field(..., )
    observation_date: str = Field(..., )
    value: float = Field(..., )
    observation_metadata: Optional[Dict[str, Any]] = Field(None, )


class ExternalProvidersResponse(AurumBaseModel):
    """Response for external providers list."""
    data: List[ExternalProvider] = Field(..., )
    meta: Meta = Field(..., )


class ExternalSeriesResponse(AurumBaseModel):
    """Response for external series list."""
    data: List[ExternalSeries] = Field(..., )
    meta: Meta = Field(..., )


class ExternalObservationsResponse(AurumBaseModel):
    """Response for external series observations."""
    data: List[ExternalObservation] = Field(..., )
    meta: Meta = Field(..., )


class ExternalMetadataResponse(AurumBaseModel):
    """Response for external data metadata."""
    providers: List[ExternalProvider] = Field(..., )
    total_series: int = Field(..., )
    last_updated: Optional[datetime] = Field(None, )


# External data query parameters

class ExternalSeriesQueryParams(AurumBaseModel):
    """Query parameters for external series requests."""
    provider: Optional[str] = Field(None, )
    frequency: Optional[str] = Field(None, )
    asof: Optional[str] = Field(None, )
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, )
    offset: int = Field(0, ge=0, )
    cursor: Optional[str] = Field(None, )

    @field_validator("frequency)
    def validate_frequency(cls, v):
        """Validate frequency values."""
        if v is not None and v not in ["daily", "weekly", "monthly", "quarterly", "yearly"]:
            raise ValueError("Frequency must be one of: daily, weekly, monthly, quarterly, yearly)
        return v

    @field_validator("asof)
    def validate_asof_format(cls, v):
        """Validate asof date format."""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d)
            except ValueError:
                raise ValueError("asof must be in YYYY-MM-DD format)
        return v


class ExternalObservationsQueryParams(AurumBaseModel):
    """Query parameters for external observations requests."""
    series_id: str = Field(..., )
    start_date: Optional[str] = Field(None, )
    end_date: Optional[str] = Field(None, )
    frequency: Optional[str] = Field(None, )
    asof: Optional[str] = Field(None, )
    limit: int = Field(MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE, )
    offset: int = Field(0, ge=0, )
    cursor: Optional[str] = Field(None, )
    format: str = Field("json", )

    @field_validator("frequency)
    def validate_frequency(cls, v):
        """Validate frequency values."""
        if v is not None and v not in ["daily", "weekly", "monthly", "quarterly", "yearly"]:
            raise ValueError("Frequency must be one of: daily, weekly, monthly, quarterly, yearly)
        return v

    @field_validator("start_date", "end_date)
    def validate_date_format(cls, v):
        """Validate date format."""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d)
            except ValueError:
                raise ValueError("Date must be in YYYY-MM-DD format)
        return v

    @field_validator("asof)
    def validate_asof_format(cls, v):
        """Validate asof date format."""
        if v is not None:
            try:
                datetime.strptime(v, "%Y-%m-%d)
            except ValueError:
                raise ValueError("asof must be in YYYY-MM-DD format)
        return v


__all__ += [
    "CreateScenarioRequest",
    "ScenarioData",
    "ScenarioResponse",
    "ScenarioListResponse",
    "ScenarioRunOptions",
    "ScenarioRunData",
    "ScenarioRunResponse",
    "ScenarioRunListResponse",
    "ScenarioOutputPoint",
    "ScenarioOutputResponse",
    "ScenarioOutputListResponse",
    "ScenarioMetricLatest",
    "ScenarioMetricLatestResponse",
    "ScenarioStatus",
    "ScenarioRunStatus",
    "ScenarioRunPriority",
    "BulkScenarioRunRequest",
    "BulkScenarioRunResponse",
    "BulkScenarioRunResult",
    "BulkScenarioRunDuplicate",
    "ScenarioRunBulkResponse",
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
    # External models
    "ExternalProvider",
    "ExternalSeries",
    "ExternalObservation",
    "ExternalProvidersResponse",
    "ExternalSeriesResponse",
    "ExternalObservationsResponse",
    "ExternalMetadataResponse",
    "ExternalSeriesQueryParams",
    "ExternalObservationsQueryParams",
]
