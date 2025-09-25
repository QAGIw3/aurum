from __future__ import annotations

"""Pydantic models exposed by the Aurum API.

The module re-exports domain-specific schemas from the submodules in this
package so existing imports like ``from aurum.api.models import Meta`` continue
working after the refactor.
"""

from importlib import util as _importlib_util
from pathlib import Path as _Path

_SCENARIO_MODELS_PATH = _Path(__file__).resolve().parent.parent / 'scenarios' / 'scenario_models.py'
_spec = _importlib_util.spec_from_file_location('aurum.api.models._scenario_models', _SCENARIO_MODELS_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - defensive
    raise ImportError('Failed to load scenario models module')
_scenario_models = _importlib_util.module_from_spec(_spec)
_spec.loader.exec_module(_scenario_models)

(
    BulkScenarioRunDuplicate,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    CreateScenarioRequest,
    ScenarioData,
    ScenarioListResponse,
    ScenarioMetricLatest,
    ScenarioMetricLatestResponse,
    ScenarioOutputListResponse,
    ScenarioOutputPoint,
    ScenarioOutputResponse,
    ScenarioResponse,
    ScenarioRunBulkResponse,
    ScenarioRunData,
    ScenarioRunListResponse,
    ScenarioRunOptions,
    ScenarioRunPriority,
    ScenarioRunResponse,
    ScenarioRunStatus,
    ScenarioStatus,
) = (
    _scenario_models.BulkScenarioRunDuplicate,
    _scenario_models.BulkScenarioRunRequest,
    _scenario_models.BulkScenarioRunResponse,
    _scenario_models.BulkScenarioRunResult,
    _scenario_models.CreateScenarioRequest,
    _scenario_models.ScenarioData,
    _scenario_models.ScenarioListResponse,
    _scenario_models.ScenarioMetricLatest,
    _scenario_models.ScenarioMetricLatestResponse,
    _scenario_models.ScenarioOutputListResponse,
    _scenario_models.ScenarioOutputPoint,
    _scenario_models.ScenarioOutputResponse,
    _scenario_models.ScenarioResponse,
    _scenario_models.ScenarioRunBulkResponse,
    _scenario_models.ScenarioRunData,
    _scenario_models.ScenarioRunListResponse,
    _scenario_models.ScenarioRunOptions,
    _scenario_models.ScenarioRunPriority,
    _scenario_models.ScenarioRunResponse,
    _scenario_models.ScenarioRunStatus,
    _scenario_models.ScenarioStatus,
)

from .admin import (
    CalendarBlocksResponse,
    CalendarHoursResponse,
    CalendarOut,
    CalendarsResponse,
    SeriesCurveMappingCreate,
    SeriesCurveMappingListResponse,
    SeriesCurveMappingOut,
    SeriesCurveMappingSearchResponse,
    SeriesCurveMappingUpdate,
    UnitMappingOut,
    UnitsCanonical,
    UnitsCanonicalResponse,
    UnitsMappingResponse,
)
from .base import AurumBaseModel
from .cache import CachePurgeDetail, CachePurgeResponse
from .common import Meta, ErrorEnvelope, ValidationErrorDetail, ValidationErrorResponse
from .curves import (
    CurveDiffPoint,
    CurveDiffQueryParams,
    CurveDiffResponse,
    CurvePoint,
    CurveQueryParams,
    CurveResponse,
)
from .dimensions import DimensionCount, DimensionsCountData, DimensionsData, DimensionsResponse
from .drought import (
    DroughtDimensions,
    DroughtDimensionsResponse,
    DroughtIndexPoint,
    DroughtIndexResponse,
    DroughtInfoResponse,
    DroughtUsdmPoint,
    DroughtUsdmResponse,
    DroughtVectorEventPoint,
    DroughtVectorResponse,
)
from .eia import (
    EiaDatasetBriefOut,
    EiaDatasetDetailOut,
    EiaDatasetResponse,
    EiaDatasetsResponse,
    EiaSeriesDimensionsData,
    EiaSeriesDimensionsResponse,
    EiaSeriesPoint,
    EiaSeriesResponse,
)
from .external import (
    ExternalMetadataResponse,
    ExternalObservation,
    ExternalObservationsQueryParams,
    ExternalObservationsResponse,
    ExternalProvider,
    ExternalProvidersResponse,
    ExternalSeries,
    ExternalSeriesQueryParams,
    ExternalSeriesResponse,
)
from .iso import (
    IsoLocationOut,
    IsoLocationResponse,
    IsoLocationsResponse,
    IsoLmpAggregatePoint,
    IsoLmpAggregateResponse,
    IsoLmpPoint,
    IsoLmpResponse,
)
from .ppa import (
    PpaContractCreate,
    PpaContractListResponse,
    PpaContractOut,
    PpaContractResponse,
    PpaContractUpdate,
    PpaMetric,
    PpaValuationListResponse,
    PpaValuationRecord,
    PpaValuationRequest,
    PpaValuationResponse,
)

__all__ = [
    # Base / common
    "AurumBaseModel",
    "Meta",
    "ErrorEnvelope",
    "ValidationErrorDetail",
    "ValidationErrorResponse",
    # Curves
    "CurveQueryParams",
    "CurveDiffQueryParams",
    "CurvePoint",
    "CurveResponse",
    "CurveDiffPoint",
    "CurveDiffResponse",
    # Dimensions
    "DimensionCount",
    "DimensionsData",
    "DimensionsCountData",
    "DimensionsResponse",
    # ISO / LMP
    "IsoLocationOut",
    "IsoLocationsResponse",
    "IsoLocationResponse",
    "IsoLmpPoint",
    "IsoLmpResponse",
    "IsoLmpAggregatePoint",
    "IsoLmpAggregateResponse",
    # Admin metadata
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
    # EIA
    "EiaDatasetBriefOut",
    "EiaDatasetsResponse",
    "EiaDatasetDetailOut",
    "EiaDatasetResponse",
    "EiaSeriesPoint",
    "EiaSeriesResponse",
    "EiaSeriesDimensionsData",
    "EiaSeriesDimensionsResponse",
    # Drought
    "DroughtIndexPoint",
    "DroughtIndexResponse",
    "DroughtUsdmPoint",
    "DroughtUsdmResponse",
    "DroughtVectorEventPoint",
    "DroughtVectorResponse",
    "DroughtDimensions",
    "DroughtDimensionsResponse",
    "DroughtInfoResponse",
    # Scenarios (re-exported from scenario_models)
    "CreateScenarioRequest",
    "ScenarioResponse",
    "ScenarioData",
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
    # PPA
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
    # Cache
    "CachePurgeDetail",
    "CachePurgeResponse",
    # External data
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
