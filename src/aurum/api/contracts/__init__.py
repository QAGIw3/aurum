"""Canonical service and DAO contract dataclasses.

These light-weight types act as the shared vocabulary between the service
layer, DAOs, and routers.  They replace the ad-hoc dictionaries that the
legacy `service.py` module relied on and make cache versioning, tracing, and
retry policies explicit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Generic, Mapping, Optional, Sequence, Tuple, TypeVar
from datetime import date


class CacheStatus(str, Enum):
    """Represents the cache outcome for a service or DAO call."""

    HIT = "hit"
    MISS = "miss"
    BYPASS = "bypass"
    STALE = "stale"


@dataclass(frozen=True)
class CacheDirective:
    """Hints describing how a service should interact with its cache."""

    namespace: str
    ttl_seconds: int
    version: Optional[str] = None
    tags: Sequence[str] = field(default_factory=tuple)
    allow_bypass: bool = False


@dataclass(frozen=True)
class ServiceCallContext:
    """Context propagated from routers into services and DAOs."""

    tenant_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_name: Optional[str] = None
    cache_directive: Optional[CacheDirective] = None
    request_headers: Mapping[str, str] | None = None
    extra: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class KeysetCursor:
    """Represents a keyset pagination cursor."""

    values: Tuple[Any, ...]
    column_order: Tuple[str, ...]

    def as_params(self) -> Dict[str, Any]:
        """Return a mapping suitable for DAO keyset helpers."""

        return {column: value for column, value in zip(self.column_order, self.values)}

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> "KeysetCursor":
        """Create a cursor from a mapping preserving column order."""

        items = list(payload.items())
        return cls(values=tuple(value for _, value in items), column_order=tuple(key for key, _ in items))


@dataclass(frozen=True)
class Pagination:
    """Pagination envelope shared between services and DAOs."""

    limit: int
    offset: int = 0
    cursor_after: KeysetCursor | None = None
    cursor_before: KeysetCursor | None = None
    descending: bool = False
    overfetch: bool = False


@dataclass(frozen=True)
class RetryDirective:
    """Simple retry policy descriptor."""

    max_attempts: int = 1
    backoff_seconds: float = 0.0
    jitter_seconds: float = 0.0


@dataclass(frozen=True)
class QueryContext:
    """Context forwarded to DAOs so they can apply instrumentation policies."""

    trace_id: Optional[str] = None
    span_name: Optional[str] = None
    tenant_id: Optional[str] = None
    timeout_seconds: Optional[float] = None
    retry: RetryDirective = field(default_factory=RetryDirective)
    extra: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class ServiceExecutionMetadata:
    """Execution metadata returned alongside service data."""

    elapsed_ms: float
    cache_status: CacheStatus = CacheStatus.BYPASS
    cache_key: Optional[str] = None
    cache_version: Optional[str] = None
    backend: Optional[str] = None
    row_count: Optional[int] = None


T = TypeVar("T")


@dataclass(frozen=True)
class ServiceExecutionResult(Generic[T]):
    """Generic wrapper for returning data plus metadata."""

    data: T
    metadata: ServiceExecutionMetadata


@dataclass(frozen=True)
class QueryResult(Generic[T]):
    """DAO-level query response envelope."""

    data: T
    elapsed_ms: float
    cache_status: CacheStatus = CacheStatus.BYPASS
    raw_query: Optional[str] = None


# --- Domain specific filters -------------------------------------------------


@dataclass(frozen=True)
class CurvesQuery:
    asof: Optional[str] = None
    curve_key: Optional[str] = None
    asset_class: Optional[str] = None
    iso: Optional[str] = None
    location: Optional[str] = None
    market: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None
    tenor_type: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        return {
            "asof": self.asof,
            "curve_key": self.curve_key,
            "asset_class": self.asset_class,
            "iso": self.iso,
            "location": self.location,
            "market": self.market,
            "product": self.product,
            "block": self.block,
            "tenor_type": self.tenor_type,
        }


@dataclass(frozen=True)
class CurvesDiffQuery:
    asof_a: date
    asof_b: date
    curve_key: Optional[str] = None
    asset_class: Optional[str] = None
    iso: Optional[str] = None
    location: Optional[str] = None
    market: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None
    tenor_type: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        params = CurvesQuery(
            curve_key=self.curve_key,
            asset_class=self.asset_class,
            iso=self.iso,
            location=self.location,
            market=self.market,
            product=self.product,
            block=self.block,
            tenor_type=self.tenor_type,
        ).as_params()
        params.update({"asof_a": self.asof_a.isoformat(), "asof_b": self.asof_b.isoformat()})
        return params


@dataclass(frozen=True)
class DimensionsQuery:
    asof: Optional[str] = None
    asset_class: Optional[str] = None
    iso: Optional[str] = None
    location: Optional[str] = None
    market: Optional[str] = None
    product: Optional[str] = None
    block: Optional[str] = None
    tenor_type: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        return {
            "asof": self.asof,
            "asset_class": self.asset_class,
            "iso": self.iso,
            "location": self.location,
            "market": self.market,
            "product": self.product,
            "block": self.block,
            "tenor_type": self.tenor_type,
        }


@dataclass(frozen=True)
class ScenarioOutputsQuery:
    tenant_id: str
    scenario_id: str
    curve_key: Optional[str] = None
    tenor_type: Optional[str] = None
    metric: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "scenario_id": self.scenario_id,
            "curve_key": self.curve_key,
            "tenor_type": self.tenor_type,
            "metric": self.metric,
        }


@dataclass(frozen=True)
class ScenarioMetricsQuery:
    scenario_id: str
    tenant_id: Optional[str] = None
    metric: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        return {
            "scenario_id": self.scenario_id,
            "tenant_id": self.tenant_id,
            "metric": self.metric,
        }


@dataclass(frozen=True)
class EiaSeriesQuery:
    series_id: Optional[str] = None
    frequency: Optional[str] = None
    area: Optional[str] = None
    sector: Optional[str] = None
    dataset: Optional[str] = None
    unit: Optional[str] = None
    canonical_unit: Optional[str] = None
    canonical_currency: Optional[str] = None
    source: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None

    def as_params(self) -> Dict[str, Any]:
        return {
            "series_id": self.series_id,
            "frequency": self.frequency,
            "area": self.area,
            "sector": self.sector,
            "dataset": self.dataset,
            "unit": self.unit,
            "canonical_unit": self.canonical_unit,
            "canonical_currency": self.canonical_currency,
            "source": self.source,
            "start": self.start,
            "end": self.end,
        }


@dataclass(frozen=True)
class IsoLmpQuery:
    iso_code: Optional[str] = None
    market: Optional[str] = None
    location_id: Optional[str] = None
    start: Optional[str] = None
    end: Optional[str] = None
    granularity: str = "hourly"

    def as_params(self) -> Dict[str, Any]:
        return {
            "iso_code": self.iso_code,
            "market": self.market,
            "location_id": self.location_id,
            "start": self.start,
            "end": self.end,
            "granularity": self.granularity,
        }


__all__ = [
    "CacheDirective",
    "CacheStatus",
    "CurvesDiffQuery",
    "CurvesQuery",
    "DimensionsQuery",
    "EiaSeriesQuery",
    "IsoLmpQuery",
    "KeysetCursor",
    "Pagination",
    "QueryContext",
    "QueryResult",
    "RetryDirective",
    "ScenarioMetricsQuery",
    "ScenarioOutputsQuery",
    "ServiceCallContext",
    "ServiceExecutionMetadata",
    "ServiceExecutionResult",
]

