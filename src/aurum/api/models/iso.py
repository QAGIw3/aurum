from __future__ import annotations

"""ISO location and LMP response models."""

from datetime import date, datetime
from typing import Optional

from pydantic import Field

from .base import AurumBaseModel
from .common import Meta


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


class IsoLmpPoint(AurumBaseModel):
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
    sample_count: int = Field(ge=0)


class IsoLmpAggregateResponse(AurumBaseModel):
    meta: Meta
    data: list[IsoLmpAggregatePoint]


__all__ = [
    "IsoLocationOut",
    "IsoLocationsResponse",
    "IsoLocationResponse",
    "IsoLmpPoint",
    "IsoLmpResponse",
    "IsoLmpAggregatePoint",
    "IsoLmpAggregateResponse",
]
