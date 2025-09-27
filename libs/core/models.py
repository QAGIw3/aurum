"""Shared domain models implemented with Pydantic v2."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from .enums import CurrencyCode, IsoCode, IsoMarket, PriceBlock, UnitOfMeasure


class AurumBaseModel(BaseModel):
    """Base model enforcing strict validation rules across the codebase."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True, validate_assignment=True)


class UnitNormalization(AurumBaseModel):
    """Canonical representation of a currency/unit mapping for price series."""

    currency: CurrencyCode = Field(default=CurrencyCode.USD)
    unit: UnitOfMeasure = Field(default=UnitOfMeasure.MWH, alias="per_unit")


class CurveKey(AurumBaseModel):
    """Identifier for a curve consisting of ISO/market/location/product metadata."""

    iso: IsoCode
    market: IsoMarket
    location: str = Field(min_length=1)
    product: Optional[str] = None
    block: Optional[PriceBlock] = None


class PriceObservation(AurumBaseModel):
    """Standardised price observation used across ingestion and API layers."""

    curve: CurveKey
    interval_start: datetime
    interval_end: Optional[datetime] = None
    delivery_date: date
    price: Optional[float] = Field(default=None, description="Primary price value")
    currency: CurrencyCode = CurrencyCode.USD
    unit: UnitOfMeasure = UnitOfMeasure.MWH


class PaginationMeta(AurumBaseModel):
    """Metadata block for paginated responses supporting offset and cursors."""

    request_id: str = Field(min_length=1)
    query_time_ms: int = Field(default=0, ge=0)
    count: Optional[int] = Field(default=None, ge=0)
    total: Optional[int] = Field(default=None, ge=0)
    offset: Optional[int] = Field(default=None, ge=0)
    limit: Optional[int] = Field(default=None, gt=0)
    next_cursor: Optional[str] = None
    prev_cursor: Optional[str] = None


class Watermark(AurumBaseModel):
    """Watermark model for tracking incremental data processing progress."""

    source: str = Field(min_length=1, description="Data source identifier")
    table: str = Field(min_length=1, description="Table/dataset identifier")
    last_value: Optional[str] = Field(default=None, description="Last processed value (string-based)")
    last_processed_at: Optional[datetime] = Field(default=None, description="Last processing timestamp")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Last update timestamp")


__all__ = [
    "AurumBaseModel",
    "UnitNormalization",
    "CurveKey",
    "PriceObservation",
    "PaginationMeta",
    "Watermark",
]
