from __future__ import annotations

"""Dimension listing schemas used by curves metadata endpoints."""

from pydantic import Field

from .base import AurumBaseModel
from .common import Meta


class DimensionCount(AurumBaseModel):
    value: str
    count: int = Field(ge=0)


class DimensionsData(AurumBaseModel):
    asset_class: list[str] | None = None
    iso: list[str] | None = None
    location: list[str] | None = None
    market: list[str] | None = None
    product: list[str] | None = None
    block: list[str] | None = None
    tenor_type: list[str] | None = None


class DimensionsCountData(AurumBaseModel):
    asset_class: list[DimensionCount] | None = None
    iso: list[DimensionCount] | None = None
    location: list[DimensionCount] | None = None
    market: list[DimensionCount] | None = None
    product: list[DimensionCount] | None = None
    block: list[DimensionCount] | None = None
    tenor_type: list[DimensionCount] | None = None


class DimensionsResponse(AurumBaseModel):
    meta: Meta
    data: DimensionsData
    counts: DimensionsCountData | None = None


__all__ = [
    "DimensionCount",
    "DimensionsData",
    "DimensionsCountData",
    "DimensionsResponse",
]
