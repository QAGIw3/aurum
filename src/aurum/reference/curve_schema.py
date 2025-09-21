"""Canonical curve schema definitions and enumerations."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Tuple


@dataclass(frozen=True)
class CurveField:
    """Single canonical curve column definition."""

    name: str
    dtype: str
    nullable: bool = True
    enum: type[Enum] | None = None
    description: str | None = None


class CurveAssetClass(str, Enum):
    POWER = "power"
    RENEWABLE = "renewable"
    GAS = "gas"
    CARBON = "carbon"
    STORAGE = "storage"
    OTHER = "other"


class CurvePriceType(str, Enum):
    MID = "MID"
    BID = "BID"
    ASK = "ASK"
    LAST = "LAST"
    SETTLE = "SETTLE"
    INDEX = "INDEX"
    STRIP = "STRIP"
    CUSTOM = "CUSTOM"


class CurveTenorType(str, Enum):
    MONTHLY = "MONTHLY"
    QUARTER = "QUARTER"
    SEASON = "SEASON"
    CALENDAR = "CALENDAR"
    STRIP = "STRIP"
    CUSTOM = "CUSTOM"


CANONICAL_CURVE_FIELDS: Tuple[CurveField, ...] = (
    CurveField("asof_date", "date", nullable=False, description="Pricing as-of date"),
    CurveField("source_file", "string", nullable=False, description="Original workbook filename"),
    CurveField("sheet_name", "string", nullable=False, description="Workbook sheet name"),
    CurveField("asset_class", "string", nullable=False, enum=CurveAssetClass),
    CurveField("region", "string"),
    CurveField("iso", "string"),
    CurveField("location", "string"),
    CurveField("market", "string"),
    CurveField("product", "string"),
    CurveField("block", "string"),
    CurveField("spark_location", "string"),
    CurveField("price_type", "string", nullable=False, enum=CurvePriceType),
    CurveField("units_raw", "string", nullable=False),
    CurveField("currency", "string"),
    CurveField("per_unit", "string"),
    CurveField("tenor_type", "string", nullable=False, enum=CurveTenorType),
    CurveField("contract_month", "date"),
    CurveField("tenor_label", "string", nullable=False),
    CurveField("value", "double"),
    CurveField("bid", "double"),
    CurveField("ask", "double"),
    CurveField("mid", "double"),
    CurveField("curve_key", "string", nullable=False),
    CurveField("version_hash", "string", nullable=False),
    CurveField("_ingest_ts", "timestamp", nullable=False),
)


CANONICAL_CURVE_COLUMNS: Tuple[str, ...] = tuple(field.name for field in CANONICAL_CURVE_FIELDS)
CURVE_ENUMS: dict[str, type[Enum]] = {
    field.name: field.enum for field in CANONICAL_CURVE_FIELDS if field.enum is not None
}
CURVE_IDENTITY_FIELDS: Tuple[str, ...] = (
    "asset_class",
    "iso",
    "region",
    "location",
    "market",
    "product",
    "block",
    "spark_location",
)


__all__ = [
    "CANONICAL_CURVE_COLUMNS",
    "CANONICAL_CURVE_FIELDS",
    "CURVE_ENUMS",
    "CURVE_IDENTITY_FIELDS",
    "CurveAssetClass",
    "CurveField",
    "CurvePriceType",
    "CurveTenorType",
]
