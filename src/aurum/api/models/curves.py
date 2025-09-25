from __future__ import annotations

"""Curve-related API schemas and validators."""

from datetime import date, datetime
from typing import Any, Optional

from pydantic import Field, field_validator, model_validator

from aurum.api.http.pagination import DEFAULT_PAGE_SIZE, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE

from .base import AurumBaseModel
from .common import Meta

_CURSOR_FIELDS = ("cursor", "since_cursor", "prev_cursor")


class CurveQueryParams(AurumBaseModel):
    """Query parameters for curve listings with cursor/offset safety."""

    asof: Optional[str] = None
    iso: Optional[str] = Field(None, max_length=10)
    market: Optional[str] = Field(None, max_length=50)
    location: Optional[str] = Field(None, max_length=100)
    product: Optional[str] = Field(None, max_length=50)
    block: Optional[str] = Field(None, max_length=50)
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(0, ge=0)
    cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH)
    since_cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH)
    prev_cursor: Optional[str] = Field(None, max_length=MAX_CURSOR_LENGTH)

    @model_validator(mode="before")
    @classmethod
    def _validate_cursor_usage(cls, values: dict[str, Any]) -> dict[str, Any]:
        cursor = values.get("cursor")
        offset = int(values.get("offset", 0) or 0)
        if cursor and offset > 0:
            raise ValueError("Cannot use cursor and offset together")

        cursor_count = sum(1 for key in _CURSOR_FIELDS if values.get(key) is not None)
        if cursor_count > 1:
            raise ValueError("Cannot use multiple cursor parameters together")
        return values

    @field_validator("asof")
    @classmethod
    def _validate_asof(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:  # pragma: no cover - validation only
            raise ValueError("asof must be in YYYY-MM-DD format") from exc
        return value

    @field_validator("iso")
    @classmethod
    def _normalize_iso(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.isalpha():
            raise ValueError("iso must contain only letters")
        return value.upper()

    @field_validator(*_CURSOR_FIELDS)
    @classmethod
    def _ensure_cursor_length(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if len(value) > MAX_CURSOR_LENGTH:
            raise ValueError("Cursor too long (max 1000 characters)")
        return value


class CurveDiffQueryParams(AurumBaseModel):
    """Query parameters for curve diff endpoints."""

    asof_a: str
    asof_b: str
    iso: Optional[str] = Field(None, max_length=10)
    market: Optional[str] = Field(None, max_length=50)
    location: Optional[str] = Field(None, max_length=100)
    product: Optional[str] = Field(None, max_length=50)
    block: Optional[str] = Field(None, max_length=50)
    tenor_type: Optional[str] = Field(None, max_length=50)
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(0, ge=0)

    @field_validator("asof_a", "asof_b")
    @classmethod
    def _validate_dates(cls, value: str) -> str:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Date must be in YYYY-MM-DD format") from exc
        return value

    @model_validator(mode="before")
    @classmethod
    def _validate_comparison(cls, values: dict[str, Any]) -> dict[str, Any]:
        asof_a = values.get("asof_a")
        asof_b = values.get("asof_b")
        if asof_a and asof_b:
            date_a = datetime.strptime(asof_a, "%Y-%m-%d")
            date_b = datetime.strptime(asof_b, "%Y-%m-%d")
            if date_a == date_b:
                raise ValueError("asof_a and asof_b must be different dates")
            if abs((date_b - date_a).days) > 365:
                raise ValueError("Date range too large: maximum 365 days")
        return values

    @model_validator(mode="before")
    @classmethod
    def _ensure_dimension_filters(cls, values: dict[str, Any]) -> dict[str, Any]:
        dimension_fields = ("iso", "market", "location", "product", "block", "tenor_type")
        if not any(values.get(field) for field in dimension_fields):
            raise ValueError("At least one dimension filter must be specified")
        return values


class CurvePoint(AurumBaseModel):
    """Individual curve datapoint."""

    curve_key: str = Field(..., min_length=1, max_length=255)
    tenor_label: str = Field(..., min_length=1, max_length=100)
    tenor_type: Optional[str] = Field(None, max_length=50)
    contract_month: Optional[date] = None
    asof_date: date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = Field(None, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def _ensure_price_present(cls, values: dict[str, Any]) -> dict[str, Any]:
        if all(values.get(field) is None for field in ("mid", "bid", "ask")):
            raise ValueError("At least one price field (mid, bid, ask) must be provided")
        return values

    @field_validator("mid", "bid", "ask")
    @classmethod
    def _validate_price(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if not -1e9 <= value <= 1e9:
            raise ValueError("Price must be between -1e9 and 1e9")
        return value

    @field_validator("curve_key")
    @classmethod
    def _validate_curve_key(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Curve key cannot be empty")
        value = value.strip()
        if not all(char.isalnum() or char in "_-." for char in value):
            raise ValueError("Curve key can only contain alphanumeric characters, underscore, hyphen, and dot")
        return value

    @field_validator("tenor_label")
    @classmethod
    def _validate_tenor_label(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Tenor label cannot be empty")
        return value.strip()


class CurveResponse(AurumBaseModel):
    """Response envelope for curve listings."""

    meta: Meta
    data: list[CurvePoint]

    @field_validator("data")
    @classmethod
    def _ensure_payload(cls, value: list[CurvePoint]) -> list[CurvePoint]:
        if not value:
            raise ValueError("Data cannot be empty")
        return value


class CurveDiffPoint(AurumBaseModel):
    """Datapoint capturing differences between two as-of dates."""

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

    @model_validator(mode="before")
    @classmethod
    def _ensure_distinct_dates(cls, values: dict[str, Any]) -> dict[str, Any]:
        asof_a = values.get("asof_a")
        asof_b = values.get("asof_b")
        if asof_a and asof_b and asof_a == asof_b:
            raise ValueError("asof_a and asof_b must be different dates")
        return values

    @model_validator(mode="before")
    @classmethod
    def _ensure_price_available(cls, values: dict[str, Any]) -> dict[str, Any]:
        if values.get("mid_a") is None and values.get("mid_b") is None:
            raise ValueError("At least one price comparison (mid_a or mid_b) must be provided")
        return values

    @field_validator("mid_a", "mid_b")
    @classmethod
    def _validate_mid(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if not -1e9 <= value <= 1e9:
            raise ValueError("Price must be between -1e9 and 1e9")
        return value

    @field_validator("diff_abs")
    @classmethod
    def _validate_abs(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if not -1e10 <= value <= 1e10:
            raise ValueError("Absolute difference must be between -1e10 and 1e10")
        return value

    @field_validator("diff_pct")
    @classmethod
    def _validate_pct(cls, value: float | None) -> float | None:
        if value is None:
            return None
        if not -1000 <= value <= 1000:
            raise ValueError("Percentage difference must be between -1000 and 1000")
        return value

    @field_validator("curve_key", "tenor_label")
    @classmethod
    def _strip_text(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("Field cannot be empty")
        return value.strip()


class CurveDiffResponse(AurumBaseModel):
    """Response envelope for curve diff endpoints."""

    meta: Meta
    data: list[CurveDiffPoint]

    @field_validator("data")
    @classmethod
    def _ensure_payload(cls, value: list[CurveDiffPoint]) -> list[CurveDiffPoint]:
        if not value:
            raise ValueError("Data cannot be empty")
        return value


__all__ = [
    "CurveQueryParams",
    "CurveDiffQueryParams",
    "CurvePoint",
    "CurveResponse",
    "CurveDiffPoint",
    "CurveDiffResponse",
]
