from __future__ import annotations

"""External provider catalog models and query params."""

from datetime import datetime
from typing import Any, Optional

from pydantic import Field, field_validator

from aurum.api.http.pagination import DEFAULT_PAGE_SIZE, MAX_CURSOR_LENGTH, MAX_PAGE_SIZE

from .base import AurumBaseModel
from .common import Meta

_ALLOWED_FREQUENCIES = {"daily", "weekly", "monthly", "quarterly", "yearly"}


class ExternalProvider(AurumBaseModel):
    id: str
    name: str
    description: str
    base_url: str | None = None
    last_updated: datetime | None = None
    series_count: int | None = None


class ExternalSeries(AurumBaseModel):
    id: str
    provider_id: str
    name: str
    frequency: str
    description: str
    units: str | None = None
    last_updated: datetime | None = None
    observation_count: int | None = None


class ExternalObservation(AurumBaseModel):
    series_id: str
    observation_date: str
    value: float
    observation_metadata: dict[str, Any] | None = None


class ExternalProvidersResponse(AurumBaseModel):
    data: list[ExternalProvider]
    meta: Meta


class ExternalSeriesResponse(AurumBaseModel):
    data: list[ExternalSeries]
    meta: Meta


class ExternalObservationsResponse(AurumBaseModel):
    data: list[ExternalObservation]
    meta: Meta


class ExternalMetadataResponse(AurumBaseModel):
    providers: list[ExternalProvider]
    total_series: int
    last_updated: datetime | None = None


class ExternalSeriesQueryParams(AurumBaseModel):
    provider: str | None = None
    frequency: str | None = None
    asof: str | None = None
    limit: int = Field(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(0, ge=0)
    cursor: str | None = Field(None, max_length=MAX_CURSOR_LENGTH)

    @field_validator("frequency")
    @classmethod
    def _validate_frequency(cls, value: str | None) -> str | None:
        if value is not None and value not in _ALLOWED_FREQUENCIES:
            raise ValueError(
                "Frequency must be one of: daily, weekly, monthly, quarterly, yearly"
            )
        return value

    @field_validator("asof")
    @classmethod
    def _validate_asof(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("asof must be in YYYY-MM-DD format") from exc
        return value


class ExternalObservationsQueryParams(AurumBaseModel):
    series_id: str
    start_date: str | None = None
    end_date: str | None = None
    frequency: str | None = None
    asof: str | None = None
    limit: int = Field(MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    offset: int = Field(0, ge=0)
    cursor: str | None = Field(None, max_length=MAX_CURSOR_LENGTH)
    format: str = Field("json")

    @field_validator("frequency")
    @classmethod
    def _validate_frequency(cls, value: str | None) -> str | None:
        if value is not None and value not in _ALLOWED_FREQUENCIES:
            raise ValueError(
                "Frequency must be one of: daily, weekly, monthly, quarterly, yearly"
            )
        return value

    @field_validator("start_date", "end_date", "asof")
    @classmethod
    def _validate_dates(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("Date must be in YYYY-MM-DD format") from exc
        return value


__all__ = [
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
