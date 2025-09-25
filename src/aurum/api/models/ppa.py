from __future__ import annotations

"""Power purchase agreement (PPA) valuation schemas."""

from datetime import date, datetime
from typing import Any, Optional

from pydantic import Field

from .base import AurumBaseModel
from .common import Meta


class PpaValuationRequest(AurumBaseModel):
    ppa_contract_id: str
    scenario_id: Optional[str] = None
    asof_date: Optional[date] = None
    options: Optional[dict] = None


class PpaMetric(AurumBaseModel):
    period_start: date
    period_end: date
    metric: str
    value: float
    currency: str | None = None
    unit: str | None = None
    run_id: str | None = None
    curve_key: str | None = None
    tenor_type: str | None = None


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


class PpaValuationListResponse(AurumBaseModel):
    meta: Meta
    data: list[PpaValuationRecord]


__all__ = [
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
]
