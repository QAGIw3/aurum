from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel, Field
from aurum.scenarios.models import ScenarioAssumption


class Meta(BaseModel):
    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: str | None = None


class CurvePoint(BaseModel):
    curve_key: str
    tenor_label: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    asof_date: date
    mid: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    price_type: Optional[str] = None


class CurveResponse(BaseModel):
    meta: Meta
    data: List[CurvePoint]


class CurveDiffPoint(BaseModel):
    curve_key: str
    tenor_label: str
    tenor_type: Optional[str] = None
    contract_month: Optional[date] = None
    asof_a: date
    mid_a: Optional[float] = None
    asof_b: date
    mid_b: Optional[float] = None
    diff_abs: Optional[float] = None
    diff_pct: Optional[float] = None


class CurveDiffResponse(BaseModel):
    meta: Meta
    data: List[CurveDiffPoint]


class DimensionsData(BaseModel):
    asset_class: list[str] | None = None
    iso: list[str] | None = None
    location: list[str] | None = None
    market: list[str] | None = None
    product: list[str] | None = None
    block: list[str] | None = None
    tenor_type: list[str] | None = None


class DimensionsResponse(BaseModel):
    meta: Meta
    data: DimensionsData


__all__ = [
    "Meta",
    "CurvePoint",
    "CurveResponse",
    "CurveDiffPoint",
    "CurveDiffResponse",
    "DimensionsData",
    "DimensionsResponse",
]

# Scenario models


class CreateScenarioRequest(BaseModel):
    tenant_id: str
    name: str
    description: Optional[str] = None
    assumptions: list[ScenarioAssumption]


class ScenarioData(BaseModel):
    scenario_id: str
    tenant_id: str
    name: str
    description: Optional[str] = None
    status: str
    assumptions: list[ScenarioAssumption]
    created_at: Optional[str] = None


class ScenarioResponse(BaseModel):
    meta: Meta
    data: ScenarioData


class ScenarioRunOptions(BaseModel):
    code_version: Optional[str] = None
    seed: Optional[int] = None


class ScenarioRunData(BaseModel):
    run_id: str
    scenario_id: str
    state: str
    code_version: Optional[str] = None
    seed: Optional[int] = None
    created_at: Optional[str] = None


class ScenarioRunResponse(BaseModel):
    meta: Meta
    data: ScenarioRunData


# PPA valuation models


class PpaValuationRequest(BaseModel):
    ppa_contract_id: str
    scenario_id: Optional[str] = None
    asof_date: Optional[date] = None
    options: Optional[dict] = None


class PpaMetric(BaseModel):
    period_start: date
    period_end: date
    metric: str
    value: float
    currency: Optional[str] = None


class PpaValuationResponse(BaseModel):
    meta: Meta
    data: list[PpaMetric]


__all__ += [
    "CreateScenarioRequest",
    "ScenarioData",
    "ScenarioResponse",
    "ScenarioRunOptions",
    "ScenarioRunData",
    "ScenarioRunResponse",
    "PpaValuationRequest",
    "PpaMetric",
    "PpaValuationResponse",
]
