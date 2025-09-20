"""Scenario assumption validation models."""

from __future__ import annotations

from enum import Enum
from typing import Literal, Optional

from pydantic import BaseModel, Field, model_validator


class DriverType(str, Enum):
    POLICY = "policy"
    LOAD_GROWTH = "load_growth"
    FUEL_CURVE = "fuel_curve"
    FLEET_CHANGE = "fleet_change"


class PolicyPayload(BaseModel):
    policy_name: str
    start_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    description: Optional[str] = None


class LoadGrowthPayload(BaseModel):
    region: Optional[str] = None
    annual_growth_pct: float = Field(description="Percentage growth (e.g., 2.5 for 2.5%)")
    start_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    end_year: Optional[int] = Field(default=None, ge=1900, le=2100)


class FuelCurvePayload(BaseModel):
    fuel: Literal["natural_gas", "coal", "co2", "oil"]
    reference_series: str
    basis_points_adjustment: float = 0.0


class FleetChangePayload(BaseModel):
    fleet_id: str
    change_type: Literal["retire", "add"]
    effective_date: str  # ISO date string
    capacity_mw: float = Field(gt=0)


_PAYLOAD_MODELS = {
    DriverType.POLICY: PolicyPayload,
    DriverType.LOAD_GROWTH: LoadGrowthPayload,
    DriverType.FUEL_CURVE: FuelCurvePayload,
    DriverType.FLEET_CHANGE: FleetChangePayload,
}


class ScenarioAssumption(BaseModel):
    driver_type: DriverType
    payload: dict
    version: Optional[str] = None

    @model_validator(mode="after")
    def _validate_payload(self) -> "ScenarioAssumption":
        driver_type: DriverType = self.driver_type
        payload_data: dict = self.payload or {}
        model = _PAYLOAD_MODELS.get(driver_type)
        if model is None:
            raise ValueError(f"Unsupported driver type: {driver_type}")
        model(**payload_data)  # raises if invalid
        return self
