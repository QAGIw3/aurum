"""Scenario assumption validation models."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator


class DriverType(str, Enum):
    POLICY = "policy"
    LOAD_GROWTH = "load_growth"
    FUEL_CURVE = "fuel_curve"
    FLEET_CHANGE = "fleet_change"
    # Phase 3: Advanced driver types
    COMPOSITE = "composite"
    STOCHASTIC = "stochastic"
    CONDITIONAL = "conditional"
    TIME_SERIES = "time_series"


class PolicyPayload(BaseModel):
    policy_name: str
    start_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    description: Optional[str] = None


class LoadGrowthPayload(BaseModel):
    region: Optional[str] = None
    annual_growth_pct: float = Field(description="Percentage growth (e.g., 2.5 for 2.5%)")
    start_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    end_year: Optional[int] = Field(default=None, ge=1900, le=2100)
    reference_curve_key: Optional[str] = None


class FuelCurvePayload(BaseModel):
    fuel: Literal["natural_gas", "coal", "co2", "oil"]
    reference_series: str
    basis_points_adjustment: float = 0.0


class FleetChangePayload(BaseModel):
    fleet_id: str
    change_type: Literal["retire", "add"]
    effective_date: str  # ISO date string
    capacity_mw: float = Field(gt=0)


# Phase 3: Advanced driver payload types
class CompositePayload(BaseModel):
    """Combines multiple drivers with weighted logic."""
    components: List[Dict[str, Any]] = Field(..., description="List of component drivers")
    composition_type: Literal["additive", "multiplicative", "conditional"] = Field(
        default="additive", description="How to combine components"
    )
    weights: Optional[Dict[str, float]] = Field(None, description="Component weights")
    validation_rules: Dict[str, Any] = Field(default_factory=dict)


class StochasticPayload(BaseModel):
    """Stochastic driver with probability distributions."""
    distribution_type: Literal["normal", "lognormal", "uniform", "triangular"] = Field(
        ..., description="Probability distribution type"
    )
    parameters: Dict[str, float] = Field(..., description="Distribution parameters")
    base_driver: Dict[str, Any] = Field(..., description="Base driver to apply stochastic variation to")
    correlation_matrix: Optional[Dict[str, Dict[str, float]]] = Field(None, description="Correlation with other drivers")


class ConditionalPayload(BaseModel):
    """Conditional driver that activates based on conditions."""
    conditions: List[Dict[str, Any]] = Field(..., description="Activation conditions")
    true_driver: Dict[str, Any] = Field(..., description="Driver when conditions are true")
    false_driver: Optional[Dict[str, Any]] = Field(None, description="Driver when conditions are false")
    evaluation_logic: Literal["all", "any", "custom"] = Field(default="all", description="How to evaluate conditions")


class TimeSeriesPayload(BaseModel):
    """Time series driver with advanced patterns."""
    series_type: Literal["trend", "seasonal", "cyclical", "irregular"] = Field(
        ..., description="Type of time series pattern"
    )
    base_values: Dict[str, float] = Field(..., description="Base time series values")
    trend_params: Optional[Dict[str, float]] = Field(None, description="Trend parameters")
    seasonal_params: Optional[Dict[str, Any]] = Field(None, description="Seasonal parameters")
    forecast_horizon: int = Field(default=24, ge=1, description="Forecast horizon in periods")


_PAYLOAD_MODELS = {
    DriverType.POLICY: PolicyPayload,
    DriverType.LOAD_GROWTH: LoadGrowthPayload,
    DriverType.FUEL_CURVE: FuelCurvePayload,
    DriverType.FLEET_CHANGE: FleetChangePayload,
    # Phase 3: Advanced driver payload mappings
    DriverType.COMPOSITE: CompositePayload,
    DriverType.STOCHASTIC: StochasticPayload,
    DriverType.CONDITIONAL: ConditionalPayload,
    DriverType.TIME_SERIES: TimeSeriesPayload,
}


class ScenarioAssumption(BaseModel):
    driver_type: DriverType
    payload: dict
    version: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def _validate_payload(cls, values: dict[str, Any]) -> dict[str, Any]:
        driver_type: DriverType = values.get("driver_type")
        payload_data: dict = values.get("payload", {})
        model = _PAYLOAD_MODELS.get(driver_type)
        if model is None:
            raise ValueError(f"Unsupported driver type: {driver_type}")
        model(**payload_data)  # raises if invalid
        return values
