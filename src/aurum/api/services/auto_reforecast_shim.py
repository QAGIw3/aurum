"""Compatibility shim for auto-reforecast service.

Provides backward compatibility for code using the old auto_reforecast_service
while redirecting to the new service architecture.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

from aurum.services.ml.auto_reforecast import (
    AutoReforecastService,
    TriggerCondition as NewTriggerCondition,
    DebounceConfig as NewDebounceConfig,
    BackpressureConfig as NewBackpressureConfig
)

# Re-export the new dataclasses that match the old interface
TriggerCondition = NewTriggerCondition
DebounceConfig = NewDebounceConfig
BackpressureConfig = NewBackpressureConfig


# Mock ForecastRequest for compatibility
class ForecastRequest(BaseModel):
    """Mock forecast request for backward compatibility."""
    forecast_type: str
    target_variable: str
    geography: str
    start_date: datetime
    end_date: datetime
    quantiles: List[str] = Field(default_factory=list)
    interval: str = "hourly"


class ForecastTrigger(BaseModel):
    """Individual forecast trigger configuration."""
    trigger_id: str
    name: str
    description: str
    conditions: List[TriggerCondition]
    forecast_config: ForecastRequest
    enabled: bool = True
    priority: float = 1.0  # Higher values = higher priority
    cooldown_minutes: int = 30  # Minimum time between triggers
    last_triggered: Optional[datetime] = None
    trigger_count: int = 0


class TriggerEvent(BaseModel):
    """Event that triggers forecast re-run."""
    event_id: str
    trigger_id: str
    data_source: str
    geography: str
    timestamp: datetime
    data_changes: Dict[str, float]  # Field name -> change magnitude
    priority_score: float = 1.0
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ReforcastJob(BaseModel):
    """Job for re-running forecasts."""
    job_id: str
    trigger_event: TriggerEvent
    forecast_request: ForecastRequest
    priority: float
    created_at: datetime
    scheduled_for: datetime
    status: str = "pending"  # pending, processing, completed, failed
    attempts: int = 0
    max_attempts: int = 3
    error_message: Optional[str] = None


# Singleton instance
_service_instance = None


def get_auto_reforecast_service(
    debounce_config: Optional[DebounceConfig] = None,
    backpressure_config: Optional[BackpressureConfig] = None
) -> AutoReforecastService:
    """Get singleton auto-reforecast service instance."""
    global _service_instance
    if _service_instance is None:
        _service_instance = AutoReforecastService()
        # Store configs if provided
        if debounce_config:
            _service_instance._debounce_config = debounce_config
        if backpressure_config:
            _service_instance._backpressure_config = backpressure_config
    return _service_instance


async def trigger_forecast_rerun(
    forecast_type: str,
    geography: str,
    reason: str,
    priority: float = 0.5
) -> Dict[str, Any]:
    """Trigger a forecast re-run - convenience function."""
    service = get_auto_reforecast_service()
    result = await service.trigger_forecast_rerun(
        forecast_type=forecast_type,
        geography=geography,
        reason=reason,
        priority=priority
    )
    return result.data if result.success else {}


async def create_weather_trigger(
    trigger_name: str,
    geography: str,
    temperature_threshold: float = 5.0,
    humidity_threshold: float = 10.0
) -> ForecastTrigger:
    """Create a weather-based forecast trigger."""
    service = get_auto_reforecast_service()
    
    # Create temperature trigger
    temp_result = await service.create_forecast_trigger(
        trigger_name=f"{trigger_name}_temp",
        data_source="weather",
        threshold_type="absolute",
        threshold_value=temperature_threshold,
        geography=geography
    )
    
    # Create humidity trigger
    humid_result = await service.create_forecast_trigger(
        trigger_name=f"{trigger_name}_humid",
        data_source="weather",
        threshold_type="absolute",
        threshold_value=humidity_threshold,
        geography=geography
    )
    
    # Return a mock ForecastTrigger
    return ForecastTrigger(
        trigger_id=trigger_name,
        name=trigger_name,
        description=f"Weather trigger for {geography}",
        conditions=[temp_result.data, humid_result.data] if temp_result.success and humid_result.success else [],
        forecast_config=ForecastRequest(
            forecast_type="load",
            target_variable="system_load",
            geography=geography,
            start_date=datetime.now(),
            end_date=datetime.now()
        )
    )


async def create_price_trigger(
    trigger_name: str,
    geography: str,
    price_volatility_threshold: float = 0.2
) -> ForecastTrigger:
    """Create a price volatility forecast trigger."""
    service = get_auto_reforecast_service()
    
    result = await service.create_forecast_trigger(
        trigger_name=trigger_name,
        data_source="price",
        threshold_type="volatility",
        threshold_value=price_volatility_threshold,
        geography=geography
    )
    
    # Return a mock ForecastTrigger
    return ForecastTrigger(
        trigger_id=trigger_name,
        name=trigger_name,
        description=f"Price volatility trigger for {geography}",
        conditions=[result.data] if result.success else [],
        forecast_config=ForecastRequest(
            forecast_type="price",
            target_variable="lmp",
            geography=geography,
            start_date=datetime.now(),
            end_date=datetime.now()
        )
    )
