"""v2 Probabilistic Forecasting API with quantile and interval support.

This module provides probabilistic forecasting endpoints for load and price predictions
with proper OpenAPI contracts and support for p10/p50/p90 quantiles.

Features:
- Probabilistic load forecasting with confidence intervals
- Price forecasting with quantiles (p10, p50, p90)
- Time-series forecasting with uncertainty quantification
- Integration with feature store for ML model inputs
- OpenAPI-compliant contracts with detailed schemas
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Literal, Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field, validator
from enum import Enum

from ..deps import get_settings, get_cache_manager
from ..telemetry.context import get_request_id, get_tenant_id
from aurum.core import AurumSettings
from ..cache.cache import CacheManager
from ..services.feature_store_service import get_feature_store_service
from ...observability.telemetry_facade import get_telemetry_facade, MetricCategory


class ForecastType(str, Enum):
    """Types of forecasts supported."""
    LOAD = "load"
    PRICE = "price"
    RENEWABLE = "renewable"
    DEMAND = "demand"


class QuantileLevel(str, Enum):
    """Standard quantile levels for probabilistic forecasts."""
    P10 = "p10"    # 10th percentile
    P25 = "p25"    # 25th percentile
    P50 = "p50"    # 50th percentile (median)
    P75 = "p75"    # 75th percentile
    P90 = "p90"    # 90th percentile


class ForecastInterval(str, Enum):
    """Time intervals for forecasts."""
    HOURLY = "1H"
    DAILY = "1D"
    WEEKLY = "7D"
    MONTHLY = "1M"


class ForecastRequest(BaseModel):
    """Request for probabilistic forecast generation."""

    forecast_type: ForecastType = Field(..., description="Type of forecast to generate")
    target_variable: str = Field(..., description="Variable to forecast (e.g., 'load_mw', 'lmp_price')")
    geography: str = Field("US", description="Geographic scope for forecast")
    start_date: datetime = Field(..., description="Start date for forecast period")
    end_date: datetime = Field(..., description="End date for forecast period")
    quantiles: List[QuantileLevel] = Field(
        default=[QuantileLevel.P10, QuantileLevel.P50, QuantileLevel.P90],
        description="Quantiles to compute"
    )
    interval: ForecastInterval = Field(ForecastInterval.HOURLY, description="Forecast granularity")
    model_version: Optional[str] = Field(None, description="Specific model version to use")
    include_feature_importance: bool = Field(False, description="Include feature importance scores")
    scenario_id: Optional[str] = Field(None, description="Scenario context for forecast")

    @validator('end_date')
    def end_after_start(cls, v, values):
        """Validate that end_date is after start_date."""
        if 'start_date' in values and v <= values['start_date']:
            raise ValueError('end_date must be after start_date')
        return v

    @validator('end_date')
    def reasonable_horizon(cls, v, values):
        """Validate forecast horizon is reasonable."""
        if 'start_date' in values:
            horizon_days = (v - values['start_date']).days
            if horizon_days > 365:
                raise ValueError('Forecast horizon cannot exceed 365 days')
        return v


class ForecastPoint(BaseModel):
    """A single forecast data point with quantiles."""

    timestamp: datetime = Field(..., description="Forecast timestamp")
    values: Dict[str, float] = Field(..., description="Quantile values (p10, p50, p90, etc.)")
    confidence_interval: Optional[Dict[str, float]] = Field(
        None,
        description="Confidence interval bounds if available"
    )
    metadata: Dict[str, str] = Field(default_factory=dict, description="Additional metadata")


class ForecastResponse(BaseModel):
    """Response containing probabilistic forecast data."""

    forecast_id: str = Field(..., description="Unique forecast identifier")
    request_id: str = Field(..., description="Request correlation ID")
    forecast_type: ForecastType = Field(..., description="Type of forecast generated")
    target_variable: str = Field(..., description="Variable that was forecasted")
    geography: str = Field(..., description="Geographic scope")
    model_version: str = Field(..., description="Model version used for forecast")
    forecast_points: List[ForecastPoint] = Field(..., description="Forecast data points")
    generated_at: datetime = Field(..., description="When forecast was generated")
    valid_from: datetime = Field(..., description="Start of forecast validity period")
    valid_until: datetime = Field(..., description="End of forecast validity period")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Additional forecast metadata")

    @property
    def forecast_horizon_hours(self) -> int:
        """Calculate forecast horizon in hours."""
        if not self.forecast_points:
            return 0
        return int((self.forecast_points[-1].timestamp - self.forecast_points[0].timestamp).total_seconds() / 3600)


class FeatureImportance(BaseModel):
    """Feature importance scores for model interpretability."""

    feature_name: str = Field(..., description="Name of the feature")
    importance_score: float = Field(..., description="Importance score (0-1)")
    feature_type: str = Field(..., description="Type of feature (weather, load, price, etc.)")
    description: str = Field(..., description="Human-readable description")


class ForecastWithExplanation(BaseModel):
    """Extended forecast response with feature importance."""

    forecast: ForecastResponse = Field(..., description="Base forecast data")
    feature_importance: List[FeatureImportance] = Field(
        default_factory=list,
        description="Feature importance scores if requested"
    )
    model_metadata: Dict[str, str] = Field(
        default_factory=dict,
        description="Model metadata and performance metrics"
    )


class ForecastBatchRequest(BaseModel):
    """Request for batch forecast generation."""

    forecasts: List[ForecastRequest] = Field(..., description="List of forecasts to generate")
    batch_id: Optional[str] = Field(None, description="Optional batch identifier")


class ForecastBatchResponse(BaseModel):
    """Response for batch forecast generation."""

    batch_id: str = Field(..., description="Batch identifier")
    forecasts: List[ForecastResponse] = Field(..., description="Generated forecasts")
    total_forecasts: int = Field(..., description="Number of forecasts generated")
    successful_forecasts: int = Field(..., description="Number of successful forecasts")
    failed_forecasts: int = Field(..., description="Number of failed forecasts")
    errors: List[Dict[str, str]] = Field(default_factory=list, description="List of errors")
    processing_time_seconds: float = Field(..., description="Total processing time")


class ForecastHistoryRequest(BaseModel):
    """Request to retrieve forecast history."""

    forecast_type: ForecastType = Field(..., description="Type of forecast")
    target_variable: str = Field(..., description="Target variable")
    geography: str = Field("US", description="Geographic scope")
    start_date: datetime = Field(..., description="Start date for history")
    end_date: datetime = Field(..., description="End date for history")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of records")
    include_actuals: bool = Field(False, description="Include actual values for comparison")


class ForecastHistoryPoint(BaseModel):
    """Historical forecast point with actual vs predicted."""

    timestamp: datetime = Field(..., description="Data timestamp")
    forecast_values: Dict[str, float] = Field(..., description="Historical forecast quantiles")
    actual_value: Optional[float] = Field(None, description="Actual observed value")
    forecast_error: Optional[float] = Field(None, description="Error (actual - forecast)")
    metadata: Dict[str, str] = Field(default_factory=dict, description="Additional metadata")


class ForecastHistoryResponse(BaseModel):
    """Response containing historical forecast performance."""

    forecast_type: ForecastType = Field(..., description="Type of forecast")
    target_variable: str = Field(..., description="Target variable")
    geography: str = Field(..., description="Geographic scope")
    history_points: List[ForecastHistoryPoint] = Field(..., description="Historical data points")
    performance_metrics: Dict[str, float] = Field(default_factory=dict, description="Performance metrics")
    retrieved_at: datetime = Field(..., description="When history was retrieved")


router = APIRouter(prefix="/v2", tags=["forecasting"])


@router.post("/forecasts", response_model=ForecastResponse)
async def generate_forecast(
    request: ForecastRequest,
    response: Response,
    settings: AurumSettings = Depends(get_settings),
    cache_manager: CacheManager = Depends(get_cache_manager)
) -> ForecastResponse:
    """Generate a probabilistic forecast with quantiles.

    This endpoint generates probabilistic forecasts for load or price data
    with support for multiple quantiles (p10, p50, p90) and confidence intervals.

    Args:
        request: Forecast generation parameters
        response: FastAPI response object
        settings: Application settings
        cache_manager: Cache manager dependency

    Returns:
        Probabilistic forecast with quantiles
    """
    telemetry = get_telemetry_facade()
    request_id = get_request_id() or str(uuid4())
    tenant_id = get_tenant_id()

    start_time = time.time()

    try:
        telemetry.info(
            "Starting probabilistic forecast generation",
            forecast_type=request.forecast_type.value,
            target_variable=request.target_variable,
            geography=request.geography,
            horizon_days=(request.end_date - request.start_date).days,
            quantiles=[q.value for q in request.quantiles],
            category="forecast"
        )

        # Generate forecast using feature store service
        feature_service = get_feature_store_service()

        # Get features for modeling
        features, _ = await feature_service.get_features_for_modeling(
            start_date=request.start_date,
            end_date=request.end_date,
            geography=request.geography,
            target_variable=request.target_variable,
            scenario_id=request.scenario_id
        )

        # Generate probabilistic forecast (simplified implementation)
        forecast_points = await _generate_probabilistic_forecast(
            features,
            request,
            request_id
        )

        # Create forecast response
        forecast_response = ForecastResponse(
            forecast_id=str(uuid4()),
            request_id=request_id,
            forecast_type=request.forecast_type,
            target_variable=request.target_variable,
            geography=request.geography,
            model_version=request.model_version or "v1.0",
            forecast_points=forecast_points,
            generated_at=datetime.utcnow(),
            valid_from=request.start_date,
            valid_until=request.end_date,
            metadata={
                "quantiles": [q.value for q in request.quantiles],
                "interval": request.interval.value,
                "feature_count": len(features),
                "model_type": "probabilistic_regression"
            }
        )

        # Add ETag for caching
        forecast_data = forecast_response.dict()
        etag = await _compute_forecast_etag(forecast_data)
        response.headers["ETag"] = etag

        # Cache forecast
        cache_key = f"forecast:{forecast_response.forecast_id}"
        await cache_manager.set(
            cache_key,
            forecast_data,
            ttl_seconds=3600  # 1 hour cache
        )

        duration = time.time() - start_time

        telemetry.info(
            "Probabilistic forecast generated successfully",
            forecast_id=forecast_response.forecast_id,
            point_count=len(forecast_points),
            duration_seconds=duration,
            category="forecast"
        )

        # Record metrics
        telemetry.increment_counter("forecasts_generated", category=MetricCategory.BUSINESS)
        telemetry.record_histogram(
            "forecast_generation_duration",
            duration,
            category=MetricCategory.PERFORMANCE,
            forecast_type=request.forecast_type.value
        )

        return forecast_response

    except Exception as e:
        duration = time.time() - start_time

        telemetry.error(
            "Forecast generation failed",
            error=str(e),
            forecast_type=request.forecast_type.value,
            duration_seconds=duration,
            category="forecast"
        )

        telemetry.increment_counter(
            "forecast_generation_errors",
            category=MetricCategory.RELIABILITY,
            error_type=type(e).__name__
        )

        raise HTTPException(
            status_code=500,
            detail=f"Forecast generation failed: {str(e)}"
        )


@router.get("/forecasts/{forecast_id}", response_model=ForecastResponse)
async def get_forecast(
    forecast_id: str,
    response: Response,
    settings: AurumSettings = Depends(get_settings),
    cache_manager: CacheManager = Depends(get_cache_manager)
) -> ForecastResponse:
    """Retrieve a previously generated forecast by ID.

    Args:
        forecast_id: Unique forecast identifier
        response: FastAPI response object
        settings: Application settings
        cache_manager: Cache manager dependency

    Returns:
        Forecast data
    """
    telemetry = get_telemetry_facade()

    try:
        # Check cache first
        cache_key = f"forecast:{forecast_id}"
        cached_forecast = await cache_manager.get(cache_key)

        if cached_forecast:
            # Add ETag for caching
            etag = await _compute_forecast_etag(cached_forecast)
            response.headers["ETag"] = etag

            telemetry.info("Forecast retrieved from cache", forecast_id=forecast_id)
            return ForecastResponse(**cached_forecast)

        raise HTTPException(status_code=404, detail="Forecast not found")

    except HTTPException:
        raise
    except Exception as e:
        telemetry.error("Forecast retrieval failed", forecast_id=forecast_id, error=str(e))
        raise HTTPException(status_code=500, detail="Failed to retrieve forecast")


@router.post("/forecasts/batch", response_model=ForecastBatchResponse)
async def generate_batch_forecasts(
    request: ForecastBatchRequest,
    settings: AurumSettings = Depends(get_settings)
) -> ForecastBatchResponse:
    """Generate multiple forecasts in batch.

    Args:
        request: Batch forecast request
        settings: Application settings

    Returns:
        Batch forecast results
    """
    telemetry = get_telemetry_facade()
    batch_id = request.batch_id or str(uuid4())

    start_time = time.time()

    try:
        telemetry.info(
            "Starting batch forecast generation",
            batch_id=batch_id,
            forecast_count=len(request.forecasts),
            category="forecast"
        )

        successful_forecasts = []
        errors = []

        # Process forecasts concurrently with controlled concurrency
        semaphore = asyncio.Semaphore(10)  # Limit concurrent forecasts

        async def generate_single_forecast(forecast_request: ForecastRequest):
            async with semaphore:
                try:
                    forecast = await generate_forecast(forecast_request, Response())
                    return forecast
                except Exception as e:
                    return {"error": str(e), "request": forecast_request.dict()}

        # Execute batch
        tasks = [generate_single_forecast(fr) for fr in request.forecasts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for result in results:
            if isinstance(result, Exception):
                errors.append({"error": str(result)})
            elif "error" in result:
                errors.append(result)
            else:
                successful_forecasts.append(result)

        duration = time.time() - start_time

        batch_response = ForecastBatchResponse(
            batch_id=batch_id,
            forecasts=successful_forecasts,
            total_forecasts=len(request.forecasts),
            successful_forecasts=len(successful_forecasts),
            failed_forecasts=len(errors),
            errors=errors,
            processing_time_seconds=duration
        )

        telemetry.info(
            "Batch forecast generation completed",
            batch_id=batch_id,
            successful=len(successful_forecasts),
            failed=len(errors),
            duration_seconds=duration,
            category="forecast"
        )

        # Record batch metrics
        telemetry.increment_counter(
            "forecast_batches_processed",
            value=len(request.forecasts),
            category=MetricCategory.BUSINESS
        )

        return batch_response

    except Exception as e:
        duration = time.time() - start_time

        telemetry.error(
            "Batch forecast generation failed",
            batch_id=batch_id,
            error=str(e),
            duration_seconds=duration,
            category="forecast"
        )

        raise HTTPException(
            status_code=500,
            detail=f"Batch forecast generation failed: {str(e)}"
        )


@router.get("/forecasts/history", response_model=ForecastHistoryResponse)
async def get_forecast_history(
    request: ForecastHistoryRequest,
    settings: AurumSettings = Depends(get_settings)
) -> ForecastHistoryResponse:
    """Retrieve historical forecast performance data.

    Args:
        request: History request parameters
        settings: Application settings

    Returns:
        Historical forecast performance data
    """
    telemetry = get_telemetry_facade()

    try:
        # In real implementation, would query historical forecast database
        # For now, return mock data structure
        history_points = []

        current_date = request.start_date
        while current_date <= request.end_date:
            # Generate mock forecast vs actual data
            forecast_values = {
                "p10": 100.0 + (current_date.hour * 0.1),  # Mock p10
                "p50": 120.0 + (current_date.hour * 0.2),  # Mock p50
                "p90": 150.0 + (current_date.hour * 0.3),  # Mock p90
            }

            actual_value = forecast_values["p50"] + (2.0 if current_date.weekday() >= 5 else 0.0)
            forecast_error = actual_value - forecast_values["p50"]

            point = ForecastHistoryPoint(
                timestamp=current_date,
                forecast_values=forecast_values,
                actual_value=actual_value if request.include_actuals else None,
                forecast_error=forecast_error if request.include_actuals else None,
                metadata={
                    "day_of_week": str(current_date.weekday()),
                    "hour": str(current_date.hour)
                }
            )

            history_points.append(point)

            if len(history_points) >= request.limit:
                break

            current_date += timedelta(hours=1)

        # Calculate performance metrics
        performance_metrics = {}
        if request.include_actuals and history_points:
            errors = [p.forecast_error for p in history_points if p.forecast_error is not None]
            if errors:
                performance_metrics = {
                    "mae": sum(abs(e) for e in errors) / len(errors),  # Mean Absolute Error
                    "rmse": (sum(e**2 for e in errors) / len(errors))**0.5,  # Root Mean Square Error
                    "mape": sum(abs(e / p.actual_value) for e, p in zip(errors, history_points) if p.actual_value) / len(errors) * 100  # Mean Absolute Percentage Error
                }

        return ForecastHistoryResponse(
            forecast_type=request.forecast_type,
            target_variable=request.target_variable,
            geography=request.geography,
            history_points=history_points[:request.limit],
            performance_metrics=performance_metrics,
            retrieved_at=datetime.utcnow()
        )

    except Exception as e:
        telemetry.error(
            "Forecast history retrieval failed",
            error=str(e),
            forecast_type=request.forecast_type.value,
            category="forecast"
        )
        raise HTTPException(status_code=500, detail="Failed to retrieve forecast history")


async def _generate_probabilistic_forecast(
    features: Dict[str, List[float]],
    request: ForecastRequest,
    request_id: str
) -> List[ForecastPoint]:
    """Generate probabilistic forecast using ML model.

    In a real implementation, this would:
    1. Load the appropriate ML model (e.g., from MLflow)
    2. Run inference with uncertainty quantification
    3. Return quantiles and confidence intervals

    For now, returns mock probabilistic data.
    """
    telemetry = get_telemetry_facade()

    # Mock implementation - in reality would use actual ML model
    forecast_points = []

    current_time = request.start_date
    while current_time <= request.end_date:
        # Generate mock quantile values based on time of day and day of week
        base_value = 100.0
        hour_factor = current_time.hour / 24.0
        day_factor = 1.0 if current_time.weekday() < 5 else 0.8  # Lower on weekends

        # Generate different quantiles with realistic spread
        p50_value = base_value * (1.0 + 0.3 * hour_factor) * day_factor

        # Create realistic quantile spread (±20% around median)
        spread_factor = 0.2
        p10_value = p50_value * (1 - spread_factor)
        p25_value = p50_value * (1 - spread_factor * 0.5)
        p75_value = p50_value * (1 + spread_factor * 0.5)
        p90_value = p50_value * (1 + spread_factor)

        values = {}
        for quantile in request.quantiles:
            if quantile == QuantileLevel.P10:
                values[quantile.value] = p10_value
            elif quantile == QuantileLevel.P25:
                values[quantile.value] = p25_value
            elif quantile == QuantileLevel.P50:
                values[quantile.value] = p50_value
            elif quantile == QuantileLevel.P75:
                values[quantile.value] = p75_value
            elif quantile == QuantileLevel.P90:
                values[quantile.value] = p90_value

        # Create confidence interval (p10-p90 range)
        confidence_interval = {
            "lower": p10_value,
            "upper": p90_value,
            "width": p90_value - p10_value
        }

        forecast_point = ForecastPoint(
            timestamp=current_time,
            values=values,
            confidence_interval=confidence_interval,
            metadata={
                "hour": str(current_time.hour),
                "day_of_week": str(current_time.weekday()),
                "model_confidence": "0.85"
            }
        )

        forecast_points.append(forecast_point)

        # Advance time based on interval
        if request.interval == ForecastInterval.HOURLY:
            current_time += timedelta(hours=1)
        elif request.interval == ForecastInterval.DAILY:
            current_time += timedelta(days=1)
        else:
            current_time += timedelta(hours=1)  # Default to hourly

    return forecast_points


async def _compute_forecast_etag(forecast_data: Dict[str, Any]) -> str:
    """Compute ETag for forecast response.

    Args:
        forecast_data: Forecast data dictionary

    Returns:
        ETag string
    """
    import hashlib
    import json

    # Create deterministic representation for ETag
    etag_data = {
        "forecast_id": forecast_data.get("forecast_id"),
        "forecast_type": forecast_data.get("forecast_type"),
        "target_variable": forecast_data.get("target_variable"),
        "geography": forecast_data.get("geography"),
        "model_version": forecast_data.get("model_version"),
        "point_count": len(forecast_data.get("forecast_points", [])),
        "generated_at": forecast_data.get("generated_at")
    }

    etag_string = json.dumps(etag_data, sort_keys=True)
    return hashlib.md5(etag_string.encode()).hexdigest()


# Register router in v2 module
__all__ = [
    "router",
    "ForecastRequest",
    "ForecastResponse",
    "ForecastPoint",
    "ForecastType",
    "QuantileLevel",
    "ForecastInterval"
]
