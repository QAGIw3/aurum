"""v2 Risk Engine API for Monte Carlo + VaR/CVaR calculations.

This module provides REST endpoints for:
- Portfolio risk calculation and VaR/CVaR analysis
- Risk scenario modeling and stress testing
- Portfolio aggregation and correlation analysis
- Risk distribution configuration and management
- Integration with forecasting and carbon pricing
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.risk_engine_service import (
    get_risk_engine_service,
    PortfolioPosition,
    RiskDistributionConfig,
    RiskScenario,
    RiskDistributionType,
    CorrelationModel
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/risk-engine", tags=["risk-engine"])


class PositionCreateRequest(BaseModel):
    """Request to add a portfolio position."""

    asset_id: str = Field(..., description="Asset identifier")
    position_type: str = Field(..., description="Position type (long, short, hedge)")
    notional_value: float = Field(..., description="Notional value")
    currency: str = Field("USD", description="Currency")
    maturity_date: Optional[datetime] = Field(None, description="Maturity date")
    risk_factors: Dict[str, float] = Field(..., description="Risk factor sensitivities")
    metadata: Dict[str, any] = Field(default_factory=dict, description="Additional metadata")


class RiskCalculationRequest(BaseModel):
    """Request for risk calculation."""

    portfolio_id: str = Field(..., description="Portfolio identifier")
    confidence_level: float = Field(0.95, description="Confidence level for VaR")
    time_horizon_days: int = Field(1, description="Time horizon in days")
    num_simulations: int = Field(10000, description="Number of Monte Carlo simulations")


class RiskDistributionConfigRequest(BaseModel):
    """Request to configure risk distribution."""

    distribution_type: RiskDistributionType = Field(..., description="Distribution type")
    parameters: Dict[str, float] = Field(..., description="Distribution parameters")
    correlation_model: CorrelationModel = Field(CorrelationModel.PEARSON, description="Correlation model")
    correlation_matrix: Optional[List[List[float]]] = Field(None, description="Correlation matrix")
    volatility_regime: str = Field("normal", description="Volatility regime")
    fat_tail_adjustment: bool = Field(True, description="Enable fat tail adjustment")
    seasonality_enabled: bool = Field(True, description="Enable seasonality")


class ScenarioCreateRequest(BaseModel):
    """Request to create a risk scenario."""

    scenario_id: str = Field(..., description="Scenario identifier")
    scenario_name: str = Field(..., description="Scenario name")
    description: str = Field(..., description="Scenario description")
    probability: float = Field(..., description="Scenario probability")
    risk_factors: Dict[str, float] = Field(..., description="Risk factor shocks")
    correlation_shocks: Dict[str, float] = Field(..., description="Correlation changes")
    duration_days: int = Field(1, description="Duration in days")
    market_impact: str = Field("moderate", description="Market impact level")


class PositionResponse(BaseModel):
    """Response containing position information."""

    asset_id: str
    position_type: str
    notional_value: float
    currency: str
    maturity_date: Optional[datetime]
    risk_factors: Dict[str, float]
    metadata: Dict[str, any]


class RiskMetricsResponse(BaseModel):
    """Response containing risk metrics."""

    portfolio_id: str
    calculation_date: datetime
    risk_metrics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    methodology: str
    parameters: Dict[str, any]
    execution_time: float


class PortfolioAggregationResponse(BaseModel):
    """Response containing portfolio aggregation."""

    portfolio_id: str
    aggregation_date: datetime
    total_var: float
    total_cvar: float
    total_volatility: float
    diversification_benefit: float
    concentration_risk: float
    liquidity_risk: float
    counterparty_risk: float
    asset_breakdown: Dict[str, Dict[str, float]]
    risk_attribution: Dict[str, float]


class ScenarioAnalysisResponse(BaseModel):
    """Response containing scenario analysis results."""

    portfolio_id: str
    scenario_results: Dict[str, Dict[str, float]]
    comparison_metrics: Dict[str, any]


@router.post("/portfolios/{portfolio_id}/positions", response_model=PositionResponse, status_code=201)
async def add_portfolio_position(
    request: Request,
    portfolio_id: str,
    position_data: PositionCreateRequest
) -> PositionResponse:
    """Add a position to a portfolio for risk analysis."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()

        # Create position
        position = PortfolioPosition(
            asset_id=position_data.asset_id,
            position_type=position_data.position_type,
            notional_value=position_data.notional_value,
            currency=position_data.currency,
            maturity_date=position_data.maturity_date,
            risk_factors=position_data.risk_factors,
            metadata=position_data.metadata
        )

        # Add position to portfolio
        await service.add_portfolio_position(portfolio_id, position)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="add_portfolio_position",
            query_time_ms=query_time_ms
        )

        return PositionResponse(
            asset_id=position.asset_id,
            position_type=position.position_type,
            notional_value=position.notional_value,
            currency=position.currency,
            maturity_date=position.maturity_date,
            risk_factors=position.risk_factors,
            metadata=position.metadata
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="add_portfolio_position",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add portfolio position: {str(exc)}"
        )


@router.post("/risk/calculate", response_model=RiskMetricsResponse, status_code=201)
async def calculate_portfolio_risk(
    request: Request,
    risk_data: RiskCalculationRequest
) -> RiskMetricsResponse:
    """Calculate Value at Risk and other risk metrics for a portfolio."""
    start_time = time.perf_counter()

    try:
        from ..services.risk_engine_service import calculate_portfolio_risk_metrics

        # Calculate risk metrics
        result = await calculate_portfolio_risk_metrics(
            portfolio_id=risk_data.portfolio_id,
            confidence_level=risk_data.confidence_level,
            time_horizon_days=risk_data.time_horizon_days
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="calculate_portfolio_risk",
            query_time_ms=query_time_ms
        )

        return RiskMetricsResponse(
            portfolio_id=result.portfolio_id,
            calculation_date=result.calculation_date,
            risk_metrics=result.risk_metrics,
            confidence_intervals=result.confidence_intervals,
            methodology=result.methodology,
            parameters=result.parameters,
            execution_time=result.execution_time
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="calculate_portfolio_risk",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate portfolio risk: {str(exc)}"
        )


@router.post("/portfolios/{portfolio_id}/aggregate", response_model=PortfolioAggregationResponse, status_code=201)
async def aggregate_portfolio_risk(
    request: Request,
    portfolio_id: str,
    include_correlations: bool = Query(True, description="Include correlation analysis")
) -> PortfolioAggregationResponse:
    """Calculate portfolio-level risk aggregation."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()

        # Calculate aggregation
        aggregation = await service.calculate_portfolio_aggregation(
            portfolio_id=portfolio_id,
            include_correlations=include_correlations
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="aggregate_portfolio_risk",
            query_time_ms=query_time_ms
        )

        return PortfolioAggregationResponse(
            portfolio_id=aggregation.portfolio_id,
            aggregation_date=aggregation.aggregation_date,
            total_var=aggregation.total_var,
            total_cvar=aggregation.total_cvar,
            total_volatility=aggregation.total_volatility,
            diversification_benefit=aggregation.diversification_benefit,
            concentration_risk=aggregation.concentration_risk,
            liquidity_risk=aggregation.liquidity_risk,
            counterparty_risk=aggregation.counterparty_risk,
            asset_breakdown=aggregation.asset_breakdown,
            risk_attribution=aggregation.risk_attribution
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="aggregate_portfolio_risk",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to aggregate portfolio risk: {str(exc)}"
        )


@router.get("/portfolios/{portfolio_id}/dashboard", response_model=Dict[str, any])
async def get_risk_dashboard(
    request: Request,
    portfolio_id: str,
    time_horizon_days: int = Query(1, description="Time horizon for analysis")
) -> Dict[str, any]:
    """Get comprehensive risk dashboard for portfolio."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()

        # Get risk dashboard
        dashboard = await service.get_risk_dashboard(
            portfolio_id=portfolio_id,
            time_horizon_days=time_horizon_days
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_risk_dashboard",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="get_risk_dashboard",
                query_time_ms=query_time_ms
            ),
            "data": dashboard
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_risk_dashboard",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get risk dashboard: {str(exc)}"
        )


@router.post("/scenarios/{portfolio_id}/stress-test", response_model=ScenarioAnalysisResponse, status_code=201)
async def run_stress_test(
    request: Request,
    portfolio_id: str,
    scenarios: List[ScenarioCreateRequest] = Field(..., description="Stress test scenarios")
) -> ScenarioAnalysisResponse:
    """Run stress test scenarios on portfolio."""
    start_time = time.perf_counter()

    try:
        # Convert to service format
        risk_scenarios = [
            RiskScenario(
                scenario_id=scenario.scenario_id,
                scenario_name=scenario.scenario_name,
                description=scenario.description,
                probability=scenario.probability,
                risk_factors=scenario.risk_factors,
                correlation_shocks=scenario.correlation_shocks,
                duration_days=scenario.duration_days,
                market_impact=scenario.market_impact
            )
            for scenario in scenarios
        ]

        from ..services.risk_engine_service import run_stress_test

        # Run stress test
        results = await run_stress_test(portfolio_id, risk_scenarios)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="run_stress_test",
            query_time_ms=query_time_ms
        )

        return ScenarioAnalysisResponse(
            portfolio_id=results["portfolio_id"],
            scenario_results=results["scenario_results"],
            comparison_metrics=results["comparison_metrics"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="run_stress_test",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to run stress test: {str(exc)}"
        )


@router.put("/distributions/{asset_type}", response_model=Dict[str, any])
async def configure_risk_distribution(
    request: Request,
    asset_type: str,
    config: RiskDistributionConfigRequest
) -> Dict[str, any]:
    """Configure risk distribution for asset type."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()

        # Convert to service format
        distribution_config = RiskDistributionConfig(
            distribution_type=config.distribution_type,
            parameters=config.parameters,
            correlation_model=config.correlation_model,
            correlation_matrix=config.correlation_matrix,
            volatility_regime=config.volatility_regime,
            fat_tail_adjustment=config.fat_tail_adjustment,
            seasonality_enabled=config.seasonality_enabled
        )

        # Set distribution
        await service.set_risk_distribution(asset_type, distribution_config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="configure_risk_distribution",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="configure_risk_distribution",
                query_time_ms=query_time_ms
            ),
            "data": {
                "message": "Risk distribution configured successfully",
                "asset_type": asset_type,
                "distribution_type": config.distribution_type.value
            }
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="configure_risk_distribution",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to configure risk distribution: {str(exc)}"
        )


@router.get("/correlations/{portfolio_id}", response_model=Dict[str, any])
async def get_portfolio_correlations(
    request: Request,
    portfolio_id: str
) -> Dict[str, any]:
    """Get correlation matrix for portfolio positions."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()

        # Get correlations
        correlations = await service.calculate_portfolio_correlations(portfolio_id)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_portfolio_correlations",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": correlations
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_portfolio_correlations",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get portfolio correlations: {str(exc)}"
        )


@router.get("/health")
async def get_risk_engine_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get risk engine service health status."""
    start_time = time.perf_counter()

    try:
        service = get_risk_engine_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_risk_engine_health",
            query_time_ms=query_time_ms
        )

        return {
            "meta": meta,
            "data": health
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_risk_engine_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get risk engine health: {str(exc)}"
        )
