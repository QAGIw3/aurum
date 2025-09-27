"""v2 Carbon and REC Data Integration API.

This module provides REST endpoints for:
- Carbon instrument registration and management
- Carbon pricing and cost calculations for assets
- Portfolio carbon exposure analysis
- REC trading data and compliance tracking
- Carbon market data and regulatory compliance
- Integration with scenarios and risk management
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.carbon_rec_service import (
    get_carbon_rec_service,
    CarbonInstrument,
    CarbonPricing,
    PortfolioCarbonExposure,
    RECTrading,
    CarbonInstrumentType,
    CarbonMarket
)
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/carbon-rec", tags=["carbon-rec"])


class CarbonInstrumentCreateRequest(BaseModel):
    """Request to register a carbon instrument."""

    instrument_type: CarbonInstrumentType = Field(..., description="Type of carbon instrument")
    market: CarbonMarket = Field(..., description="Carbon market/registry")
    vintage_year: int = Field(..., description="Vintage year for the instrument")
    expiry_date: Optional[datetime] = Field(None, description="Instrument expiry date")
    quantity_tons: float = Field(..., description="Quantity in tons of CO2")
    registry_id: str = Field(..., description="Registry identifier")
    project_id: Optional[str] = Field(None, description="Project identifier")
    project_type: Optional[str] = Field(None, description="Project type")
    location: str = Field(..., description="Geographic location")
    methodology: str = Field(..., description="Verification methodology")
    verification_standard: str = Field(..., description="Verification standard")
    verification_body: str = Field(..., description="Verification body")
    metadata: Dict[str, any] = Field(default_factory=dict, description="Additional metadata")


class CarbonPricingRequest(BaseModel):
    """Request for carbon pricing calculation."""

    asset_id: str = Field(..., description="Asset identifier")
    geography: str = Field(..., description="Geographic scope")
    start_date: datetime = Field(..., description="Start date for pricing")
    end_date: datetime = Field(..., description="End date for pricing")
    carbon_intensity: Optional[float] = Field(None, description="Carbon intensity (tons CO2/MWh)")


class PortfolioExposureRequest(BaseModel):
    """Request for portfolio carbon exposure analysis."""

    portfolio_id: str = Field(..., description="Portfolio identifier")
    scenario_id: str = Field(..., description="Scenario for analysis")
    analysis_date: datetime = Field(..., description="Analysis date")


class CarbonInstrumentResponse(BaseModel):
    """Response containing carbon instrument information."""

    instrument_id: str
    instrument_type: str
    market: str
    vintage_year: int
    expiry_date: Optional[datetime]
    quantity_tons: float
    status: str
    registry_id: str
    project_id: Optional[str]
    project_type: Optional[str]
    location: str
    methodology: str
    verification_standard: str
    verification_body: str
    issuance_date: datetime
    metadata: Dict[str, any]


class CarbonPricingResponse(BaseModel):
    """Response containing carbon pricing information."""

    asset_id: str
    timestamp: datetime
    geography: str
    carbon_intensity: float
    carbon_price: float
    carbon_cost: float
    carbon_cost_percent: float
    pricing_method: str
    market_source: str
    confidence: float
    metadata: Dict[str, any]


class PortfolioExposureResponse(BaseModel):
    """Response containing portfolio carbon exposure."""

    portfolio_id: str
    analysis_date: datetime
    total_emissions_tons: float
    total_carbon_cost: float
    carbon_intensity: float
    carbon_cost_per_mwh: float
    compliance_obligations: Dict[str, float]
    carbon_credits_held: Dict[str, int]
    net_carbon_position: float
    risk_metrics: Dict[str, float]
    scenario_impact: Dict[str, float]


class CarbonMarketDataResponse(BaseModel):
    """Response containing carbon market data."""

    market: str
    market_name: str
    data: List[Dict[str, any]]
    summary: Dict[str, any]


@router.post("/instruments", response_model=CarbonInstrumentResponse, status_code=201)
async def register_carbon_instrument(
    request: Request,
    instrument_data: CarbonInstrumentCreateRequest
) -> CarbonInstrumentResponse:
    """Register a new carbon instrument."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()

        # Create carbon instrument
        instrument = CarbonInstrument(
            instrument_id=str(uuid4()),
            instrument_type=instrument_data.instrument_type,
            market=instrument_data.market,
            vintage_year=instrument_data.vintage_year,
            expiry_date=instrument_data.expiry_date,
            quantity_tons=instrument_data.quantity_tons,
            registry_id=instrument_data.registry_id,
            project_id=instrument_data.project_id,
            project_type=instrument_data.project_type,
            location=instrument_data.location,
            methodology=instrument_data.methodology,
            verification_standard=instrument_data.verification_standard,
            verification_body=instrument_data.verification_body,
            issuance_date=datetime.utcnow(),
            metadata=instrument_data.metadata
        )

        # Register instrument
        success = await service.register_carbon_instrument(instrument)

        if not success:
            raise HTTPException(status_code=500, detail="Failed to register carbon instrument")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="register_carbon_instrument",
            query_time_ms=query_time_ms
        )

        return CarbonInstrumentResponse(
            instrument_id=instrument.instrument_id,
            instrument_type=instrument.instrument_type.value,
            market=instrument.market.value,
            vintage_year=instrument.vintage_year,
            expiry_date=instrument.expiry_date,
            quantity_tons=instrument.quantity_tons,
            status=instrument.status,
            registry_id=instrument.registry_id,
            project_id=instrument.project_id,
            project_type=instrument.project_type,
            location=instrument.location,
            methodology=instrument.methodology,
            verification_standard=instrument.verification_standard,
            verification_body=instrument.verification_body,
            issuance_date=instrument.issuance_date,
            metadata=instrument.metadata
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="register_carbon_instrument",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to register carbon instrument: {str(exc)}"
        )


@router.get("/pricing/{asset_id}", response_model=List[CarbonPricingResponse])
async def get_asset_carbon_pricing(
    request: Request,
    asset_id: str,
    geography: str = Query(..., description="Geographic scope"),
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    carbon_intensity: Optional[float] = Query(None, description="Carbon intensity override")
) -> List[CarbonPricingResponse]:
    """Get carbon pricing for an asset over time period."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()

        # Get carbon pricing
        pricing_data = await service.get_carbon_pricing(
            asset_id=asset_id,
            geography=geography,
            start_date=start_date,
            end_date=end_date
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        pricing_responses = [
            CarbonPricingResponse(
                asset_id=pricing.asset_id,
                timestamp=pricing.timestamp,
                geography=pricing.geography,
                carbon_intensity=pricing.carbon_intensity,
                carbon_price=pricing.carbon_price,
                carbon_cost=pricing.carbon_cost,
                carbon_cost_percent=pricing.carbon_cost_percent,
                pricing_method=pricing.pricing_method,
                market_source=pricing.market_source,
                confidence=pricing.confidence,
                metadata=pricing.metadata
            )
            for pricing in pricing_data
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_asset_carbon_pricing",
            query_time_ms=query_time_ms
        )

        return pricing_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_asset_carbon_pricing",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get asset carbon pricing: {str(exc)}"
        )


@router.post("/portfolios/{portfolio_id}/exposure", response_model=PortfolioExposureResponse, status_code=201)
async def calculate_portfolio_carbon_exposure(
    request: Request,
    portfolio_id: str,
    exposure_data: PortfolioExposureRequest
) -> PortfolioExposureResponse:
    """Calculate carbon exposure for a portfolio under a scenario."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()

        # Calculate exposure
        exposure = await service.calculate_portfolio_carbon_exposure(
            portfolio_id=exposure_data.portfolio_id,
            scenario_id=exposure_data.scenario_id,
            analysis_date=exposure_data.analysis_date
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="calculate_portfolio_carbon_exposure",
            query_time_ms=query_time_ms
        )

        return PortfolioExposureResponse(
            portfolio_id=exposure.portfolio_id,
            analysis_date=exposure.analysis_date,
            total_emissions_tons=exposure.total_emissions_tons,
            total_carbon_cost=exposure.total_carbon_cost,
            carbon_intensity=exposure.carbon_intensity,
            carbon_cost_per_mwh=exposure.carbon_cost_per_mwh,
            compliance_obligations=exposure.compliance_obligations,
            carbon_credits_held=exposure.carbon_credits_held,
            net_carbon_position=exposure.net_carbon_position,
            risk_metrics=exposure.risk_metrics,
            scenario_impact=exposure.scenario_impact
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="calculate_portfolio_carbon_exposure",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate portfolio carbon exposure: {str(exc)}"
        )


@router.get("/recs", response_model=List[Dict[str, any]])
async def get_rec_trading_data(
    request: Request,
    response: Response,
    geography: str = Query(..., description="Geographic scope"),
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date"),
    limit: int = Query(100, ge=1, le=1000)
) -> List[Dict[str, any]]:
    """Get REC trading data for geography and time period."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()

        # Get REC trading data
        recs = await service.get_rec_trading_data(
            geography=geography,
            start_date=start_date,
            end_date=end_date
        )

        # Apply limit
        limited_recs = recs[:limit]

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        rec_responses = [
            {
                "rec_id": rec.rec_id,
                "vintage_year": rec.vintage_year,
                "generation_date": rec.generation_date,
                "generation_source": rec.generation_source,
                "generation_location": rec.generation_location,
                "quantity_mwh": rec.quantity_mwh,
                "status": rec.status,
                "current_owner": rec.current_owner,
                "trading_history": rec.trading_history,
                "metadata": rec.metadata
            }
            for rec in limited_recs
        ]

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_rec_trading_data",
            query_time_ms=query_time_ms
        )

        return rec_responses

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_rec_trading_data",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get REC trading data: {str(exc)}"
        )


@router.get("/markets/{market}", response_model=CarbonMarketDataResponse)
async def get_carbon_market_data(
    request: Request,
    market: CarbonMarket,
    start_date: datetime = Query(..., description="Start date"),
    end_date: datetime = Query(..., description="End date")
) -> CarbonMarketDataResponse:
    """Get carbon market price and volume data."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()

        # Get market data
        market_data = await service.get_carbon_market_data(
            market=market,
            start_date=start_date,
            end_date=end_date
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_carbon_market_data",
            query_time_ms=query_time_ms
        )

        return CarbonMarketDataResponse(
            market=market_data["market"],
            market_name=market_data["market_name"],
            data=market_data["data"],
            summary=market_data["summary"]
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_carbon_market_data",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get carbon market data: {str(exc)}"
        )


@router.get("/health")
async def get_carbon_rec_health(
    request: Request,
    response: Response
) -> Dict[str, any]:
    """Get carbon REC service health status."""
    start_time = time.perf_counter()

    try:
        service = get_carbon_rec_service()
        health = await service.get_service_health()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="get_carbon_rec_health",
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
            operation="get_carbon_rec_health",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get carbon REC health: {str(exc)}"
        )
