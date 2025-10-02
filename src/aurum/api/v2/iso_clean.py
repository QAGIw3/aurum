"""V2 ISO Market API endpoints using clean architecture."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ...application.energy.iso_service import (
    IsoApplicationService,
    CreateIsoMarketCommand,
    AddLMPDataCommand,
    AddLoadDataCommand,
    AddGenerationMixCommand,
    IsoMarketDTO,
)


# Request/Response Models

class CreateIsoMarketRequest(BaseModel):
    """Request model for creating an ISO market."""
    
    tenant_id: str = Field(..., description="Tenant identifier")
    iso_code: str = Field(..., description="ISO code (e.g., PJM, CAISO)", max_length=10)
    iso_name: str = Field(..., description="Full ISO name", max_length=255)
    timezone: str = Field(..., description="Timezone (e.g., America/New_York)", max_length=50)
    
    class Config:
        json_schema_extra = {
            "example": {
                "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
                "iso_code": "PJM",
                "iso_name": "PJM Interconnection",
                "timezone": "America/New_York"
            }
        }


class AddLMPDataRequest(BaseModel):
    """Request model for adding LMP data."""
    
    node_id: str = Field(..., description="Node identifier")
    energy_price: Decimal = Field(..., description="Energy component price")
    congestion_price: Decimal = Field(..., description="Congestion component price")
    loss_price: Decimal = Field(..., description="Loss component price")
    timestamp: datetime = Field(..., description="Data timestamp")
    market_type: str = Field(..., description="Market type (DAM, RTM, HAM)")
    location_zone: Optional[str] = Field(default=None, description="Location zone")
    location_node: Optional[str] = Field(default=None, description="Location node")
    
    class Config:
        json_schema_extra = {
            "example": {
                "node_id": "NODE123",
                "energy_price": "45.50",
                "congestion_price": "2.30",
                "loss_price": "0.20",
                "timestamp": "2025-10-02T12:00:00Z",
                "market_type": "DAM",
                "location_zone": "WEST"
            }
        }


class AddLoadDataRequest(BaseModel):
    """Request model for adding load data."""
    
    zone_id: str = Field(..., description="Zone identifier")
    load_mw: Decimal = Field(..., description="Load in megawatts", gt=0)
    timestamp: datetime = Field(..., description="Data timestamp")
    forecast: bool = Field(default=False, description="Is this forecasted load?")
    
    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": "ZONE_WEST",
                "load_mw": "12500.50",
                "timestamp": "2025-10-02T12:00:00Z",
                "forecast": False
            }
        }


class AddGenerationMixRequest(BaseModel):
    """Request model for adding generation mix data."""
    
    zone_id: str = Field(..., description="Zone identifier")
    fuel_type: str = Field(..., description="Fuel type (e.g., coal, gas, solar)")
    generation_mw: Decimal = Field(..., description="Generation in megawatts", ge=0)
    percentage: Decimal = Field(..., description="Percentage of total generation", ge=0, le=100)
    timestamp: datetime = Field(..., description="Data timestamp")
    
    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": "ZONE_WEST",
                "fuel_type": "solar",
                "generation_mw": "2500.75",
                "percentage": "20.5",
                "timestamp": "2025-10-02T12:00:00Z"
            }
        }


class IsoMarketResponse(BaseModel):
    """Response model for ISO market."""
    
    id: str
    tenant_id: str
    iso_code: str
    iso_name: str
    timezone: str
    active: bool
    created_at: datetime
    updated_at: datetime


class ErrorResponse(BaseModel):
    """Error response model."""
    
    error_code: str
    message: str
    details: Optional[dict] = None


# Router

router = APIRouter(
    prefix="/v2/iso-markets",
    tags=["iso-markets-v2"],
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# Dependency injection
async def get_iso_service() -> IsoApplicationService:
    """Get ISO application service.
    
    Wire this up with your dependency injection container.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Service dependency injection not yet configured."
    )


# Endpoints

@router.post(
    "/",
    response_model=IsoMarketResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create ISO market",
    description="Create a new ISO market region.",
)
async def create_iso_market(
    request: CreateIsoMarketRequest,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Create a new ISO market."""
    
    command = CreateIsoMarketCommand(
        tenant_id=request.tenant_id,
        iso_code=request.iso_code,
        iso_name=request.iso_name,
        timezone=request.timezone,
    )
    
    result = await service.create_iso_market(command)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error_code": result.error, "message": result.message, "details": result.details}
        )


@router.get(
    "/{iso_market_id}",
    response_model=IsoMarketResponse,
    summary="Get ISO market",
    description="Retrieve an ISO market by ID.",
)
async def get_iso_market(
    iso_market_id: str,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Get an ISO market by ID."""
    
    result = await service.get_iso_market(iso_market_id)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        if result.error == "NOT_FOUND":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error_code": result.error, "message": result.message}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error_code": result.error, "message": result.message}
            )


@router.post(
    "/{iso_market_id}/lmp",
    response_model=IsoMarketResponse,
    summary="Add LMP data",
    description="Add locational marginal pricing data to an ISO market.",
)
async def add_lmp_data(
    iso_market_id: str,
    request: AddLMPDataRequest,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Add LMP data to an ISO market."""
    
    command = AddLMPDataCommand(
        iso_market_id=iso_market_id,
        node_id=request.node_id,
        energy_price=request.energy_price,
        congestion_price=request.congestion_price,
        loss_price=request.loss_price,
        timestamp=request.timestamp,
        market_type=request.market_type,
        location_zone=request.location_zone,
        location_node=request.location_node,
    )
    
    result = await service.add_lmp_data(command)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        if result.error == "NOT_FOUND":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error_code": result.error, "message": result.message}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error_code": result.error, "message": result.message, "details": result.details}
            )


@router.post(
    "/{iso_market_id}/load",
    response_model=IsoMarketResponse,
    summary="Add load data",
    description="Add system load data to an ISO market.",
)
async def add_load_data(
    iso_market_id: str,
    request: AddLoadDataRequest,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Add load data to an ISO market."""
    
    command = AddLoadDataCommand(
        iso_market_id=iso_market_id,
        zone_id=request.zone_id,
        load_mw=request.load_mw,
        timestamp=request.timestamp,
        forecast=request.forecast,
    )
    
    result = await service.add_load_data(command)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        if result.error == "NOT_FOUND":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error_code": result.error, "message": result.message}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error_code": result.error, "message": result.message, "details": result.details}
            )


@router.post(
    "/{iso_market_id}/generation-mix",
    response_model=IsoMarketResponse,
    summary="Add generation mix data",
    description="Add generation mix by fuel type to an ISO market.",
)
async def add_generation_mix(
    iso_market_id: str,
    request: AddGenerationMixRequest,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Add generation mix data to an ISO market."""
    
    command = AddGenerationMixCommand(
        iso_market_id=iso_market_id,
        zone_id=request.zone_id,
        fuel_type=request.fuel_type,
        generation_mw=request.generation_mw,
        percentage=request.percentage,
        timestamp=request.timestamp,
    )
    
    result = await service.add_generation_mix(command)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        if result.error == "NOT_FOUND":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error_code": result.error, "message": result.message}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error_code": result.error, "message": result.message, "details": result.details}
            )


@router.post(
    "/{iso_market_id}/deactivate",
    response_model=IsoMarketResponse,
    summary="Deactivate ISO market",
    description="Deactivate an ISO market (soft delete).",
)
async def deactivate_iso_market(
    iso_market_id: str,
    service: IsoApplicationService = Depends(get_iso_service),
) -> IsoMarketResponse:
    """Deactivate an ISO market."""
    
    result = await service.deactivate_iso_market(iso_market_id)
    
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        if result.error == "NOT_FOUND":
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error_code": result.error, "message": result.message}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={"error_code": result.error, "message": result.message, "details": result.details}
            )


# Helper functions

def _dto_to_response(dto: IsoMarketDTO) -> IsoMarketResponse:
    """Convert IsoMarketDTO to API response model."""
    return IsoMarketResponse(
        id=dto.id,
        tenant_id=dto.tenant_id,
        iso_code=dto.iso_code,
        iso_name=dto.iso_name,
        timezone=dto.timezone,
        active=dto.active,
        created_at=dto.created_at,
        updated_at=dto.updated_at,
    )

