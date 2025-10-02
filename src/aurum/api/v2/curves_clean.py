"""V2 Curve API endpoints using clean architecture.

This module provides REST API endpoints for curve management using the new
clean architecture with domain models, application services, and proper
dependency injection.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from ...application.energy.curve_service import (
    CurveApplicationService,
    CreateCurveCommand,
    AddCurvePointCommand,
    UpdateCurvePointCommand,
    CurveDTO,
)


# Request/Response Models

class CurvePointRequest(BaseModel):
    """Request model for a curve point."""
    
    tenor: Decimal = Field(..., description="Tenor value (e.g., days, months)")
    value: Decimal = Field(..., description="Price or value at this tenor")


class CreateCurveRequest(BaseModel):
    """Request model for creating a curve."""
    
    tenant_id: str = Field(..., description="Tenant identifier")
    curve_key: str = Field(..., description="Unique curve identifier", min_length=1, max_length=255)
    as_of_date: datetime = Field(..., description="As-of date for the curve")
    points: List[CurvePointRequest] = Field(..., description="Curve points", min_items=1)
    currency: str = Field(default="USD", description="Currency code (ISO 4217)", min_length=3, max_length=3)
    tenor_type: Optional[str] = Field(default=None, description="Type of tenor (daily, monthly, etc.)")
    price_type: Optional[str] = Field(default=None, description="Type of price (forward, spot, etc.)")
    measure: str = Field(default="value", description="What this curve measures")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
                "curve_key": "PJM_DA_2025_Q1",
                "as_of_date": "2025-10-02T00:00:00Z",
                "points": [
                    {"tenor": "1", "value": "100.50"},
                    {"tenor": "2", "value": "105.75"},
                    {"tenor": "3", "value": "103.25"}
                ],
                "currency": "USD",
                "tenor_type": "monthly",
                "price_type": "forward",
                "measure": "price_per_mwh"
            }
        }


class AddCurvePointRequest(BaseModel):
    """Request model for adding a point to a curve."""
    
    tenor: Decimal = Field(..., description="Tenor value")
    value: Decimal = Field(..., description="Price or value")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tenor": "4",
                "value": "108.50"
            }
        }


class UpdateCurvePointRequest(BaseModel):
    """Request model for updating a curve point."""
    
    tenor: Decimal = Field(..., description="Tenor to update")
    new_value: Decimal = Field(..., description="New value")
    
    class Config:
        json_schema_extra = {
            "example": {
                "tenor": "2",
                "new_value": "110.00"
            }
        }


class CurvePointResponse(BaseModel):
    """Response model for a curve point."""
    
    tenor: str
    value: str
    timestamp: Optional[str]
    quality_flag: Optional[str]


class CurveResponse(BaseModel):
    """Response model for a curve."""
    
    id: str
    tenant_id: str
    curve_key: str
    as_of_date: datetime
    points: List[CurvePointResponse]
    measure: str
    metadata: dict
    
    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "tenant_id": "550e8400-e29b-41d4-a716-446655440000",
                "curve_key": "PJM_DA_2025_Q1",
                "as_of_date": "2025-10-02T00:00:00Z",
                "points": [
                    {"tenor": "1", "value": "100.50", "timestamp": None, "quality_flag": None},
                    {"tenor": "2", "value": "105.75", "timestamp": None, "quality_flag": None}
                ],
                "measure": "price_per_mwh",
                "metadata": {
                    "currency": "USD",
                    "tenor_type": "monthly",
                    "price_type": "forward",
                    "day_count": None,
                    "calendar": None,
                    "asset_class": None
                }
            }
        }


class ErrorResponse(BaseModel):
    """Error response model."""
    
    error_code: str
    message: str
    details: Optional[dict] = None


# Router

router = APIRouter(
    prefix="/v2/curves",
    tags=["curves-v2"],
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)


# Dependency injection
# TODO: Wire this up with your DI container
async def get_curve_service() -> CurveApplicationService:
    """Get curve application service.
    
    This should be wired up with your dependency injection container.
    For now, this is a placeholder that would need actual implementation.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Service dependency injection not yet configured. "
               "Wire up CurveApplicationService in your DI container."
    )


# Endpoints

@router.post(
    "/",
    response_model=CurveResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new curve",
    description="Create a new curve with the specified points and metadata.",
)
async def create_curve(
    request: CreateCurveRequest,
    service: CurveApplicationService = Depends(get_curve_service),
) -> CurveResponse:
    """Create a new curve."""
    
    # Convert request to command
    command = CreateCurveCommand(
        tenant_id=request.tenant_id,
        curve_key=request.curve_key,
        as_of_date=request.as_of_date,
        points=[(point.tenor, point.value) for point in request.points],
        currency=request.currency,
        tenor_type=request.tenor_type,
        price_type=request.price_type,
        measure=request.measure,
    )
    
    # Execute use case
    result = await service.create_curve(command)
    
    # Handle result
    if result.is_success():
        return _dto_to_response(result.value)
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error_code": result.error, "message": result.message, "details": result.details}
        )


@router.get(
    "/{curve_id}",
    response_model=CurveResponse,
    summary="Get curve by ID",
    description="Retrieve a curve by its unique identifier.",
)
async def get_curve(
    curve_id: str,
    service: CurveApplicationService = Depends(get_curve_service),
) -> CurveResponse:
    """Get a curve by ID."""
    
    result = await service.get_curve(curve_id)
    
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
    "/{curve_id}/points",
    response_model=CurveResponse,
    summary="Add point to curve",
    description="Add a new point to an existing curve.",
)
async def add_curve_point(
    curve_id: str,
    request: AddCurvePointRequest,
    service: CurveApplicationService = Depends(get_curve_service),
) -> CurveResponse:
    """Add a point to a curve."""
    
    command = AddCurvePointCommand(
        curve_id=curve_id,
        tenor=request.tenor,
        value=request.value,
    )
    
    result = await service.add_curve_point(command)
    
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


@router.patch(
    "/{curve_id}/points",
    response_model=CurveResponse,
    summary="Update curve point",
    description="Update the value of an existing point on a curve.",
)
async def update_curve_point(
    curve_id: str,
    request: UpdateCurvePointRequest,
    service: CurveApplicationService = Depends(get_curve_service),
) -> CurveResponse:
    """Update a curve point."""
    
    command = UpdateCurvePointCommand(
        curve_id=curve_id,
        tenor=request.tenor,
        new_value=request.new_value,
    )
    
    result = await service.update_curve_point(command)
    
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

def _dto_to_response(dto: CurveDTO) -> CurveResponse:
    """Convert CurveDTO to API response model."""
    return CurveResponse(
        id=dto.id,
        tenant_id=dto.tenant_id,
        curve_key=dto.curve_key,
        as_of_date=dto.as_of_date,
        points=[
            CurvePointResponse(**point)
            for point in dto.points
        ],
        measure=dto.measure,
        metadata=dto.metadata,
    )

