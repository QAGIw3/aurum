"""Curve API routes using new service layer.

Example of integrating the new service architecture with FastAPI.
"""

from typing import Optional, List, Dict, Any
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from aurum.services.core import CurveService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError, ServiceError
from aurum.data.repositories import CurveRepository


# Response models
class CurveResponse(BaseModel):
    """Curve data point response."""
    curve_key: str
    interval_start: str
    value: float
    # Add other fields as needed


class CurvesListResponse(BaseModel):
    """Response for list of curves."""
    data: List[Dict[str, Any]]
    metadata: Dict[str, Any] = Field(default_factory=dict)


class LatestAsofResponse(BaseModel):
    """Response for latest as-of date."""
    latest_asof: Optional[str]
    iso: Optional[str]


# Router setup
router = APIRouter(prefix="/v2/curves", tags=["curves"])


# Dependency injection
async def get_curve_service() -> CurveService:
    """Provide curve service with dependencies.
    
    This creates the service with all required dependencies.
    In production, use a DI container for better management.
    """
    # Create repository
    repo = CurveRepository()
    await repo.initialize()
    
    try:
        # Create and yield service
        service = CurveService(repo)
        yield service
    finally:
        # Cleanup
        await repo.close()


async def get_service_context() -> ServiceContext:
    """Extract service context from request.
    
    In production, extract from:
    - Auth headers (tenant_id, user_id)
    - Request headers (request_id, correlation_id)
    - JWT claims
    """
    # Placeholder implementation
    return ServiceContext(
        tenant_id="default",  # Extract from auth
        user_id="anonymous",  # Extract from JWT
        request_id=None  # Will auto-generate
    )


# Routes

@router.get("/", response_model=CurvesListResponse)
async def list_curves(
    iso: Optional[str] = Query(None, description="ISO/RTO identifier"),
    market: Optional[str] = Query(None, description="Market type (DA, RT)"),
    location: Optional[str] = Query(None, description="Location/node identifier"),
    product: Optional[str] = Query(None, description="Product type"),
    asof: Optional[date] = Query(None, description="As-of date (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    service: CurveService = Depends(get_curve_service),
    context: ServiceContext = Depends(get_service_context)
):
    """Get curves with filters.
    
    Query curves based on ISO, market, location, product, and as-of date.
    Results are paginated and can be filtered by multiple criteria.
    """
    try:
        result = await service.get_curves(
            iso=iso,
            market=market,
            location=location,
            product=product,
            asof=asof,
            limit=limit,
            offset=offset,
            context=context
        )
        
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        
        return CurvesListResponse(
            data=result.data,
            metadata=result.metadata
        )
        
    except ValidationError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{curve_key}", response_model=CurvesListResponse)
async def get_curve(
    curve_key: str,
    asof: Optional[date] = Query(None, description="As-of date (YYYY-MM-DD)"),
    service: CurveService = Depends(get_curve_service),
    context: ServiceContext = Depends(get_service_context)
):
    """Get specific curve by key.
    
    Retrieve all data points for a specific curve identifier.
    """
    try:
        result = await service.get_curve_by_key(
            curve_key=curve_key,
            asof=asof,
            context=context
        )
        
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        
        return CurvesListResponse(
            data=result.data,
            metadata=result.metadata
        )
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/meta/latest-asof", response_model=LatestAsofResponse)
async def get_latest_asof(
    iso: Optional[str] = Query(None, description="Filter by ISO"),
    service: CurveService = Depends(get_curve_service),
    context: ServiceContext = Depends(get_service_context)
):
    """Get the latest as-of date for curves.
    
    Returns the most recent date for which curve data is available.
    Useful for determining data freshness.
    """
    try:
        result = await service.get_latest_asof(iso=iso, context=context)
        
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        
        return LatestAsofResponse(
            latest_asof=result.data.isoformat() if result.data else None,
            iso=iso
        )
        
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compare/{curve_key_1}/{curve_key_2}")
async def compare_curves(
    curve_key_1: str,
    curve_key_2: str,
    asof: Optional[date] = Query(None, description="As-of date for comparison"),
    service: CurveService = Depends(get_curve_service),
    context: ServiceContext = Depends(get_service_context)
):
    """Compare two curves.
    
    Provides analytics comparing two curve series.
    """
    try:
        result = await service.compare_curves(
            curve_key_1=curve_key_1,
            curve_key_2=curve_key_2,
            asof=asof,
            context=context
        )
        
        if not result.success:
            raise HTTPException(status_code=500, detail=result.error)
        
        return {
            "comparison": result.data,
            "metadata": result.metadata
        }
        
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ServiceError as e:
        raise HTTPException(status_code=500, detail=str(e))

