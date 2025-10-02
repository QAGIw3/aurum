"""V2 Curves API routes using modern patterns.

Demonstrates standardized router pattern with dependency injection,
service layer integration, and consistent response formats.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Annotated, Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status
    from fastapi.responses import StreamingResponse
except ImportError:
    APIRouter = None  # type: ignore
    Depends = None  # type: ignore
    HTTPException = None  # type: ignore
    Query = None  # type: ignore
    status = None  # type: ignore
    StreamingResponse = None  # type: ignore

from aurum.services.core import CurveService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.core.container import get_service

logger = logging.getLogger(__name__)


def get_curve_service() -> CurveService:
    """Dependency provider for CurveService."""
    import asyncio
    return asyncio.run(get_service(CurveService))


# Create router
router = APIRouter(prefix="/v2/curves", tags=["curves-v2"])


@router.get("/")
async def list_curves(
    iso: Optional[str] = Query(None, description="ISO/RTO identifier"),
    market: Optional[str] = Query(None, description="Market type (DA, RT)"),
    location: Optional[str] = Query(None, description="Location/node identifier"),
    product: Optional[str] = Query(None, description="Product type"),
    asof: Optional[date] = Query(None, description="As-of date"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    use_cache: bool = Query(True, description="Use caching for performance"),
    service: CurveService = Depends(get_curve_service)
) -> Dict[str, Any]:
    """List market curves with filters and optional caching.
    
    Returns curve data points filtered by ISO, market, location, and other dimensions.
    Results are cached by default for better performance on repeated queries.
    
    Example:
        GET /v2/curves?iso=PJM&market=DA&limit=100
    """
    try:
        context = ServiceContext()  # Would extract from request headers
        
        result = await service.get_curves(
            iso=iso,
            market=market,
            location=location,
            product=product,
            asof=asof,
            limit=limit,
            offset=offset,
            use_cache=use_cache,
            context=context
        )
        
        if not result.success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.error or "Failed to retrieve curves"
            )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error in list_curves: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get("/{curve_key}")
async def get_curve(
    curve_key: str,
    asof: Optional[date] = Query(None, description="As-of date"),
    service: CurveService = Depends(get_curve_service)
) -> Dict[str, Any]:
    """Get specific curve by key.
    
    Example:
        GET /v2/curves/PJM_DA_WESTERN_HUB_LMP
    """
    try:
        context = ServiceContext()
        
        result = await service.get_curve_by_key(
            curve_key=curve_key,
            asof=asof,
            context=context
        )
        
        if not result.success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.error
            )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except NotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except ValidationError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Error in get_curve: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )


@router.get("/export")
async def export_curves(
    iso: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    asof: Optional[date] = Query(None),
    format: str = Query("json", regex="^(json|csv)$"),
    service: CurveService = Depends(get_curve_service)
):
    """Export curves as streaming response.
    
    Streams curves to avoid loading large datasets into memory.
    
    Example:
        GET /v2/curves/export?iso=PJM&market=DA&format=json
    """
    try:
        context = ServiceContext()
        
        async def generate_json():
            """Generate JSON lines for streaming."""
            yield "[\n"
            first = True
            
            async for curve in service.export_curves(
                iso=iso,
                market=market,
                location=location,
                asof=asof,
                context=context
            ):
                if not first:
                    yield ",\n"
                import json
                yield json.dumps(curve)
                first = False
            
            yield "\n]"
        
        return StreamingResponse(
            generate_json(),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=curves_export.json"}
        )
        
    except Exception as e:
        logger.error(f"Error in export_curves: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Export failed"
        )


@router.post("/cache/invalidate")
async def invalidate_cache(
    iso: Optional[str] = None,
    market: Optional[str] = None,
    service: CurveService = Depends(get_curve_service)
) -> Dict[str, Any]:
    """Invalidate curve cache.
    
    Use this endpoint to clear cached curve data after updates.
    
    Example:
        POST /v2/curves/cache/invalidate?iso=PJM&market=DA
    """
    try:
        context = ServiceContext()
        
        result = await service.invalidate_curve_cache(
            iso=iso,
            market=market,
            context=context
        )
        
        return {
            "success": True,
            "data": result.data,
            "metadata": result.metadata
        }
        
    except Exception as e:
        logger.error(f"Error invalidating cache: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache invalidation failed"
        )


__all__ = ["router"]

