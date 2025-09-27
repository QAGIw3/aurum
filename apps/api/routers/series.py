"""Series router with ETag support and clean repository pattern."""
from __future__ import annotations

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from libs.core import CurveKey, PriceObservation
from libs.storage import TimescaleSeriesRepo
from ..main import get_timescale_repo

router = APIRouter()


@router.get("/observations")
async def get_observations(
    request: Request,
    response: Response,
    iso: str,
    market: str,
    location: str,
    start_date: date,
    end_date: date,
    product: Optional[str] = None,
    limit: Optional[int] = 1000,
    repo: TimescaleSeriesRepo = Depends(get_timescale_repo),
) -> List[dict]:
    """Get price observations for a curve with ETag support."""
    
    # Create curve key
    curve = CurveKey(
        iso=iso,
        market=market,
        location=location,
        product=product,
    )
    
    try:
        # Get observations from repository
        observations = await repo.get_observations(
            curve=curve,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
        
        # Convert to dict format for response
        result = [obs.model_dump() for obs in observations]
        
        # Generate ETag for caching
        import hashlib
        import json
        
        etag_content = f"{curve.model_dump_json()}-{start_date}-{end_date}-{len(result)}"
        etag = hashlib.md5(etag_content.encode()).hexdigest()
        
        # Check If-None-Match header for 304 response
        if request.headers.get("if-none-match") == f'"{etag}"':
            return Response(status_code=304)
        
        # Set ETag header
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = "max-age=300"  # 5 minute cache
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retrieve observations: {str(e)}")


@router.get("/curves")
async def list_curves(
    request: Request,
    response: Response,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    limit: Optional[int] = 100,
    offset: Optional[int] = 0,
    repo: TimescaleSeriesRepo = Depends(get_timescale_repo),
) -> dict:
    """List available curves with pagination and ETag support."""
    
    try:
        curves, total_count = await repo.list_curves(
            iso=iso,
            market=market,
            limit=limit,
            offset=offset,
        )
        
        # Convert curves to dict format
        curve_data = [curve.model_dump() for curve in curves]
        
        result = {
            "curves": curve_data,
            "pagination": {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "count": len(curve_data),
            }
        }
        
        # Generate ETag
        import hashlib
        etag_content = f"{iso}-{market}-{limit}-{offset}-{total_count}"
        etag = hashlib.md5(etag_content.encode()).hexdigest()
        
        if request.headers.get("if-none-match") == f'"{etag}"':
            return Response(status_code=304)
        
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = "max-age=3600"  # 1 hour cache for list
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list curves: {str(e)}")


@router.get("/curves/{iso}/{market}/{location}/metadata")
async def get_curve_metadata(
    request: Request,
    response: Response,
    iso: str,
    market: str,
    location: str,
    product: Optional[str] = None,
    repo: TimescaleSeriesRepo = Depends(get_timescale_repo),
) -> dict:
    """Get metadata for a specific curve."""
    
    curve = CurveKey(
        iso=iso,
        market=market,
        location=location,
        product=product,
    )
    
    try:
        metadata = await repo.get_curve_metadata(curve)
        
        if not metadata:
            raise HTTPException(status_code=404, detail="Curve not found")
        
        # Generate ETag
        import hashlib
        etag_content = f"{curve.model_dump_json()}-metadata"
        etag = hashlib.md5(etag_content.encode()).hexdigest()
        
        if request.headers.get("if-none-match") == f'"{etag}"':
            return Response(status_code=304)
        
        response.headers["ETag"] = f'"{etag}"'
        response.headers["Cache-Control"] = "max-age=7200"  # 2 hour cache for metadata
        
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get curve metadata: {str(e)}")