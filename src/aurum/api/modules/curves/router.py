"""Enhanced async curves API router with performance optimizations."""

from __future__ import annotations

import asyncio
from datetime import date
from typing import List, Optional

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
    from fastapi.responses import StreamingResponse
except ImportError:
    # Graceful fallback for environments without FastAPI
    APIRouter = None
    Depends = None
    HTTPException = None
    Query = None
    Request = None
    Response = None
    StreamingResponse = None

from ....core import get_current_context, request_context, get_service
from ...exceptions import ValidationException, NotFoundException, ServiceUnavailableException
from ...models import CurveResponse, CurveDiffResponse, Meta
from ...responses import create_etag_response, create_csv_response
from .service import AsyncCurveService, CurveQuery

# Only create router if FastAPI is available
if APIRouter is not None:
    router = APIRouter(prefix="/v1/curves", tags=["Curves"])
    
    def get_curve_service() -> AsyncCurveService:
        """Dependency injection for curve service."""
        return get_service(AsyncCurveService)
    
    @router.get("/", response_model=CurveResponse)
    async def get_curves(
        request: Request,
        response: Response,
        iso: str = Query(..., description="ISO code (e.g., PJM, CAISO)"),
        market: Optional[str] = Query(None, description="Market type"),
        location: Optional[str] = Query(None, description="Location/hub name"),
        commodity: Optional[str] = Query(None, description="Commodity type"),
        start_date: Optional[date] = Query(None, description="Start date (YYYY-MM-DD)"),
        end_date: Optional[date] = Query(None, description="End date (YYYY-MM-DD)"),
        limit: Optional[int] = Query(None, ge=1, le=10000, description="Maximum number of records"),
        format: Optional[str] = Query("json", description="Response format (json, csv)"),
        use_cache: bool = Query(True, description="Use caching"),
        warm_cache: bool = Query(False, description="Warm related cache entries"),
        curve_service: AsyncCurveService = Depends(get_curve_service)
    ) -> CurveResponse:
        """Get curve data with intelligent caching and performance optimization."""
        
        try:
            # Set request context
            async with request_context(
                request_id=request.headers.get("X-Request-ID"),
                tenant_id=request.headers.get("X-Tenant-ID"),
                user_id=request.headers.get("X-User-ID"),
                operation="fetch_curves"
            ):
                # Fetch curves
                curves, meta = await curve_service.fetch_curves(
                    iso=iso,
                    market=market,
                    location=location,
                    commodity=commodity,
                    start_date=start_date,
                    end_date=end_date,
                    limit=limit,
                    use_cache=use_cache,
                    warm_cache=warm_cache
                )
                
                # Create response
                curve_response = CurveResponse(data=curves, meta=meta)
                
                # Handle different response formats
                if format.lower() == "csv":
                    return create_csv_response(curves, "curves.csv")
                
                # Add ETag for caching
                return create_etag_response(curve_response, response)
        
        except ValidationException as e:
            raise HTTPException(status_code=400, detail=str(e))
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except ServiceUnavailableException as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            context = get_current_context()
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error (request_id: {context.request_id})"
            )
    
    @router.get("/diff", response_model=CurveDiffResponse)
    async def get_curve_diff(
        request: Request,
        response: Response,
        iso: str = Query(..., description="ISO code"),
        market: str = Query(..., description="Market type"),
        location: str = Query(..., description="Location/hub name"),
        base_date: date = Query(..., description="Base date for comparison"),
        compare_date: date = Query(..., description="Compare date"),
        format: Optional[str] = Query("json", description="Response format (json, csv)"),
        use_cache: bool = Query(True, description="Use caching"),
        curve_service: AsyncCurveService = Depends(get_curve_service)
    ) -> CurveDiffResponse:
        """Get curve differences between two dates."""
        
        try:
            async with request_context(
                request_id=request.headers.get("X-Request-ID"),
                tenant_id=request.headers.get("X-Tenant-ID"),
                user_id=request.headers.get("X-User-ID"),
                operation="fetch_curve_diff"
            ):
                # Fetch curve differences
                diff_points, meta = await curve_service.fetch_curve_diff(
                    iso=iso,
                    market=market,
                    location=location,
                    base_date=base_date,
                    compare_date=compare_date,
                    use_cache=use_cache
                )
                
                # Create response
                diff_response = CurveDiffResponse(data=diff_points, meta=meta)
                
                # Handle different response formats
                if format.lower() == "csv":
                    return create_csv_response(diff_points, "curve_diff.csv")
                
                return create_etag_response(diff_response, response)
        
        except ValidationException as e:
            raise HTTPException(status_code=400, detail=str(e))
        except NotFoundException as e:
            raise HTTPException(status_code=404, detail=str(e))
        except ServiceUnavailableException as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            context = get_current_context()
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error (request_id: {context.request_id})"
            )
    
    @router.post("/batch")
    async def get_curves_batch(
        request: Request,
        response: Response,
        queries: List[CurveQuery],
        use_cache: bool = Query(True, description="Use caching"),
        curve_service: AsyncCurveService = Depends(get_curve_service)
    ) -> List[CurveResponse]:
        """Batch fetch multiple curve queries for efficiency."""
        
        try:
            async with request_context(
                request_id=request.headers.get("X-Request-ID"),
                tenant_id=request.headers.get("X-Tenant-ID"),
                user_id=request.headers.get("X-User-ID"),
                operation="batch_fetch_curves"
            ):
                # Validate batch size
                if len(queries) > 50:
                    raise ValidationException("Batch size cannot exceed 50 queries")
                
                # Batch fetch curves
                results = await curve_service.batch_fetch_curves(queries, use_cache=use_cache)
                
                # Create responses
                responses = []
                for curves, meta in results:
                    responses.append(CurveResponse(data=curves, meta=meta))
                
                return responses
        
        except ValidationException as e:
            raise HTTPException(status_code=400, detail=str(e))
        except ServiceUnavailableException as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            context = get_current_context()
            raise HTTPException(
                status_code=500,
                detail=f"Internal server error (request_id: {context.request_id})"
            )
    
    @router.get("/stream")
    async def stream_curves(
        request: Request,
        iso: str = Query(..., description="ISO code"),
        market: Optional[str] = Query(None, description="Market type"),
        location: Optional[str] = Query(None, description="Location/hub name"),
        start_date: Optional[date] = Query(None, description="Start date"),
        end_date: Optional[date] = Query(None, description="End date"),
        curve_service: AsyncCurveService = Depends(get_curve_service)
    ) -> StreamingResponse:
        """Stream large curve datasets to handle memory efficiently."""
        
        async def generate_curve_stream():
            """Generate streaming curve data."""
            try:
                async with request_context(
                    request_id=request.headers.get("X-Request-ID"),
                    tenant_id=request.headers.get("X-Tenant-ID"),
                    user_id=request.headers.get("X-User-ID"),
                    operation="stream_curves"
                ):
                    # For large datasets, we'd implement chunked streaming
                    # This is a simplified example
                    curves, meta = await curve_service.fetch_curves(
                        iso=iso,
                        market=market,
                        location=location,
                        start_date=start_date,
                        end_date=end_date,
                        use_cache=True
                    )
                    
                    # Stream as JSONL (JSON Lines)
                    yield f'{{"meta": {meta.model_dump_json()}}}\n'
                    
                    for curve in curves:
                        yield f'{curve.model_dump_json()}\n'
            
            except Exception as e:
                context = get_current_context()
                error_line = {
                    "error": str(e),
                    "request_id": context.request_id
                }
                yield f'{error_line}\n'
        
        return StreamingResponse(
            generate_curve_stream(),
            media_type="application/x-ndjson",
            headers={"Content-Disposition": f"attachment; filename=curves_{iso}.jsonl"}
        )
    
    @router.delete("/cache")
    async def invalidate_curve_cache(
        request: Request,
        iso: Optional[str] = Query(None, description="ISO to invalidate (all if not specified)"),
        market: Optional[str] = Query(None, description="Market to invalidate"),
        curve_service: AsyncCurveService = Depends(get_curve_service)
    ) -> dict:
        """Invalidate curve cache entries."""
        
        try:
            async with request_context(
                request_id=request.headers.get("X-Request-ID"),
                tenant_id=request.headers.get("X-Tenant-ID"),
                user_id=request.headers.get("X-User-ID"),
                operation="invalidate_curve_cache"
            ):
                invalidated_count = await curve_service.invalidate_cache(iso=iso, market=market)
                
                return {
                    "message": "Cache invalidated successfully",
                    "invalidated_entries": invalidated_count,
                    "iso": iso,
                    "market": market
                }
        
        except Exception as e:
            context = get_current_context()
            raise HTTPException(
                status_code=500,
                detail=f"Cache invalidation failed (request_id: {context.request_id})"
            )

else:
    # Fallback when FastAPI is not available
    router = None