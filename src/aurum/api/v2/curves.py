"""v2 Curves API with enhanced features.

This module provides the v2 implementation of the curves API with:
- Cursor-only pagination (offset deprecated)
- Consistent error shapes using RFC 7807
- Enhanced ETag support
- Improved validation and error handling
- Better observability
"""

from __future__ import annotations

import time
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag, deprecation_warning_headers
from ..curves import get_curve_service
from ...telemetry.context import get_request_id

router = APIRouter()


class CurveResponse(BaseModel):
    """Response for curve data with v2 enhancements."""
    id: str = Field(..., description="Curve ID")
    name: str = Field(..., description="Curve name")
    description: Optional[str] = Field(None, description="Curve description")
    data_points: int = Field(..., description="Number of data points")
    created_at: str = Field(..., description="Creation timestamp")
    meta: dict = Field(..., description="Metadata")


class CurveListResponse(BaseModel):
    """Response for listing curves with v2 enhancements."""
    data: List[CurveResponse] = Field(..., description="List of curves")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/curves", response_model=CurveListResponse)
async def get_curves_v2(
    request: Request,
    response: Response,
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    name_filter: Optional[str] = Query(None, description="Filter by curve name"),
) -> CurveListResponse:
    """List curves with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        # Get curve service
        service = await get_curve_service()

        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/curves"
                    }
                )

        # List curves
        curves = await service.list_curves(
            offset=offset,
            limit=limit,
            name_filter=name_filter
        )

        # Create next cursor
        next_cursor = None
        if len(curves) == limit:
            next_offset = offset + limit
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        curve_responses = []
        for curve in curves:
            curve_responses.append(CurveResponse(
                **curve.model_dump(),
                meta={
                    "request_id": get_request_id(),
                    "processing_time_ms": round(duration_ms, 2),
                    "version": "v2"
                }
            ))

        # Create response with enhanced metadata
        result = CurveListResponse(
            data=curve_responses,
            meta={
                "request_id": get_request_id(),
                "total_count": len(curves) + (1 if next_cursor else 0),  # Estimate
                "returned_count": len(curves),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
        )

        # Add ETag for caching
        return respond_with_etag(result, request, response)

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list curves: {str(exc)}",
                "instance": "/v2/curves",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/curves/{curve_id}/diff", response_model=CurveResponse)
async def get_curve_diff_v2(
    request: Request,
    response: Response,
    curve_id: str,
    from_timestamp: str = Query(..., description="From timestamp"),
    to_timestamp: str = Query(..., description="To timestamp"),
) -> CurveResponse:
    """Get curve diff with enhanced error handling."""
    start_time = time.perf_counter()

    try:
        # Get curve service
        service = await get_curve_service()

        # Get curve diff
        curve_diff = await service.get_curve_diff(
            curve_id=curve_id,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = CurveResponse(
            **curve_diff.model_dump(),
            meta={
                "request_id": get_request_id(),
                "from_timestamp": from_timestamp,
                "to_timestamp": to_timestamp,
                "processing_time_ms": round(duration_ms, 2),
                "version": "v2"
            }
        )

        # Add ETag for caching
        return respond_with_etag(result, request, response)

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to get curve diff: {str(exc)}",
                "instance": f"/v2/curves/{curve_id}/diff",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
