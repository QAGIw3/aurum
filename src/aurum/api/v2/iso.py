"""v2 ISO API with enhanced features.

This module provides the v2 implementation of the ISO API with:
- Cursor-only pagination (offset deprecated)
- Consistent error shapes using RFC 7807
- Enhanced ETag support
- Improved validation and error handling
- Better observability
- Link headers for navigation
- Tenant context enforcement

Notes:
- Base path: `/v2/*` (see app wiring in src/aurum/api/app.py)
- Migration guidance from v1 endpoints: docs/migration-guide.md
"""

from __future__ import annotations

import time
from typing import List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag
from ...telemetry.context import get_request_id

router = APIRouter()


class IsoLmpPoint(BaseModel):
    """Response for ISO LMP point data with v2 enhancements."""
    timestamp: str = Field(..., description="Timestamp")
    location_id: str = Field(..., description="Location identifier")
    value: float = Field(..., description="LMP value")
    unit: str = Field(..., description="Unit of measurement")
    meta: dict = Field(..., description="Metadata")


class IsoLmpResponse(BaseModel):
    """Response for ISO LMP data with v2 enhancements."""
    data: List[IsoLmpPoint] = Field(..., description="List of LMP points")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class IsoLmpAggregateResponse(BaseModel):
    """Response for ISO LMP aggregate data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of aggregate LMP data")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/iso/lmp/last-24h", response_model=IsoLmpResponse)
async def get_lmp_last_24h_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    iso: str = Query(..., description="ISO identifier"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> IsoLmpResponse:
    """Get last 24h LMP data with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
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
                        "instance": "/v2/iso/lmp/last-24h"
                    }
                )

        # Get LMP service (placeholder - would integrate with actual service)
        from ..routes import _lmp_last_24h_data
        lmp_data = _lmp_last_24h_data(iso)

        # Apply pagination
        total_count = len(lmp_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = lmp_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        lmp_points = []
        for point_data in paginated_data:
            lmp_points.append(IsoLmpPoint(
                timestamp=point_data["timestamp"],
                location_id=point_data["location_id"],
                value=point_data["value"],
                unit=point_data.get("unit", "USD/MWh"),
                meta={"tenant_id": tenant_id, "iso": iso}
            ))

        # Create response with enhanced metadata
        result = IsoLmpResponse(
            data=lmp_points,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "iso": iso,
                "total_count": total_count,
                "returned_count": len(lmp_points),
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

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to get LMP last 24h: {str(exc)}",
                "instance": "/v2/iso/lmp/last-24h",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/iso/lmp/hourly", response_model=IsoLmpAggregateResponse)
async def get_lmp_hourly_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    iso: str = Query(..., description="ISO identifier"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> IsoLmpAggregateResponse:
    """Get hourly LMP aggregate data with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
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
                        "instance": "/v2/iso/lmp/hourly"
                    }
                )

        # Get hourly LMP service (placeholder - would integrate with actual service)
        from ..routes import _lmp_hourly_data
        hourly_data = _lmp_hourly_data(iso, date)

        # Apply pagination
        total_count = len(hourly_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = hourly_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        result = IsoLmpAggregateResponse(
            data=paginated_data,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "iso": iso,
                "date": date,
                "total_count": total_count,
                "returned_count": len(paginated_data),
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

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to get LMP hourly: {str(exc)}",
                "instance": "/v2/iso/lmp/hourly",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/iso/lmp/daily", response_model=IsoLmpAggregateResponse)
async def get_lmp_daily_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    iso: str = Query(..., description="ISO identifier"),
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> IsoLmpAggregateResponse:
    """Get daily LMP aggregate data with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
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
                        "instance": "/v2/iso/lmp/daily"
                    }
                )

        # Get daily LMP service (placeholder - would integrate with actual service)
        from ..routes import _lmp_daily_data
        daily_data = _lmp_daily_data(iso, date)

        # Apply pagination
        total_count = len(daily_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = daily_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        result = IsoLmpAggregateResponse(
            data=paginated_data,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "iso": iso,
                "date": date,
                "total_count": total_count,
                "returned_count": len(paginated_data),
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

        # Add ETag for caching with Link headers
        return respond_with_etag(
            result,
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=str(request.url)
        )

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to get LMP daily: {str(exc)}",
                "instance": "/v2/iso/lmp/daily",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
