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
from .pagination import (
    resolve_pagination,
    build_next_cursor,
    build_pagination_envelope,
)
from ...telemetry.context import get_request_id

router = APIRouter(prefix="/v2", tags=["iso"])


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
        # Resolve pagination (no additional filters beyond iso)
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"iso": iso},
        )

        # Get LMP service
        from ..iso_v2_service import get_iso_service
        svc = await get_iso_service()
        paginated_data = await svc.lmp_last_24h(iso=iso, offset=offset, limit=effective_limit)

        # Create cursors and envelope
        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"iso": iso},
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"iso": iso})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

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
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "iso": iso,
            "returned_count": len(lmp_points),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = IsoLmpResponse(
            data=lmp_points,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

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
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"iso": iso, "date": date},
        )

        from ..iso_v2_service import get_iso_service
        svc = await get_iso_service()
        paginated_data = await svc.lmp_hourly(iso=iso, date=date, offset=offset, limit=effective_limit)

        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"iso": iso, "date": date},
        )
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"iso": iso, "date": date})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total_count,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "iso": iso,
            "date": date,
            "returned_count": len(paginated_data),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = IsoLmpAggregateResponse(
            data=paginated_data,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

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
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"iso": iso, "date": date},
        )

        from ..iso_v2_service import get_iso_service
        svc = await get_iso_service()
        paginated_data = await svc.lmp_daily(iso=iso, date=date, offset=offset, limit=effective_limit)

        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"iso": iso, "date": date},
        )
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"iso": iso, "date": date})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "iso": iso,
            "date": date,
            "returned_count": len(paginated_data),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = IsoLmpAggregateResponse(
            data=paginated_data,
            meta=meta_out,
            links=links,
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, next_cursor=next_cursor, canonical_url=str(request.url.remove_query_params("cursor")))

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
