"""v2 Drought API with enhanced features.

This module provides the v2 implementation of the drought API with:
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


class DroughtIndexPoint(BaseModel):
    """Response for drought index point data with v2 enhancements."""
    series_id: str = Field(..., description="Series identifier")
    dataset: str = Field(..., description="Dataset")
    index: str = Field(..., description="Index name")
    timescale: str = Field(..., description="Timescale")
    valid_date: str = Field(..., description="Valid date")
    as_of: Optional[str] = Field(None, description="As-of date")
    value: Optional[float] = Field(None, description="Index value")
    unit: Optional[str] = Field(None, description="Unit")
    poc: Optional[str] = Field(None, description="Point of contact")
    region_type: str = Field(..., description="Region type")
    region_id: str = Field(..., description="Region ID")
    region_name: Optional[str] = Field(None, description="Region name")
    parent_region_id: Optional[str] = Field(None, description="Parent region ID")
    source_url: Optional[str] = Field(None, description="Source URL")
    metadata: Optional[dict] = Field(None, description="Additional metadata")


class DroughtIndexResponse(BaseModel):
    """Response for drought index data with v2 enhancements."""
    data: List[DroughtIndexPoint] = Field(..., description="List of index points")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class DroughtUsdmResponse(BaseModel):
    """Response for USDM drought data with v2 enhancements."""
    data: List[dict] = Field(..., description="List of USDM data")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/drought/indices", response_model=DroughtIndexResponse)
async def get_drought_indices_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    dataset: Optional[str] = Query(None, description="Source dataset filter"),
    index: Optional[str] = Query(None, description="Index name filter"),
    timescale: Optional[str] = Query(None, description="Timescale filter"),
    region: Optional[str] = Query(None, description="Region filter"),
    region_type: Optional[str] = Query(None, description="Region type filter"),
    region_id: Optional[str] = Query(None, description="Region ID filter"),
    start: Optional[str] = Query(None, description="Start date filter"),
    end: Optional[str] = Query(None, description="End date filter"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> DroughtIndexResponse:
    """Get drought indices with enhanced pagination and error handling."""
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
                        "instance": "/v2/drought/indices"
                    }
                )

        # Get drought indices service (placeholder - would integrate with actual service)
        from ..routes import _drought_indices_data
        indices_data = _drought_indices_data(
            dataset=dataset,
            index=index,
            timescale=timescale,
            region=region,
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end
        )

        # Apply pagination
        total_count = len(indices_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = indices_data[start_idx:end_idx]

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
        index_points = []
        for point_data in paginated_data:
            index_points.append(DroughtIndexPoint(
                series_id=point_data["series_id"],
                dataset=point_data["dataset"],
                index=point_data["index"],
                timescale=point_data["timescale"],
                valid_date=point_data["valid_date"],
                as_of=point_data.get("as_of"),
                value=point_data.get("value"),
                unit=point_data.get("unit"),
                poc=point_data.get("poc"),
                region_type=point_data["region_type"],
                region_id=point_data["region_id"],
                region_name=point_data.get("region_name"),
                parent_region_id=point_data.get("parent_region_id"),
                source_url=point_data.get("source_url"),
                metadata=point_data.get("metadata"),
            ))

        # Create response with enhanced metadata
        result = DroughtIndexResponse(
            data=index_points,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "filters": {
                    "dataset": dataset,
                    "index": index,
                    "timescale": timescale,
                    "region": region,
                    "region_type": region_type,
                    "region_id": region_id,
                    "start": start,
                    "end": end,
                },
                "total_count": total_count,
                "returned_count": len(index_points),
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
                "detail": f"Failed to get drought indices: {str(exc)}",
                "instance": "/v2/drought/indices",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/drought/usdm", response_model=DroughtUsdmResponse)
async def get_drought_usdm_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    region: Optional[str] = Query(None, description="Region filter"),
    region_type: Optional[str] = Query(None, description="Region type filter"),
    region_id: Optional[str] = Query(None, description="Region ID filter"),
    start: Optional[str] = Query(None, description="Start date filter"),
    end: Optional[str] = Query(None, description="End date filter"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> DroughtUsdmResponse:
    """Get USDM drought data with enhanced pagination and error handling."""
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
                        "instance": "/v2/drought/usdm"
                    }
                )

        # Get USDM service (placeholder - would integrate with actual service)
        from ..routes import _drought_usdm_data
        usdm_data = _drought_usdm_data(
            region=region,
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end
        )

        # Apply pagination
        total_count = len(usdm_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = usdm_data[start_idx:end_idx]

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
        result = DroughtUsdmResponse(
            data=paginated_data,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "filters": {
                    "region": region,
                    "region_type": region_type,
                    "region_id": region_id,
                    "start": start,
                    "end": end,
                },
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
                "detail": f"Failed to get USDM data: {str(exc)}",
                "instance": "/v2/drought/usdm",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
