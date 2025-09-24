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

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag
from .pagination import (
    resolve_pagination,
    build_next_cursor,
    build_pagination_envelope,
)
from ...telemetry.context import get_request_id

router = APIRouter(prefix="/v2", tags=["drought"])


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
        # Resolve pagination
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={
                "dataset": dataset,
                "index": index,
                "timescale": timescale,
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )

        from ..drought_v2_service import get_drought_service
        svc = await get_drought_service()
        paginated_data = await svc.list_indices(
            dataset=dataset,
            index=index,
            timescale=timescale,
            region=region,
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=effective_limit,
        )

        # Build cursor and metadata envelope
        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={
                "dataset": dataset,
                "index": index,
                "timescale": timescale,
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={
                "dataset": dataset,
                "index": index,
                "timescale": timescale,
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

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
        meta_out = dict(meta_page)
        meta_out.update({
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
            "returned_count": len(index_points),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = DroughtIndexResponse(
            data=index_points,
            meta=meta_out,
            links=links,
        )

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
                "detail": f"Failed to get drought indices: {str(exc)}",
                "instance": "/v2/drought/indices",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2),
            },
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
        # Resolve pagination
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )

        from ..drought_v2_service import get_drought_service
        svc = await get_drought_service()
        paginated_data = await svc.list_usdm(
            region_type=region_type,
            region_id=region_id,
            start=start,
            end=end,
            offset=offset,
            limit=effective_limit,
        )

        # Build cursor and metadata envelope
        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
        )
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "filters": {
                "region": region,
                "region_type": region_type,
                "region_id": region_id,
                "start": start,
                "end": end,
            },
            "returned_count": len(paginated_data),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = DroughtUsdmResponse(
            data=paginated_data,
            meta=meta_out,
            links=links,
        )

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
                "detail": f"Failed to get USDM data: {str(exc)}",
                "instance": "/v2/drought/usdm",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2),
            },
        )
