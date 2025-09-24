"""v2 EIA API with enhanced features.

This module provides the v2 implementation of the EIA API with:
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
from ..models import EiaSeriesDimensionsData, EiaSeriesDimensionsResponse, Meta

router = APIRouter(prefix="/v2", tags=["eia"])


class EiaDatasetResponse(BaseModel):
    """Response for EIA dataset data with v2 enhancements."""
    dataset_path: str = Field(..., description="Dataset path")
    title: str = Field(..., description="Dataset title")
    description: str = Field(..., description="Dataset description")
    last_updated: str = Field(..., description="Last updated timestamp")
    meta: dict = Field(..., description="Metadata")


class EiaDatasetsResponse(BaseModel):
    """Response for EIA datasets data with v2 enhancements."""
    data: List[EiaDatasetResponse] = Field(..., description="List of datasets")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class EiaSeriesPoint(BaseModel):
    """Response for EIA series point data with v2 enhancements."""
    series_id: str = Field(..., description="Series identifier")
    period: str = Field(..., description="Period")
    period_start: str = Field(..., description="Period start")
    value: float = Field(..., description="Value")
    unit: str = Field(..., description="Unit")
    meta: dict = Field(..., description="Metadata")


class EiaSeriesResponse(BaseModel):
    """Response for EIA series data with v2 enhancements."""
    data: List[EiaSeriesPoint] = Field(..., description="List of series points")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/metadata/eia/datasets", response_model=EiaDatasetsResponse)
async def list_eia_datasets_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> EiaDatasetsResponse:
    """List EIA datasets with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        # Resolve pagination using shared helper
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters=None,
        )

        from ..eia_v2_service import get_eia_service
        svc = await get_eia_service()
        paginated_data, total_count = await svc.list_datasets(offset=offset, limit=effective_limit)

        # Create cursors and pagination envelope
        has_more = (offset + effective_limit) < total_count
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters=None,
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters=None,
        )
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
        datasets = []
        for item in paginated_data:
            datasets.append(EiaDatasetResponse(
                dataset_path=item.dataset_path,
                title=item.title,
                description=item.description,
                last_updated=item.last_updated or "unknown",
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(datasets),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = EiaDatasetsResponse(
            data=datasets,
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
                "detail": f"Failed to list EIA datasets: {str(exc)}",
                "instance": "/v2/metadata/eia/datasets",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/ref/eia/series", response_model=EiaSeriesResponse)
async def get_eia_series_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    series_id: str = Query(..., description="EIA series identifier"),
    start_date: Optional[str] = Query(None, description="Start date filter"),
    end_date: Optional[str] = Query(None, description="End date filter"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
) -> EiaSeriesResponse:
    """Get EIA series data with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        # Resolve pagination using shared helper (embed filters for integrity)
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"series_id": series_id, "start_date": start_date, "end_date": end_date},
        )

        from ..eia_v2_service import get_eia_service
        svc = await get_eia_service()
        series_points = await svc.get_series(
            series_id=series_id,
            start_date=start_date,
            end_date=end_date,
            offset=offset,
            limit=effective_limit,
        )

        has_more = len(series_points) == effective_limit
        # Create cursors and pagination envelope
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"series_id": series_id, "start_date": start_date, "end_date": end_date},
        )
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={"series_id": series_id, "start_date": start_date, "end_date": end_date},
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
        series_points_out = []
        for point in series_points:
            series_points_out.append(EiaSeriesPoint(
                series_id=point.series_id,
                period=point.period,
                period_start=point.period_start,
                value=point.value,
                unit=point.unit or "unknown",
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "series_id": series_id,
            "start_date": start_date,
            "end_date": end_date,
            "returned_count": len(series_points),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = EiaSeriesResponse(
            data=series_points_out,
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
                "detail": f"Failed to get EIA series: {str(exc)}",
                "instance": "/v2/ref/eia/series",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/ref/eia/series/dimensions", response_model=EiaSeriesDimensionsResponse)
async def get_eia_series_dimensions_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    series_id: Optional[str] = Query(None, description="Optional exact series identifier filter"),
    frequency: Optional[str] = Query(None, description="Optional frequency filter"),
    area: Optional[str] = Query(None, description="Area filter"),
    sector: Optional[str] = Query(None, description="Sector filter"),
    dataset: Optional[str] = Query(None, description="Dataset filter"),
    unit: Optional[str] = Query(None, description="Unit filter"),
    canonical_unit: Optional[str] = Query(None, description="Normalized unit filter"),
    canonical_currency: Optional[str] = Query(None, description="Canonical currency filter"),
    source: Optional[str] = Query(None, description="Source filter"),
) -> EiaSeriesDimensionsResponse:
    """List distinct EIA series dimension values (v2)."""
    start_time = time.perf_counter()

    try:
        from ..eia_v2_service import get_eia_service
        svc = await get_eia_service()
        values = await svc.get_series_dimensions(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000
        data = EiaSeriesDimensionsData(
            dataset=values.get("dataset"),
            area=values.get("area"),
            sector=values.get("sector"),
            unit=values.get("unit"),
            canonical_unit=values.get("canonical_unit"),
            canonical_currency=values.get("canonical_currency"),
            frequency=values.get("frequency"),
            source=values.get("source"),
        )
        meta = Meta(request_id=get_request_id(), query_time_ms=int(round(duration_ms)))
        result = EiaSeriesDimensionsResponse(meta=meta, data=data)
        return respond_with_etag(result, request, response, canonical_url=str(request.url))
    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to list EIA series dimensions: {str(exc)}",
                "instance": "/v2/ref/eia/series/dimensions",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
