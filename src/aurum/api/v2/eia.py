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
from ...telemetry.context import get_request_id

router = APIRouter()


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
                        "instance": "/v2/metadata/eia/datasets"
                    }
                )

        # Get EIA datasets service (placeholder - would integrate with actual service)
        from ..routes import _eia_datasets_data
        datasets_data = _eia_datasets_data()

        # Apply pagination
        total_count = len(datasets_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = datasets_data[start_idx:end_idx]

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
        datasets = []
        for dataset_data in paginated_data:
            datasets.append(EiaDatasetResponse(
                dataset_path=dataset_data["dataset_path"],
                title=dataset_data["title"],
                description=dataset_data["description"],
                last_updated=dataset_data.get("last_updated", "unknown"),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        result = EiaDatasetsResponse(
            data=datasets,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "total_count": total_count,
                "returned_count": len(datasets),
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
                        "instance": "/v2/ref/eia/series"
                    }
                )

        # Get EIA series service (placeholder - would integrate with actual service)
        from ..routes import _eia_series_data
        series_data = _eia_series_data(series_id, start_date, end_date)

        # Apply pagination
        total_count = len(series_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = series_data[start_idx:end_idx]

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
        series_points = []
        for point_data in paginated_data:
            series_points.append(EiaSeriesPoint(
                series_id=series_id,
                period=point_data["period"],
                period_start=point_data["period_start"],
                value=point_data["value"],
                unit=point_data.get("unit", "unknown"),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        result = EiaSeriesResponse(
            data=series_points,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "series_id": series_id,
                "start_date": start_date,
                "end_date": end_date,
                "total_count": total_count,
                "returned_count": len(series_points),
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
                "detail": f"Failed to get EIA series: {str(exc)}",
                "instance": "/v2/ref/eia/series",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
