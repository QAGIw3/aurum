"""v2 Admin API with enhanced features.

This module provides the v2 implementation of the admin API with:
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

router = APIRouter(prefix="/v2", tags=["admin"])


class CachePurgeResponse(BaseModel):
    """Response for cache purge operation with v2 enhancements."""
    cache_type: str = Field(..., description="Type of cache purged")
    keys_purged: int = Field(..., description="Number of keys purged")
    meta: dict = Field(..., description="Metadata")


class SeriesCurveMappingResponse(BaseModel):
    """Response for series-curve mapping data with v2 enhancements."""
    provider: str = Field(..., description="Data provider")
    series_id: str = Field(..., description="Series identifier")
    curve_key: str = Field(..., description="Curve key")
    mapping_type: str = Field(..., description="Mapping type")
    meta: dict = Field(..., description="Metadata")


class SeriesCurveMappingListResponse(BaseModel):
    """Response for series-curve mappings list with v2 enhancements."""
    data: List[SeriesCurveMappingResponse] = Field(..., description="List of mappings")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.post("/admin/cache/scenario/{scenario_id}/invalidate", status_code=204)
async def invalidate_scenario_cache_v2(
    request: Request,
    response: Response,
    scenario_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
) -> Response:
    """Invalidate scenario cache with enhanced validation and error handling."""
    start_time = time.perf_counter()

    try:
        # Invalidate cache service (placeholder - would integrate with actual service)
        from ..routes import _invalidate_scenario_cache
        keys_purged = _invalidate_scenario_cache(scenario_id)

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        response.headers["X-Request-Id"] = get_request_id()
        response.headers["X-Tenant-Id"] = tenant_id
        response.headers["X-Processing-Time-Ms"] = str(round(duration_ms, 2))
        response.headers["X-Keys-Purged"] = str(keys_purged)

        return response

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to invalidate scenario cache: {str(exc)}",
                "instance": f"/v2/admin/cache/scenario/{scenario_id}/invalidate",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.post("/admin/cache/curves/invalidate", response_model=CachePurgeResponse, status_code=200)
async def invalidate_curves_cache_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
) -> CachePurgeResponse:
    """Invalidate curves cache with enhanced validation and error handling."""
    start_time = time.perf_counter()

    try:
        # Invalidate curves cache service (placeholder - would integrate with actual service)
        from ..routes import _invalidate_curves_cache
        keys_purged = _invalidate_curves_cache()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = CachePurgeResponse(
            cache_type="curves",
            keys_purged=keys_purged,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "processing_time_ms": round(duration_ms, 2),
                "version": "v2"
            }
        )

        # Add ETag for caching with Link headers
        return respond_with_etag(result, request, response, canonical_url=str(request.url.remove_query_params("cursor")))

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to invalidate curves cache: {str(exc)}",
                "instance": "/v2/admin/cache/curves/invalidate",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/admin/mappings", response_model=SeriesCurveMappingListResponse)
async def list_mappings_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    provider_filter: Optional[str] = Query(None, description="Filter by provider"),
    series_filter: Optional[str] = Query(None, description="Filter by series ID"),
) -> SeriesCurveMappingListResponse:
    """List series-curve mappings with enhanced pagination and filtering."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"provider": provider_filter, "series": series_filter},
        )

        # Get mappings service (placeholder - would integrate with actual service)
        from ..routes import _mappings_data
        mappings_data = _mappings_data(provider_filter, series_filter)

        # Apply pagination
        total_count = len(mappings_data)
        start_idx = offset
        end_idx = offset + effective_limit
        paginated_data = mappings_data[start_idx:end_idx]

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=end_idx < total_count,
            filters={"provider": provider_filter, "series": series_filter},
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"provider": provider_filter, "series": series_filter})
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
        mappings = []
        for mapping_data in paginated_data:
            mappings.append(SeriesCurveMappingResponse(
                provider=mapping_data["provider"],
                series_id=mapping_data["series_id"],
                curve_key=mapping_data["curve_key"],
                mapping_type=mapping_data["mapping_type"],
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "filters": {
                "provider": provider_filter,
                "series": series_filter,
            },
            "returned_count": len(mappings),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = SeriesCurveMappingListResponse(
            data=mappings,
            meta=meta_out,
            links=links,
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
                "detail": f"Failed to list mappings: {str(exc)}",
                "instance": "/v2/admin/mappings",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
