"""v2 Curves API with enhanced features.

This module provides the v2 implementation of the curves API with:
- Cursor-only pagination (offset deprecated)
- Consistent error shapes using RFC 7807
- Enhanced ETag support
- Improved validation and error handling
- Better observability

Notes:
- Base path: `/v2/*` (see app wiring in src/aurum/api/app.py)
- Migration guidance from v1 endpoints: docs/migration-guide.md
"""

from __future__ import annotations

import time
from typing import Any, AsyncIterator, Dict, List, Optional
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from ..deps import get_settings, get_cache_manager
from ..services import CurvesService
from aurum.core import AurumSettings
from ..cache.cache import CacheManager
from ..http import respond_with_etag, deprecation_warning_headers, csv_response
from .pagination import (
    resolve_pagination,
    build_next_cursor,
    build_pagination_envelope,
)
from ...telemetry.context import get_request_id

router = APIRouter(prefix="/v2", tags=["curves"])


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
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    name_filter: Optional[str] = Query(None, description="Filter by curve name"),
    settings: AurumSettings = Depends(get_settings),
    cache_manager: Optional[CacheManager] = Depends(get_cache_manager),
) -> CurveListResponse:
    """List curves with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        # Touch DI (ensures wiring is valid; no behavior change)
        if cache_manager is not None:
            pass
        if settings:
            pass

        # Get curve service façade
        service = CurvesService()

        # Resolve pagination using hardened utilities (embeds filters for integrity)
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"name_filter": name_filter},
        )

        # List curves
        curves = await service.list_curves(
            offset=offset,
            limit=effective_limit,
            name_filter=name_filter,
        )

        # Compute pagination cursors and envelope
        has_more = len(curves) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"name_filter": name_filter},
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={"name_filter": name_filter},
        )
        meta, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

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
        # Create response with enhanced metadata
        # Augment meta with request-specific fields while keeping standardized keys
        meta_out = dict(meta)
        meta_out.update({
            "request_id": get_request_id(),
            "returned_count": len(curves),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = CurveListResponse(
            data=curve_responses,
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
                "detail": f"Failed to list curves: {str(exc)}",
                "instance": "/v2/curves",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get(
    "/curves/export",
    response_class=StreamingResponse,
    summary="Stream curve observations as CSV",
)
async def export_curves_v2(
    request: Request,
    asof: Optional[str] = Query(None, description="Filter by as-of date (YYYY-MM-DD)"),
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    chunk_size: int = Query(1000, ge=100, le=5000, description="Number of rows per streamed chunk"),
    settings: AurumSettings = Depends(get_settings),
) -> StreamingResponse:
    """Stream curve observations backed by Trino."""

    svc = CurvesService()
    async def row_stream() -> AsyncIterator[Dict[str, Any]]:
        async for row in svc.stream_curve_export(
            asof=asof,
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
            chunk_size=chunk_size,
        ):
            yield row

    request_id = get_request_id() or str(uuid4())
    fieldnames = [
        "curve_key",
        "tenor_label",
        "tenor_type",
        "contract_month",
        "asof_date",
        "mid",
        "bid",
        "ask",
        "price_type",
    ]
    filename = f"curve_export_{asof or 'all'}.csv"

    response = csv_response(
        request_id,
        row_stream(),
        fieldnames,
        filename,
        cache_control="public, max-age=300",
    )
    response.headers["Content-Disposition"] = f"attachment; filename=\"{filename}\""
    return response


@router.get("/curves/{curve_id}/diff", response_model=CurveResponse)
async def get_curve_diff_v2(
    request: Request,
    response: Response,
    curve_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    from_timestamp: str = Query(..., description="From timestamp"),
    to_timestamp: str = Query(..., description="To timestamp"),
) -> CurveResponse:
    """Get curve diff with enhanced error handling."""
    start_time = time.perf_counter()

    try:
        # Get curve diff via façade
        svc = CurvesService()
        curve_diff = await svc.get_curve_diff(
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

        # Add ETag for caching with Link headers
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
                "detail": f"Failed to get curve diff: {str(exc)}",
                "instance": f"/v2/curves/{curve_id}/diff",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
