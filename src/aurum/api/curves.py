"""Curve data endpoints for market intelligence."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..core.pagination import Cursor, OffsetLimit
from ..telemetry.context import get_request_id
from .models import CurveResponse, CurveDiffResponse, CurvePoint, CurveDiffPoint
from .service import (
    fetch_curve_data,
    fetch_curve_diff_data,
    fetch_curve_strips_data,
    invalidate_curve_cache,
)

router = APIRouter()


@router.get("/v1/curves", response_model=CurveResponse)
async def get_curves(
    request: Request,
    asof: Optional[str] = None,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    cursor: Optional[str] = None,
    since_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None,
) -> CurveResponse:
    """Retrieve curve observations with filtering by identity and tenor."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require curves read permission
    require_permission(principal, Permission.CURVES_READ)

    start_time = time.perf_counter()

    # Handle cursor-based pagination
    if cursor or since_cursor:
        try:
            cursor_obj = Cursor.from_string(cursor or since_cursor)
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid cursor") from exc
    else:
        cursor_obj = None

    # Handle offset-based pagination
    pagination = OffsetLimit(limit=limit, offset=offset)

    try:
        # Start tracing for this API request
        from ..observability.tracing import trace_api_request
        async with trace_api_request("/v1/curves", "GET") as span:
            span.set_attribute("filters", {
                "asof": asof,
                "iso": iso,
                "market": market,
                "location": location,
                "product": product,
                "block": block
            })
            span.set_attribute("pagination", {
                "limit": limit,
                "offset": offset,
                "cursor": bool(cursor_obj)
            })

            # Start database operation trace
            from ..observability.tracing import trace_database_operation
            async with trace_database_operation("fetch_curve_data") as db_span:
                points, meta = await fetch_curve_data(
                    asof=asof,
                    iso=iso,
                    market=market,
                    location=location,
                    product=product,
                    block=block,
                    pagination=pagination,
                    cursor=cursor_obj,
                    prev_cursor=prev_cursor,
                )

                db_span.set_attribute("rows_returned", len(points))
                db_span.set_attribute("has_cursor", bool(meta.next_cursor))

            query_time_ms = (time.perf_counter() - start_time) * 1000

            # Log the API request
            from ..observability.logging import get_api_logger, log_api_request
            api_logger = get_api_logger()
            log_api_request(
                api_logger,
                "/v1/curves",
                "GET",
                200,
                query_time_ms / 1000,
                rows_returned=len(points),
                filters_applied=sum(1 for f in [asof, iso, market, location, product, block] if f is not None)
            )

            # Record metrics
            from ..observability.metrics import increment_api_requests, observe_api_latency
            await increment_api_requests("/v1/curves", "GET", 200)
            await observe_api_latency("/v1/curves", "GET", query_time_ms / 1000)

            return CurveResponse(
                meta={
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                    "next_cursor": meta.next_cursor,
                    "prev_cursor": meta.prev_cursor,
                },
                data=points,
            )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Log the error
        from ..observability.logging import get_api_logger, log_error
        api_logger = get_api_logger()
        log_error(
            api_logger,
            exc,
            "fetch_curve_data",
            endpoint="/v1/curves",
            method="GET"
        )

        # Record error metrics
        from ..observability.metrics import increment_api_requests
        await increment_api_requests("/v1/curves", "GET", 500)

        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch curve data: {str(exc)}"
        ) from exc


@router.get("/v1/curves/diff", response_model=CurveDiffResponse)
async def get_curve_diff(
    request: Request,
    asof_a: str,
    asof_b: str,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> CurveDiffResponse:
    """Compare curve values between two as-of dates."""
    start_time = time.perf_counter()

    pagination = OffsetLimit(limit=limit, offset=offset)

    try:
        points, meta = await fetch_curve_diff_data(
            asof_a=asof_a,
            asof_b=asof_b,
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
            pagination=pagination,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CurveDiffResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=points,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch curve diff data: {str(exc)}"
        ) from exc


@router.get("/v1/curves/strips", response_model=CurveResponse)
async def get_curve_strips(
    request: Request,
    strip_type: str = Query(..., description="Type of strip (CALENDAR, SEASON, etc.)"),
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> CurveResponse:
    """Retrieve curve strips aggregated by time periods."""
    start_time = time.perf_counter()

    pagination = OffsetLimit(limit=limit, offset=offset)

    try:
        points, meta = await fetch_curve_strips_data(
            strip_type=strip_type,
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
            pagination=pagination,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return CurveResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=points,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch curve strips data: {str(exc)}"
        ) from exc
