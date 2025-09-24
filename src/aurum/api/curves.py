"""Curve data endpoints for market intelligence."""

from __future__ import annotations

import asyncio
import csv
import json
import time
from io import StringIO
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, Response

from ..core.pagination import Cursor, OffsetLimit
from ..telemetry.context import get_request_id
from .models import CurveResponse, CurveDiffResponse, CurvePoint, CurveDiffPoint
from .service import (
    fetch_curve_data,
    fetch_curve_diff_data,
    fetch_curve_strips_data,
    invalidate_curve_cache,
)
from .http import respond_with_etag, prepare_csv_value
from .trino_client import get_trino_client
from .query import build_curve_export_query

router = APIRouter()


@router.get("/v1/curves", response_model=CurveResponse)
async def get_curves(
    request: Request,
    response: Response,
    asof: Optional[str] = None,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    prev_cursor: Optional[str] = Query(None, description="Cursor for previous page"),
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

    # Add deprecation headers if offset is used
    if offset is not None:
        from .http import deprecation_warning_headers
        response.headers.update(deprecation_warning_headers(
            "offset-based pagination",
            "v2.0.0"
        ))

    try:
        # Guardrails: require at least one filter unless explicitly overridden
        pass  # enforce_basic_query_guardrails disabled for now
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

            model = CurveResponse(
                meta={
                    "request_id": get_request_id(),
                    "query_time_ms": round(query_time_ms, 2),
                    "next_cursor": meta.next_cursor,
                    "prev_cursor": meta.prev_cursor,
                },
                data=points,
            )

            # Handle ETag and If-None-Match
            return respond_with_etag(model, request, response)

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
    response: Response,
    asof_a: str,
    asof_b: str,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    limit: int = Query(100, ge=1, le=500),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
) -> CurveDiffResponse:
    """Compare curve values between two as-of dates."""
    start_time = time.perf_counter()

    pagination = OffsetLimit(limit=limit, offset=offset)

    # Add deprecation headers if offset is used
    if offset is not None:
        from .http import deprecation_warning_headers
        response.headers.update(deprecation_warning_headers(
            "offset-based pagination",
            "v2.0.0"
        ))

    try:
        pass  # enforce_basic_query_guardrails disabled for now
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

        model = CurveDiffResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=points,
        )

        # Handle ETag and If-None-Match
        return respond_with_etag(model, request, response)

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
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
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


@router.get(
    "/v2/curves/export",
    response_class=StreamingResponse,
    summary="Stream curve observations as CSV",
)
async def export_curves(
    request: Request,
    asof: Optional[str] = Query(None, description="Filter by as-of date (YYYY-MM-DD)"),
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    chunk_size: int = Query(1000, ge=100, le=5000, description="Number of rows per streamed chunk"),
) -> StreamingResponse:
    """Stream curve observations with CSV backpressure-friendly streaming."""
    from .auth import require_permission, Permission

    principal = getattr(request.state, "principal", None)
    require_permission(principal, Permission.CURVES_READ)

    query = build_curve_export_query(
        asof=asof,
        iso=iso,
        market=market,
        location=location,
        product=product,
        block=block,
    )

    client = get_trino_client()
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

    async def csv_stream():
        buffer = StringIO()
        writer = csv.DictWriter(buffer, fieldnames=fieldnames)
        writer.writeheader()
        payload = buffer.getvalue()
        if payload:
            yield payload
        buffer.seek(0)
        buffer.truncate(0)

        async for chunk in client.stream_query(query, chunk_size=chunk_size):
            for row in chunk:
                formatted = {field: prepare_csv_value(row.get(field)) for field in fieldnames}
                writer.writerow(formatted)

            payload = buffer.getvalue()
            if payload:
                yield payload
            buffer.seek(0)
            buffer.truncate(0)
            await asyncio.sleep(0)

    filename = f"curve_export_{asof or 'all'}.csv"
    response = StreamingResponse(csv_stream(), media_type="text/csv; charset=utf-8")
    response.headers["Content-Disposition"] = f"attachment; filename=\"{filename}\""
    return response


# --- v2 integration helpers -------------------------------------------------
from .curves_v2_service import get_curve_service  # re-export for v2 router
