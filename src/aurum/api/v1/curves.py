"""v1 curve endpoints split from the monolithic router for maintainability.

Endpoints:
- /v1/curves
- /v1/curves/diff
- /v1/curves/strips

This router is always enabled by default. The AURUM_API_V1_SPLIT_CURVES flag is deprecated.
"""

from __future__ import annotations

import time
from datetime import date
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response

from aurum.core.pagination import Cursor

from ..http import (
    build_cache_control_header,
    compute_etag,
    csv_response as http_csv_response,
    decode_cursor,
    encode_cursor,
    extract_cursor_payload_from_row,
    prepare_csv_value,
    respond_with_etag,
    MAX_PAGE_SIZE,
)
from ..config import CacheConfig
from ..state import get_settings
from ..models import CurveDiffPoint, CurveDiffResponse, CurvePoint, CurveResponse, Meta
from ..query import ORDER_COLUMNS as CURVE_ORDER_COLUMNS, DIFF_ORDER_COLUMNS as CURVE_DIFF_ORDER_COLUMNS
from ..service import query_curves, query_curves_diff
from ..routes import (
    CURVE_CACHE_TTL,
    CURVE_DIFF_CACHE_TTL,
    CURVE_MAX_LIMIT,
    CURVE_STRIP_CACHE_TTL,
    _current_request_id,
    _trino_config,
)

router = APIRouter()


CURVE_CURSOR_FIELDS = list(CURVE_ORDER_COLUMNS)
CURVE_DIFF_CURSOR_FIELDS = list(CURVE_DIFF_ORDER_COLUMNS)


@router.get("/v1/curves", response_model=CurveResponse)
def list_curves(
    request: Request,
    asof: Optional[date] = Query(None, description="As-of date filter (YYYY-MM-DD)"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    prev_cursor: Optional[str] = Query(
        None,
        description="Cursor pointing to the previous page; obtained from meta.prev_cursor",
    ),
    format: str = Query(
        "json",
        pattern="^(json|csv)$",
        description="Set to 'csv' to stream results as CSV",
    ),
    *,
    response: Response,
) -> CurveResponse:
    """List curve observations with cursor pagination and optional CSV export."""
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    cache_cfg = CacheConfig.from_settings(get_settings(), ttl_override=CURVE_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    effective_offset = offset
    cursor_after: Optional[dict] = None
    cursor_before: Optional[dict] = None
    descending = False

    current_filters = {
        "asof": asof,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    prev_token = prev_cursor
    forward_token = cursor or since_cursor
    if prev_token:
        try:
            cursor_obj = Cursor.from_string(prev_token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc

        if cursor_obj.is_expired():
            raise HTTPException(status_code=400, detail="Cursor has expired")
        if not cursor_obj.matches_filters(current_filters):
            raise HTTPException(
                status_code=400,
                detail="Cursor filters do not match current query parameters",
            )

        cursor_before = cursor_obj.filters
        descending = True
        effective_offset = 0
    elif forward_token:
        try:
            cursor_obj = Cursor.from_string(forward_token)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc

        if cursor_obj.is_expired():
            raise HTTPException(status_code=400, detail="Cursor has expired")
        if not cursor_obj.matches_filters(current_filters):
            raise HTTPException(
                status_code=400,
                detail="Cursor filters do not match current query parameters",
            )

        effective_offset = cursor_obj.offset
        cursor_after = cursor_obj.filters

    dimension_count = sum(
        1
        for dim in [curve_key, asset_class, iso, location, market, product, block, tenor_type]
        if dim is not None
    )

    if dimension_count == 0 and limit > 500:
        raise HTTPException(
            status_code=400,
            detail="Queries without dimension filters with high limits may be too broad. Consider adding filters or reducing limit.",
        )

    if dimension_count == 1 and limit > 1000:
        raise HTTPException(
            status_code=400,
            detail="Single-dimension queries with high limits may be too broad. Consider adding more filters or reducing limit.",
        )

    expensive_combinations = [
        {"iso": None, "market": None, "product": None},
        {"asset_class": None, "location": None},
        {"curve_key": None},
    ]

    for combo in expensive_combinations:
        if all(current_filters.get(k) is None for k in combo.keys()) and limit > 500:
            raise HTTPException(
                status_code=400,
                detail=f"Query with dimensions {list(combo.keys())} and limit > 500 may be too expensive. Consider adding more specific filters.",
            )

    try:
        rows, elapsed_ms = query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=effective_limit + 1,
            offset=effective_offset or 0,
            cursor_after=cursor_after,
            cursor_before=cursor_before,
            descending=descending,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    more = len(rows) > effective_limit
    if descending:
        rows = rows[:effective_limit]
        rows.reverse()
    elif more:
        rows = rows[:effective_limit]

    next_cursor = None
    if more and rows:
        next_cursor_obj = Cursor(
            offset=(effective_offset or 0) + len(rows),
            limit=effective_limit,
            timestamp=time.time(),
            filters=current_filters,
        )
        next_cursor = next_cursor_obj.to_string()

    prev_cursor_value = None
    if rows:
        if descending and cursor_before and more:
            prev_payload = extract_cursor_payload_from_row(rows[0], CURVE_CURSOR_FIELDS)
            prev_cursor_obj = Cursor(
                offset=effective_offset or 0,
                limit=effective_limit,
                timestamp=time.time(),
                filters={**current_filters, "before": prev_payload},
            )
            prev_cursor_value = prev_cursor_obj.to_string()
        elif prev_token or forward_token or (effective_offset or 0) > 0:
            prev_payload = extract_cursor_payload_from_row(rows[0], CURVE_CURSOR_FIELDS)
            prev_cursor_obj = Cursor(
                offset=max((effective_offset or 0) - effective_limit, 0),
                limit=effective_limit,
                timestamp=time.time(),
                filters={**current_filters, "before": prev_payload},
            )
            prev_cursor_value = prev_cursor_obj.to_string()

    if format.lower() == "csv":
        cache_seconds = CURVE_CACHE_TTL
        cache_header = build_cache_control_header(cache_seconds)
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
        serialisable_rows = [
            {field: prepare_csv_value(row.get(field)) for field in fieldnames}
            for row in rows
        ]
        etag = compute_etag({"data": serialisable_rows, "cursor": next_cursor})
        incoming = request.headers.get("if-none-match")
        not_modified_headers = {
            "ETag": etag,
            "X-Request-Id": request_id,
            "Cache-Control": cache_header,
        }
        if next_cursor:
            not_modified_headers["X-Next-Cursor"] = next_cursor
        if prev_cursor_value:
            not_modified_headers["X-Prev-Cursor"] = prev_cursor_value
        if incoming and incoming == etag:
            return Response(status_code=304, headers=not_modified_headers)

        csv_response = http_csv_response(
            request_id,
            serialisable_rows,
            fieldnames,
            filename="curves.csv",
            cache_seconds=cache_seconds,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        )
        csv_response.headers["ETag"] = etag
        return csv_response

    model = CurveResponse(
        meta=Meta(
            request_id=request_id,
            query_time_ms=int(elapsed_ms),
            next_cursor=next_cursor,
            prev_cursor=prev_cursor_value,
        ),
        data=[CurvePoint(**row) for row in rows],
    )
    return respond_with_etag(model, request, response, cache_seconds=CURVE_CACHE_TTL)


@router.get("/v1/curves/diff", response_model=CurveDiffResponse)
def list_curves_diff(
    request: Request,
    asof_a: date = Query(..., description="First as-of date"),
    asof_b: date = Query(..., description="Second as-of date"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    tenor_type: Optional[str] = Query(None, pattern="^(MONTHLY|CALENDAR|SEASON|QUARTER)$"),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    *,
    response: Response,
) -> CurveDiffResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    cache_cfg = CacheConfig.from_settings(get_settings(), ttl_override=CURVE_DIFF_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    cursor_after: Optional[dict] = None
    effective_cursor = cursor or since_cursor

    current_filters = {
        "asof_a": asof_a,
        "asof_b": asof_b,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    if effective_cursor:
        try:
            cursor_obj = Cursor.from_string(effective_cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid cursor: {exc}") from exc

        if cursor_obj.is_expired():
            raise HTTPException(status_code=400, detail="Cursor has expired")
        if not cursor_obj.matches_filters(current_filters):
            raise HTTPException(
                status_code=400,
                detail="Cursor filters do not match current query parameters",
            )

        offset = cursor_obj.offset
        cursor_after = cursor_obj.filters

    if asof_a == asof_b:
        raise HTTPException(status_code=400, detail="asof_a and asof_b must be different dates for comparison")

    days_diff = (asof_b - asof_a).days
    if abs(days_diff) > 365:
        raise HTTPException(
            status_code=400,
            detail=f"Date range too large: {abs(days_diff)} days. Maximum allowed: 365 days",
        )

    dimension_count = sum(
        1
        for dim in [curve_key, asset_class, iso, location, market, product, block, tenor_type]
        if dim is not None
    )

    if dimension_count == 0:
        raise HTTPException(
            status_code=400,
            detail="At least one dimension filter must be specified to avoid excessive result sets",
        )

    if dimension_count == 1 and limit > 1000:
        raise HTTPException(
            status_code=400,
            detail="Single-dimension queries with high limits may be too broad. Consider adding more filters or reducing limit.",
        )

    expensive_combinations = [
        {"iso": None, "market": None, "product": None},
        {"asset_class": None, "location": None},
    ]

    current_dims = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }

    for combo in expensive_combinations:
        if all(current_dims.get(k) is None for k in combo.keys()) and limit > 500:
            raise HTTPException(
                status_code=400,
                detail=f"Query with dimensions {list(combo.keys())} and limit > 500 may be too expensive. Consider adding more specific filters.",
            )

    try:
        rows, elapsed_ms = query_curves_diff(
            trino_cfg,
            cache_cfg,
            asof_a=asof_a,
            asof_b=asof_b,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=tenor_type,
            limit=effective_limit + 1,
            offset=offset or 0,
            cursor_after=cursor_after,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor = None
    if len(rows) > effective_limit:
        rows = rows[:effective_limit]
        next_payload = extract_cursor_payload_from_row(rows[-1], CURVE_DIFF_CURSOR_FIELDS)
        next_cursor = encode_cursor(next_payload)

    model = CurveDiffResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurveDiffPoint(**row) for row in rows],
    )
    return respond_with_etag(model, request, response, cache_seconds=CURVE_DIFF_CACHE_TTL)


@router.get("/v1/curves/strips", response_model=CurveResponse)
def list_curve_strips(
    request: Request,
    asof: Optional[date] = Query(None),
    type: str = Query(..., pattern="^(CALENDAR|SEASON|QUARTER)$", description="Strip type"),
    curve_key: Optional[str] = Query(None),
    asset_class: Optional[str] = Query(None),
    iso: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    product: Optional[str] = Query(None),
    block: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=MAX_PAGE_SIZE),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration from a previous next_cursor value"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    *,
    response: Response,
) -> CurveResponse:
    request_id = _current_request_id()
    trino_cfg = _trino_config()
    cache_cfg = CacheConfig.from_settings(get_settings(), ttl_override=CURVE_STRIP_CACHE_TTL)

    effective_limit = min(limit, CURVE_MAX_LIMIT)
    effective_offset = offset or 0
    effective_cursor = cursor or since_cursor
    if effective_cursor:
        payload = decode_cursor(effective_cursor)
        effective_offset = int(payload.get("offset", 0))

    try:
        rows, elapsed_ms = query_curves(
            trino_cfg,
            cache_cfg,
            asof=asof,
            curve_key=curve_key,
            asset_class=asset_class,
            iso=iso,
            location=location,
            market=market,
            product=product,
            block=block,
            tenor_type=type,
            limit=effective_limit + 1,
            offset=effective_offset,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc

    next_cursor = None
    if len(rows) > effective_limit:
        rows = rows[:effective_limit]
        next_cursor = encode_cursor({"offset": effective_offset + effective_limit})

    model = CurveResponse(
        meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor),
        data=[CurvePoint(**row) for row in rows],
    )
    return respond_with_etag(model, request, response, cache_seconds=CURVE_STRIP_CACHE_TTL)


__all__ = ["router"]
