"""v1 ISO LMP endpoints split from the monolith for maintainability.

Endpoints:
- /v1/iso/lmp/last-24h (JSON/CSV)
- /v1/iso/lmp/hourly (JSON/CSV)
- /v1/iso/lmp/daily (JSON/CSV)
- /v1/iso/lmp/negative (JSON/CSV)

Enable via AURUM_API_V1_SPLIT_ISO=1 in app wiring.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, List, Mapping, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response

from ..http.responses import (
    respond_with_etag,
    csv_response as http_csv_response,
    prepare_csv_value,
    compute_etag,
    build_cache_control_header,
)
from ..models import (
    Meta,
    IsoLmpPoint,
    IsoLmpResponse,
    IsoLmpAggregateResponse,
    IsoLmpAggregatePoint,
)
from ..service import (
    query_iso_lmp_last_24h,
    query_iso_lmp_hourly,
    query_iso_lmp_daily,
    query_iso_lmp_negative,
)
from ..config import CacheConfig
from ..state import get_settings
from ...telemetry.context import get_request_id


router = APIRouter()


def _csv_etag_guard(
    request: Request,
    *,
    rows: Iterable[Mapping[str, Any]],
    fieldnames: List[str],
    filename: str,
    cache_seconds: int,
):
    serialisable_rows = [
        {field: prepare_csv_value(row.get(field)) for field in fieldnames} for row in rows
    ]
    etag = compute_etag({"data": serialisable_rows})
    cache_header = build_cache_control_header(cache_seconds)
    incoming = request.headers.get("if-none-match")
    if incoming and incoming == etag:
        return Response(status_code=304, headers={"ETag": etag, "Cache-Control": cache_header})

    resp = http_csv_response(
        get_request_id() or "unknown",
        serialisable_rows,
        fieldnames,
        filename,
        cache_seconds=cache_seconds,
    )
    resp.headers["ETag"] = etag
    return resp


@router.get("/v1/iso/lmp/last-24h", response_model=IsoLmpResponse)
def iso_lmp_last_24h(
    request: Request,
    response: Response,
    iso_code: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    location_id: Optional[str] = Query(None),
    limit: int = Query(500, ge=1, le=1000),
    format: str = Query("json", pattern="^(json|csv)$"),
) -> IsoLmpResponse:
    cache_cfg = CacheConfig.from_settings(get_settings())
    rows, elapsed = query_iso_lmp_last_24h(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        limit=limit,
        cache_cfg=cache_cfg,
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "delivery_date",
            "interval_start",
            "interval_end",
            "interval_minutes",
            "location_id",
            "location_name",
            "location_type",
            "price_total",
            "price_energy",
            "price_congestion",
            "price_loss",
            "currency",
            "uom",
            "settlement_point",
            "source_run_id",
            "ingest_ts",
            "record_hash",
            "metadata",
        ]
        return _csv_etag_guard(
            request,
            rows=rows,
            fieldnames=fieldnames,
            filename="iso-lmp-last-24h.csv",
            cache_seconds=cache_cfg.ttl_seconds,
        )

    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed)), data=data)
    return respond_with_etag(model, request, response)


@router.get("/v1/iso/lmp/hourly", response_model=IsoLmpAggregateResponse)
def iso_lmp_hourly(
    request: Request,
    response: Response,
    iso_code: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    location_id: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(500, ge=1, le=1000),
    format: str = Query("json", pattern="^(json|csv)$"),
) -> IsoLmpAggregateResponse:
    if start and end and start > end:
        raise HTTPException(status_code=400, detail="start_after_end")
    cache_cfg = CacheConfig.from_settings(get_settings())
    rows, elapsed = query_iso_lmp_hourly(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        start=start,
        end=end,
        limit=limit,
        cache_cfg=cache_cfg,
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "interval_start",
            "location_id",
            "currency",
            "uom",
            "price_avg",
            "price_min",
            "price_max",
            "price_stddev",
            "sample_count",
        ]
        return _csv_etag_guard(
            request,
            rows=rows,
            fieldnames=fieldnames,
            filename="iso-lmp-hourly.csv",
            cache_seconds=cache_cfg.ttl_seconds,
        )

    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed)), data=data)
    return respond_with_etag(model, request, response)


@router.get("/v1/iso/lmp/daily", response_model=IsoLmpAggregateResponse)
def iso_lmp_daily(
    request: Request,
    response: Response,
    iso_code: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    location_id: Optional[str] = Query(None),
    start: Optional[datetime] = Query(None),
    end: Optional[datetime] = Query(None),
    limit: int = Query(500, ge=1, le=1000),
    format: str = Query("json", pattern="^(json|csv)$"),
) -> IsoLmpAggregateResponse:
    if start and end and start > end:
        raise HTTPException(status_code=400, detail="start_after_end")
    cache_cfg = CacheConfig.from_settings(get_settings())
    rows, elapsed = query_iso_lmp_daily(
        iso_code=iso_code,
        market=market,
        location_id=location_id,
        start=start,
        end=end,
        limit=limit,
        cache_cfg=cache_cfg,
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "interval_start",
            "location_id",
            "currency",
            "uom",
            "price_avg",
            "price_min",
            "price_max",
            "price_stddev",
            "sample_count",
        ]
        return _csv_etag_guard(
            request,
            rows=rows,
            fieldnames=fieldnames,
            filename="iso-lmp-daily.csv",
            cache_seconds=cache_cfg.ttl_seconds,
        )

    data = [IsoLmpAggregatePoint(**row) for row in rows]
    model = IsoLmpAggregateResponse(meta=Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed)), data=data)
    return respond_with_etag(model, request, response)


@router.get("/v1/iso/lmp/negative", response_model=IsoLmpResponse)
def iso_lmp_negative(
    request: Request,
    response: Response,
    iso_code: Optional[str] = Query(None),
    market: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    format: str = Query("json", pattern="^(json|csv)$"),
) -> IsoLmpResponse:
    cache_cfg = CacheConfig.from_settings(get_settings())
    rows, elapsed = query_iso_lmp_negative(
        iso_code=iso_code,
        market=market,
        limit=limit,
        cache_cfg=cache_cfg,
    )

    if format.lower() == "csv":
        fieldnames = [
            "iso_code",
            "market",
            "delivery_date",
            "interval_start",
            "interval_end",
            "interval_minutes",
            "location_id",
            "location_name",
            "location_type",
            "price_total",
            "price_energy",
            "price_congestion",
            "price_loss",
            "currency",
            "uom",
            "settlement_point",
            "source_run_id",
            "ingest_ts",
            "record_hash",
            "metadata",
        ]
        return _csv_etag_guard(
            request,
            rows=rows,
            fieldnames=fieldnames,
            filename="iso-lmp-negative.csv",
            cache_seconds=cache_cfg.ttl_seconds,
        )

    data = [IsoLmpPoint(**row) for row in rows]
    model = IsoLmpResponse(meta=Meta(request_id=get_request_id() or "unknown", query_time_ms=int(elapsed)), data=data)
    return respond_with_etag(model, request, response)

