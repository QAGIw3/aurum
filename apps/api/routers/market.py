"""Market API views routed through pluggable data backends with pagination.

Exposes canonical API-oriented views under `iceberg.market.*` via Trino,
and surfaces backend query IDs and pool metrics when debug=true.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response

from aurum.core.settings import AurumSettings, get_settings
from aurum.libs.services.market_service import MarketService
from aurum.api.v2.pagination import (
    resolve_pagination,
    build_next_cursor,
    build_prev_cursor,
    build_pagination_envelope,
)


router = APIRouter()

_service = MarketService()


@router.get("/curves/latest")
async def curves_latest(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None),
    limit: Optional[int] = Query(100, ge=1, le=1000),
    debug: bool = Query(False),
):
    offset, eff_limit = resolve_pagination(cursor=cursor, limit=limit, default_limit=100, filters={"tenant_id": tenant_id})
    data, dbg = await _service.curves_latest(tenant_id=tenant_id, offset=offset, limit=eff_limit, include_debug=debug)
    has_more = len(data) == eff_limit
    next_cursor = build_next_cursor(offset=offset, limit=eff_limit, has_more=has_more, filters={"tenant_id": tenant_id})
    prev_cursor = build_prev_cursor(offset=offset, limit=eff_limit, filters={"tenant_id": tenant_id})
    meta, links = build_pagination_envelope(
        request_url=request.url,
        offset=offset,
        limit=eff_limit,
        total=None,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
    )
    meta_out = {**meta}
    if debug and dbg:
        meta_out["debug"] = dbg
    return {"data": data, "meta": meta_out, "links": links}


@router.get("/curves/asof")
async def curves_asof(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    asof_date: Optional[str] = Query(None, description="Filter by as-of date (YYYY-MM-DD)"),
    cursor: Optional[str] = Query(None),
    limit: Optional[int] = Query(100, ge=1, le=1000),
    debug: bool = Query(False),
):
    filters = {"tenant_id": tenant_id, "asof_date": asof_date}
    offset, eff_limit = resolve_pagination(cursor=cursor, limit=limit, default_limit=100, filters=filters)
    data, dbg = await _service.curves_asof(tenant_id=tenant_id, asof_date=asof_date, offset=offset, limit=eff_limit, include_debug=debug)
    has_more = len(data) == eff_limit
    next_cursor = build_next_cursor(offset=offset, limit=eff_limit, has_more=has_more, filters=filters)
    prev_cursor = build_prev_cursor(offset=offset, limit=eff_limit, filters=filters)
    meta, links = build_pagination_envelope(
        request_url=request.url,
        offset=offset,
        limit=eff_limit,
        total=None,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
    )
    meta_out = {**meta}
    if debug and dbg:
        meta_out["debug"] = dbg
    return {"data": data, "meta": meta_out, "links": links}


@router.get("/curves/asof/diff")
async def curves_asof_diff(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    cursor: Optional[str] = Query(None),
    limit: Optional[int] = Query(100, ge=1, le=1000),
    debug: bool = Query(False),
):
    filters = {"tenant_id": tenant_id}
    offset, eff_limit = resolve_pagination(cursor=cursor, limit=limit, default_limit=100, filters=filters)
    data, dbg = await _service.curves_asof_diff(tenant_id=tenant_id, offset=offset, limit=eff_limit, include_debug=debug)
    has_more = len(data) == eff_limit
    next_cursor = build_next_cursor(offset=offset, limit=eff_limit, has_more=has_more, filters=filters)
    prev_cursor = build_prev_cursor(offset=offset, limit=eff_limit, filters=filters)
    meta, links = build_pagination_envelope(
        request_url=request.url,
        offset=offset,
        limit=eff_limit,
        total=None,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
    )
    meta_out = {**meta}
    if debug and dbg:
        meta_out["debug"] = dbg
    return {"data": data, "meta": meta_out, "links": links}


@router.get("/scenarios/output")
async def scenario_output_view(
    request: Request,
    response: Response,
    tenant_id: str = Query(...),
    scenario_id: Optional[str] = Query(None),
    metric: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    limit: Optional[int] = Query(100, ge=1, le=1000),
    debug: bool = Query(False),
):
    filters = {"tenant_id": tenant_id, "scenario_id": scenario_id, "metric": metric}
    offset, eff_limit = resolve_pagination(cursor=cursor, limit=limit, default_limit=100, filters=filters)
    data, dbg = await _service.scenario_output_view(
        tenant_id=tenant_id,
        scenario_id=scenario_id,
        metric=metric,
        offset=offset,
        limit=eff_limit,
        include_debug=debug,
    )
    has_more = len(data) == eff_limit
    next_cursor = build_next_cursor(offset=offset, limit=eff_limit, has_more=has_more, filters=filters)
    prev_cursor = build_prev_cursor(offset=offset, limit=eff_limit, filters=filters)
    meta, links = build_pagination_envelope(
        request_url=request.url,
        offset=offset,
        limit=eff_limit,
        total=None,
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
    )
    meta_out = {**meta}
    if debug and dbg:
        meta_out["debug"] = dbg
    return {"data": data, "meta": meta_out, "links": links}




