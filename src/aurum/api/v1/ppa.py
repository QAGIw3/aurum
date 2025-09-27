"""v1 PPA endpoints split from the monolith for maintainability.

Endpoints:
- /v1/ppa/contracts (list, create)
- /v1/ppa/contracts/{contract_id} (get, patch, delete)
- /v1/ppa/contracts/{contract_id}/valuations (list)
- /v1/ppa/valuate (adhoc valuation)

This router is always enabled by default. The AURUM_API_V1_SPLIT_PPA flag is deprecated.
"""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response

from ..http.pagination import encode_cursor, decode_cursor
from ..models import (
    Meta,
    PpaContractCreate,
    PpaContractUpdate,
    PpaContractOut,
    PpaContractResponse,
    PpaContractListResponse,
    PpaValuationRecord,
    PpaValuationListResponse,
    PpaValuationRequest,
    PpaValuationResponse,
    PpaMetric,
)
from ..services.ppa_service import PpaService
from ..http.responses import respond_with_etag
from ...telemetry.context import get_request_id
from ..scenarios.scenario_service import STORE as ScenarioStore


router = APIRouter()
_PPA_SERVICE = PpaService()


def _tenant_from_request(request: Request, explicit: Optional[str] = None) -> Optional[str]:
    if explicit:
        return explicit
    if getattr(request.state, "tenant", None):
        return request.state.tenant
    principal = getattr(request.state, "principal", {}) or {}
    if principal.get("tenant"):
        return principal.get("tenant")
    header_tenant = request.headers.get("X-Aurum-Tenant")
    if header_tenant:
        return header_tenant
    return None


def _contract_to_model(record) -> PpaContractOut:
    if record is None:
        raise ValueError("PPA contract record is required")
    terms = record.terms if isinstance(record.terms, dict) else {}
    return PpaContractOut(
        ppa_contract_id=record.id,
        tenant_id=record.tenant_id,
        instrument_id=record.instrument_id,
        terms=terms,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@router.get("/v1/ppa/contracts", response_model=PpaContractListResponse)
def list_ppa_contracts(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=200),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
    offset: Optional[int] = Query(None, ge=0),
) -> PpaContractListResponse:
    request_id = get_request_id() or "unknown"
    tenant_id = _tenant_from_request(request)
    start = time.perf_counter()
    effective_offset = offset
    token = cursor or since_cursor
    if token:
        payload = decode_cursor(token)
        try:
            effective_offset = max(int(payload.values.get("offset", 0)), 0) if hasattr(payload, "values") else int(payload.get("offset", 0))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid cursor") from exc

    records = ScenarioStore.list_ppa_contracts(tenant_id, limit=limit + 1, offset=effective_offset)
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    more = len(records) > limit
    if more:
        records = records[:limit]

    next_cursor = None
    if more:
        next_cursor = encode_cursor({"offset": effective_offset + limit})

    prev_cursor_value = None
    if (effective_offset or 0) > 0:
        prev_offset = max((effective_offset or 0) - limit, 0)
        prev_cursor_value = encode_cursor({"offset": prev_offset})

    data = [_contract_to_model(record) for record in records]
    meta = Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor, prev_cursor=prev_cursor_value)
    model = PpaContractListResponse(meta=meta, data=data)
    return respond_with_etag(model, request, response)


@router.post("/v1/ppa/contracts", response_model=PpaContractResponse, status_code=201)
def create_ppa_contract(payload: PpaContractCreate, request: Request) -> PpaContractResponse:
    request_id = get_request_id() or "unknown"
    tenant_id = _tenant_from_request(request)
    if not tenant_id:
        raise HTTPException(status_code=400, detail="tenant_id is required")
    record = ScenarioStore.create_ppa_contract(tenant_id=tenant_id, instrument_id=payload.instrument_id, terms=payload.terms)
    return PpaContractResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=_contract_to_model(record))


@router.get("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
def get_ppa_contract(contract_id: str, request: Request, response: Response) -> PpaContractResponse:
    request_id = get_request_id() or "unknown"
    tenant_id = _tenant_from_request(request)
    record = ScenarioStore.get_ppa_contract(contract_id, tenant_id=tenant_id)
    if record is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    model = PpaContractResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=_contract_to_model(record))
    return respond_with_etag(model, request, response)


@router.patch("/v1/ppa/contracts/{contract_id}", response_model=PpaContractResponse)
def update_ppa_contract(contract_id: str, payload: PpaContractUpdate, request: Request) -> PpaContractResponse:
    request_id = get_request_id() or "unknown"
    tenant_id = _tenant_from_request(request)
    record = ScenarioStore.update_ppa_contract(tenant_id=tenant_id, contract_id=contract_id, instrument_id=payload.instrument_id, terms=payload.terms)
    if record is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    return PpaContractResponse(meta=Meta(request_id=request_id, query_time_ms=0), data=_contract_to_model(record))


@router.delete("/v1/ppa/contracts/{contract_id}", status_code=204)
def delete_ppa_contract(contract_id: str, request: Request) -> Response:
    tenant_id = _tenant_from_request(request)
    deleted = ScenarioStore.delete_ppa_contract(contract_id, tenant_id=tenant_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="PPA contract not found")
    return Response(status_code=204)


@router.get("/v1/ppa/contracts/{contract_id}/valuations", response_model=PpaValuationListResponse)
def list_ppa_contract_valuations(
    contract_id: str,
    request: Request,
    response: Response,
    scenario_id: Optional[str] = Query(None),
    metric: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    cursor: Optional[str] = Query(None),
    since_cursor: Optional[str] = Query(None),
    offset: Optional[int] = Query(None, ge=0),
    prev_cursor: Optional[str] = Query(None),
) -> PpaValuationListResponse:
    request_id = get_request_id() or "unknown"
    tenant_id = _tenant_from_request(request)
    # Verify contract exists
    if ScenarioStore.get_ppa_contract(contract_id, tenant_id=tenant_id) is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")

    effective_offset = offset or 0
    token = prev_cursor or cursor or since_cursor
    if token:
        try:
            payload = decode_cursor(token)
            effective_offset = max(int(payload.values.get("offset", 0)), 0) if hasattr(payload, "values") else int(payload.get("offset", 0))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid cursor")

    rows, elapsed_ms = _PPA_SERVICE.list_contract_valuation_rows(
        contract_id=contract_id,
        scenario_id=scenario_id,
        metric=metric,
        tenant_id=tenant_id,
        limit=limit + 1,
        offset=effective_offset,
        trino_cfg=None,
    )

    more = len(rows) > limit
    if more:
        rows = rows[:limit]

    data: List[PpaValuationRecord] = []
    for row in rows:
        data.append(
            PpaValuationRecord(
                asof_date=row.get("asof_date"),
                scenario_id=row.get("scenario_id"),
                period_start=row.get("period_start"),
                period_end=row.get("period_end"),
                metric=row.get("metric"),
                value=row.get("value"),
                cashflow=row.get("cashflow"),
                npv=row.get("npv"),
                irr=row.get("irr"),
                curve_key=row.get("curve_key"),
                version_hash=row.get("version_hash"),
                ingested_at=row.get("_ingest_ts"),
            )
        )

    next_cursor = encode_cursor({"offset": effective_offset + limit}) if more else None
    prev_cursor_value = encode_cursor({"offset": max(effective_offset - limit, 0)}) if effective_offset > 0 else None

    meta = Meta(request_id=request_id, query_time_ms=int(elapsed_ms), next_cursor=next_cursor, prev_cursor=prev_cursor_value)
    model = PpaValuationListResponse(meta=meta, data=data)
    return respond_with_etag(model, request, response)


@router.post("/v1/ppa/valuate", response_model=PpaValuationResponse)
def valuate_ppa(payload: PpaValuationRequest, request: Request) -> PpaValuationResponse:
    request_id = get_request_id() or "unknown"
    if not payload.scenario_id:
        raise HTTPException(status_code=400, detail="scenario_id is required for valuation")
    tenant_id = _tenant_from_request(request)
    if ScenarioStore.get_ppa_contract(payload.ppa_contract_id, tenant_id=tenant_id) is None:
        raise HTTPException(status_code=404, detail="PPA contract not found")

    rows, elapsed_ms = _PPA_SERVICE.calculate_valuation(
        scenario_id=payload.scenario_id,
        tenant_id=tenant_id,
        asof_date=payload.asof_date,
        options=payload.options or None,
        trino_cfg=None,
    )

    fallback_date = payload.asof_date or __import__("datetime").date.today()
    metrics: List[PpaMetric] = []
    for row in rows:
        period_start = row.get("period_start") or fallback_date
        period_end = row.get("period_end") or period_start
        metrics.append(
            PpaMetric(
                period_start=period_start,
                period_end=period_end,
                metric=str(row.get("metric") or "mid"),
                value=float(row.get("value") or 0.0),
                currency=row.get("currency"),
                unit=row.get("unit"),
                run_id=row.get("run_id"),
                curve_key=row.get("curve_key"),
                tenor_type=row.get("tenor_type"),
            )
        )

    return PpaValuationResponse(meta=Meta(request_id=request_id, query_time_ms=int(elapsed_ms)), data=metrics)
