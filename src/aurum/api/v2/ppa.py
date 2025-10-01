"""v2 PPA API with enhanced features.

This module provides the v2 implementation of the PPA API with:
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
from datetime import date, datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag
from ..http.response_builders import etag_response_builder, etag_cursor_response_builder
from ..ppa_v2_service import PpaV2Service, get_ppa_service
from .pagination import (
    resolve_pagination,
    build_next_cursor,
    build_prev_cursor,
    build_pagination_envelope,
)
from ...telemetry.context import get_request_id
from ..exceptions import QueryParameterValidationException, NotImplementedException

router = APIRouter(prefix="/v2", tags=["ppa"])


def _normalize_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _validation_error(request: Request, parameter: str, message: str) -> HTTPException:
    # Raise standardized domain exception; middleware/handler will convert to RFC7807
    raise QueryParameterValidationException(parameter=parameter, message=message, request_id=get_request_id())


def _validate_tenant_id(request: Request, tenant_id: str) -> str:
    normalized = _normalize_text(tenant_id)
    if not normalized:
        _validation_error(request, "tenant_id", "tenant_id must be provided")
    return normalized


def _validate_contract_id(request: Request, contract_id: str) -> str:
    normalized = _normalize_text(contract_id)
    if not normalized:
        _validation_error(request, "contract_id", "contract_id must be provided")
    return normalized


def _normalize_date_query(request: Request, parameter: str, value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    text = value.strip()
    if not text:
        raise _validation_error(request, parameter, f"{parameter} must be a non-empty ISO-8601 date")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
        parsed_date = parsed.date()
    except ValueError:
        try:
            parsed_date = date.fromisoformat(text)
        except ValueError as exc:
            _validation_error(request, parameter, f"{parameter} must be a valid ISO-8601 date")
    return parsed_date.isoformat()


def _ensure_date_order(request: Request, start: Optional[str], end: Optional[str]) -> None:
    if start and end and start > end:
        _validation_error(request, "date_range", "start_date must be on or before end_date")


class PpaContractResponse(BaseModel):
    """Response for PPA contract data with v2 enhancements."""
    contract_id: str = Field(..., description="Contract identifier")
    name: str = Field(..., description="Contract name")
    counterparty: str = Field(..., description="Counterparty")
    capacity_mw: float = Field(..., description="Capacity in MW")
    price_usd_mwh: float = Field(..., description="Price in USD/MWh")
    start_date: Optional[str] = Field(None, description="Start date")
    end_date: Optional[str] = Field(None, description="End date")
    meta: dict = Field(..., description="Metadata")


class PpaContractListResponse(BaseModel):
    """Response for PPA contracts list with v2 enhancements."""
    data: List[PpaContractResponse] = Field(..., description="List of contracts")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class PpaValuationResponse(BaseModel):
    """Response for PPA valuation data with v2 enhancements."""
    contract_id: str = Field(..., description="Contract identifier")
    valuation_date: Optional[str] = Field(None, description="Valuation date")
    period_start: Optional[str] = Field(None, description="Valuation period start")
    period_end: Optional[str] = Field(None, description="Valuation period end")
    metric: Optional[str] = Field(None, description="Metric name")
    present_value: Optional[float] = Field(None, description="Present value")
    cashflow: Optional[float] = Field(None, description="Cashflow for the period")
    irr: Optional[float] = Field(None, description="Internal rate of return")
    currency: str = Field(..., description="Currency")
    meta: dict = Field(..., description="Metadata")


class PpaValuationListResponse(BaseModel):
    """Response for PPA valuations list with v2 enhancements."""
    data: List[PpaValuationResponse] = Field(..., description="List of valuations")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/ppa/contracts", response_model=PpaContractListResponse)
async def list_ppa_contracts_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    counterparty_filter: Optional[str] = Query(None, description="Filter by counterparty"),
    service: PpaV2Service = Depends(get_ppa_service),
) -> PpaContractListResponse:
    """List PPA contracts with enhanced pagination and error handling.

    Maintainer note: ETag applied via standardized builder.
    """
    start_time = time.perf_counter()

    try:
        tenant_id_value = _validate_tenant_id(request, tenant_id)
        counterparty_value = _normalize_text(counterparty_filter)

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={
                "tenant_id": tenant_id_value,
                "counterparty_filter": counterparty_value,
            },
        )

        paginated_data = await service.list_contracts(
            tenant_id=tenant_id_value,
            offset=offset,
            limit=effective_limit + 1,
            counterparty_filter=counterparty_value,
        )

        has_more = len(paginated_data) > effective_limit
        if has_more:
            paginated_data = paginated_data[:effective_limit]
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={
                "tenant_id": tenant_id_value,
                "counterparty_filter": counterparty_value,
            },
        )
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={
                "tenant_id": tenant_id_value,
                "counterparty_filter": counterparty_value,
            },
        )

        request_url = request.url.include_query_params(tenant_id=tenant_id_value)
        if counterparty_value is None:
            request_url = request_url.remove_query_params("counterparty_filter")
        else:
            request_url = request_url.include_query_params(counterparty_filter=counterparty_value)

        meta_page, links = build_pagination_envelope(
            request_url=request_url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        contracts = []
        for contract_data in paginated_data:
            capacity_value = contract_data.get("capacity_mw", 0.0)
            price_value = contract_data.get("price_usd_mwh", 0.0)
            contracts.append(PpaContractResponse(
                contract_id=contract_data.get("contract_id", ""),
                name=contract_data.get("name", ""),
                counterparty=contract_data.get("counterparty", "unknown"),
                capacity_mw=float(capacity_value if capacity_value is not None else 0.0),
                price_usd_mwh=float(price_value if price_value is not None else 0.0),
                start_date=contract_data.get("start_date"),
                end_date=contract_data.get("end_date"),
                meta={"tenant_id": tenant_id_value}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id_value,
            "returned_count": len(contracts),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = PpaContractListResponse(
            data=contracts,
            meta=meta_out,
            links=links,
        )

        # Add ETag via standardized builder
        canonical_url = str(request_url.remove_query_params("cursor"))
        build = etag_response_builder(request, response, canonical_url=canonical_url)
        return build(result)

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list PPA contracts: {str(exc)}"
        )


@router.post("/ppa/valuate", response_model=PpaValuationResponse, status_code=201)
async def valuate_ppa_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    contract_id: str = Query(..., description="Contract ID to valuate"),
    valuation_date: str = Query(..., description="Valuation date"),
) -> PpaValuationResponse:
    """Valuate a PPA contract with enhanced validation and error handling."""
    start_time = time.perf_counter()

    try:
        # Not implemented yet: v2 valuation requires scenario context.
        raise NotImplementedException(
            detail="PPA valuation in v2 requires scenario context and is not available yet",
            context={"endpoint": "/v2/ppa/valuate"},
            request_id=get_request_id(),
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # dead code; kept for structure

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(status_code=500, detail=f"Failed to valuate PPA: {str(exc)}")


@router.get("/ppa/contracts/{contract_id}/valuations", response_model=PpaValuationListResponse)
async def list_ppa_valuations_v2(
    request: Request,
    response: Response,
    contract_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    start_date: Optional[str] = Query(None, description="Start date filter"),
    end_date: Optional[str] = Query(None, description="End date filter"),
    service: PpaV2Service = Depends(get_ppa_service),
) -> PpaValuationListResponse:
    """List PPA valuations with enhanced pagination and filtering.

    Maintainer note: ETag + Link headers applied via standardized builder.
    """
    start_time = time.perf_counter()

    try:
        tenant_id_value = _validate_tenant_id(request, tenant_id)
        contract_id_value = _validate_contract_id(request, contract_id)
        start_date_value = _normalize_date_query(request, "start_date", start_date)
        end_date_value = _normalize_date_query(request, "end_date", end_date)
        _ensure_date_order(request, start_date_value, end_date_value)

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={
                "tenant_id": tenant_id_value,
                "contract_id": contract_id_value,
                "start_date": start_date_value,
                "end_date": end_date_value,
            },
        )

        paginated_data = await service.list_valuations(
            tenant_id=tenant_id_value,
            contract_id=contract_id_value,
            offset=offset,
            limit=effective_limit + 1,
            start_date=start_date_value,
            end_date=end_date_value,
        )

        has_more = len(paginated_data) > effective_limit
        if has_more:
            paginated_data = paginated_data[:effective_limit]
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={
                "tenant_id": tenant_id_value,
                "contract_id": contract_id_value,
                "start_date": start_date_value,
                "end_date": end_date_value,
            },
        )
        prev_cursor = build_prev_cursor(
            offset=offset,
            limit=effective_limit,
            filters={
                "tenant_id": tenant_id_value,
                "contract_id": contract_id_value,
                "start_date": start_date_value,
                "end_date": end_date_value,
            },
        )

        request_url = request.url.include_query_params(tenant_id=tenant_id_value)
        if start_date_value is None:
            request_url = request_url.remove_query_params("start_date")
        else:
            request_url = request_url.include_query_params(start_date=start_date_value)
        if end_date_value is None:
            request_url = request_url.remove_query_params("end_date")
        else:
            request_url = request_url.include_query_params(end_date=end_date_value)

        meta_page, links = build_pagination_envelope(
            request_url=request_url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
            prev_cursor=prev_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        valuations = []
        for valuation_data in paginated_data:
            valuations.append(PpaValuationResponse(
                contract_id=contract_id_value,
                valuation_date=valuation_data.get("valuation_date"),
                period_start=valuation_data.get("period_start"),
                period_end=valuation_data.get("period_end"),
                metric=valuation_data.get("metric"),
                present_value=valuation_data.get("present_value"),
                cashflow=valuation_data.get("cashflow"),
                irr=valuation_data.get("irr"),
                currency=str(valuation_data.get("currency") or "USD"),
                meta={"tenant_id": tenant_id_value}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "contract_id": contract_id_value,
            "tenant_id": tenant_id_value,
            "start_date": start_date_value,
            "end_date": end_date_value,
            "returned_count": len(valuations),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = PpaValuationListResponse(
            data=valuations,
            meta=meta_out,
            links=links,
        )

        # Add ETag via standardized builder (with cursor)
        canonical_url = str(request_url.remove_query_params("cursor"))
        build = etag_cursor_response_builder(
            request,
            response,
            next_cursor=next_cursor,
            canonical_url=canonical_url,
        )
        return build(result)

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(status_code=500, detail=f"Failed to list PPA valuations: {str(exc)}")
