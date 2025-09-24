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

router = APIRouter(prefix="/v2", tags=["ppa"])


class PpaContractResponse(BaseModel):
    """Response for PPA contract data with v2 enhancements."""
    contract_id: str = Field(..., description="Contract identifier")
    name: str = Field(..., description="Contract name")
    counterparty: str = Field(..., description="Counterparty")
    capacity_mw: float = Field(..., description="Capacity in MW")
    price_usd_mwh: float = Field(..., description="Price in USD/MWh")
    start_date: str = Field(..., description="Start date")
    end_date: str = Field(..., description="End date")
    meta: dict = Field(..., description="Metadata")


class PpaContractListResponse(BaseModel):
    """Response for PPA contracts list with v2 enhancements."""
    data: List[PpaContractResponse] = Field(..., description="List of contracts")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class PpaValuationResponse(BaseModel):
    """Response for PPA valuation data with v2 enhancements."""
    contract_id: str = Field(..., description="Contract identifier")
    valuation_date: str = Field(..., description="Valuation date")
    present_value: float = Field(..., description="Present value")
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
) -> PpaContractListResponse:
    """List PPA contracts with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"counterparty_filter": counterparty_filter},
        )

        from ..ppa_v2_service import get_ppa_service
        svc = await get_ppa_service()
        paginated_data = await svc.list_contracts(
            tenant_id=tenant_id,
            offset=offset,
            limit=effective_limit,
            counterparty_filter=counterparty_filter,
        )

        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"counterparty_filter": counterparty_filter},
        )
        from .pagination import build_prev_cursor
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"counterparty_filter": counterparty_filter})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
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
            contracts.append(PpaContractResponse(
                contract_id=contract_data.get("contract_id", ""),
                name=contract_data.get("name", ""),
                counterparty=contract_data.get("counterparty", "unknown"),
                capacity_mw=float(contract_data.get("capacity_mw", 0.0)),
                price_usd_mwh=float(contract_data.get("price_usd_mwh", 0.0)),
                start_date=str(contract_data.get("start_date", "")),
                end_date=str(contract_data.get("end_date", "")),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(contracts),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = PpaContractListResponse(
            data=contracts,
            meta=meta_out,
            links=links,
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
                "detail": f"Failed to list PPA contracts: {str(exc)}",
                "instance": "/v2/ppa/contracts",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
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
        raise HTTPException(status_code=501, detail={
            "type": "not_implemented",
            "title": "Valuation not implemented",
            "detail": "PPA valuation in v2 requires scenario context and is not available yet",
            "instance": "/v2/ppa/valuate",
        })

        duration_ms = (time.perf_counter() - start_time) * 1000

        # dead code; kept for structure

    except HTTPException:
        raise
    except Exception as exc:
        duration_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "type": "internal_error",
                "title": "Internal server error",
                "detail": f"Failed to valuate PPA: {str(exc)}",
                "instance": "/v2/ppa/valuate",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


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
) -> PpaValuationListResponse:
    """List PPA valuations with enhanced pagination and filtering."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"contract_id": contract_id, "start_date": start_date, "end_date": end_date},
        )

        from ..ppa_v2_service import get_ppa_service
        svc = await get_ppa_service()
        paginated_data = await svc.list_valuations(
            tenant_id=tenant_id,
            contract_id=contract_id,
            offset=offset,
            limit=effective_limit,
            start_date=start_date,
            end_date=end_date,
        )

        has_more = len(paginated_data) == effective_limit
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={"contract_id": contract_id, "start_date": start_date, "end_date": end_date},
        )
        prev_cursor = build_prev_cursor(offset=offset, limit=effective_limit, filters={"contract_id": contract_id, "start_date": start_date, "end_date": end_date})
        meta_page, links = build_pagination_envelope(
            request_url=request.url,
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
                contract_id=contract_id,
                valuation_date=str(valuation_data.get("valuation_date")),
                present_value=float(valuation_data.get("present_value", 0.0)),
                currency=str(valuation_data.get("currency", "USD")),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        meta_out = dict(meta_page)
        meta_out.update({
            "request_id": get_request_id(),
            "contract_id": contract_id,
            "tenant_id": tenant_id,
            "start_date": start_date,
            "end_date": end_date,
            "returned_count": len(valuations),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = PpaValuationListResponse(
            data=valuations,
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
                "detail": f"Failed to list PPA valuations: {str(exc)}",
                "instance": f"/v2/ppa/contracts/{contract_id}/valuations",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
