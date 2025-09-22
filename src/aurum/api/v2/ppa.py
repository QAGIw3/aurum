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
from ...telemetry.context import get_request_id

router = APIRouter()


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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": "/v2/ppa/contracts"
                    }
                )

        # Get PPA contracts service (placeholder - would integrate with actual service)
        from ..routes import _ppa_contracts_data
        contracts_data = _ppa_contracts_data(counterparty_filter)

        # Apply pagination
        total_count = len(contracts_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = contracts_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        contracts = []
        for contract_data in paginated_data:
            contracts.append(PpaContractResponse(
                contract_id=contract_data["contract_id"],
                name=contract_data["name"],
                counterparty=contract_data["counterparty"],
                capacity_mw=contract_data["capacity_mw"],
                price_usd_mwh=contract_data["price_usd_mwh"],
                start_date=contract_data["start_date"],
                end_date=contract_data["end_date"],
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        result = PpaContractListResponse(
            data=contracts,
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "total_count": total_count,
                "returned_count": len(contracts),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
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
        # Get PPA valuation service (placeholder - would integrate with actual service)
        from ..routes import _valuate_ppa_data
        valuation_data = _valuate_ppa_data(contract_id, valuation_date)

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = PpaValuationResponse(
            contract_id=contract_id,
            valuation_date=valuation_date,
            present_value=valuation_data["present_value"],
            currency=valuation_data.get("currency", "USD"),
            meta={
                "request_id": get_request_id(),
                "tenant_id": tenant_id,
                "processing_time_ms": round(duration_ms, 2),
                "version": "v2"
            }
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
        # Parse cursor if provided
        offset = 0
        if cursor:
            try:
                import base64
                import json
                cursor_data = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                offset = cursor_data.get("offset", 0)
            except Exception:
                raise HTTPException(
                    status_code=400,
                    detail={
                        "type": "invalid_cursor",
                        "title": "Invalid cursor format",
                        "detail": "The provided cursor is not valid",
                        "instance": f"/v2/ppa/contracts/{contract_id}/valuations"
                    }
                )

        # Get PPA valuations service (placeholder - would integrate with actual service)
        from ..routes import _ppa_valuations_data
        valuations_data = _ppa_valuations_data(contract_id, start_date, end_date)

        # Apply pagination
        total_count = len(valuations_data)
        start_idx = offset
        end_idx = offset + limit
        paginated_data = valuations_data[start_idx:end_idx]

        # Create next cursor
        next_cursor = None
        if end_idx < total_count:
            next_offset = end_idx
            cursor_data = {"offset": next_offset}
            import base64
            import json
            next_cursor = base64.urlsafe_b64encode(
                json.dumps(cursor_data).encode()
            ).decode()

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        valuations = []
        for valuation_data in paginated_data:
            valuations.append(PpaValuationResponse(
                contract_id=contract_id,
                valuation_date=valuation_data["valuation_date"],
                present_value=valuation_data["present_value"],
                currency=valuation_data.get("currency", "USD"),
                meta={"tenant_id": tenant_id}
            ))

        # Create response with enhanced metadata
        result = PpaValuationListResponse(
            data=valuations,
            meta={
                "request_id": get_request_id(),
                "contract_id": contract_id,
                "tenant_id": tenant_id,
                "start_date": start_date,
                "end_date": end_date,
                "total_count": total_count,
                "returned_count": len(valuations),
                "has_more": next_cursor is not None,
                "cursor": cursor,
                "next_cursor": next_cursor,
                "processing_time_ms": round(duration_ms, 2),
            },
            links={
                "self": str(request.url),
                "next": f"{request.url}&cursor={next_cursor}" if next_cursor else None,
            }
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
