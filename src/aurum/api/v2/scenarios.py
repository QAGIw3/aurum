"""v2 Scenarios API with enhanced features.

This module provides the v2 implementation of the scenarios API with:
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
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import BaseModel, Field

from ..http import respond_with_etag
from ..container import get_service
from ..async_service import AsyncScenarioService
from ..scenario_models import (
    ScenarioCreateRequest,
    ScenarioData,
    ScenarioResponse,
    ScenarioRunCreateRequest,
    ScenarioRunData,
    ScenarioRunResponse,
    ScenarioRunStatus,
)
from ...telemetry.context import get_request_id
from .pagination import (
    build_next_cursor,
    build_pagination_envelope,
    resolve_pagination,
)


async def get_scenario_service() -> AsyncScenarioService:
    """Provide AsyncScenarioService compatible with existing awaited usage."""
    return get_service(AsyncScenarioService)

router = APIRouter(prefix="/v2", tags=["scenarios"])
# NOTE: the prefix keeps paths aligned with the app wiring so tests can mount
# this router in isolation without replicating the entire application factory.


class ScenarioListResponse(BaseModel):
    """Response for listing scenarios with v2 enhancements."""
    data: List[ScenarioData] = Field(..., description="List of scenarios")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


class ScenarioRunListResponse(BaseModel):
    """Response for listing scenario runs with v2 enhancements."""
    data: List[ScenarioRunData] = Field(..., description="List of scenario runs")
    meta: dict = Field(..., description="Pagination and metadata")
    links: dict = Field(..., description="Pagination links")


@router.get("/scenarios", response_model=ScenarioListResponse)
async def list_scenarios_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    name_filter: Optional[str] = Query(None, description="Filter by scenario name"),
) -> ScenarioListResponse:
    """List scenarios with enhanced pagination and error handling."""
    start_time = time.perf_counter()

    try:
        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=10,
            filters={
                "tenant_id": tenant_id,
                "name_filter": name_filter,
            },
        )

        service = get_service(AsyncScenarioService)
        scenarios, total, _meta = await service.list_scenarios(
            tenant_id=tenant_id,
            limit=effective_limit,
            offset=offset,
            name_contains=name_filter,
        )

        total_count = total if total is not None else offset + len(scenarios)
        has_more = (offset + len(scenarios)) < total_count
        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=has_more,
            filters={
                "tenant_id": tenant_id,
                "name_filter": name_filter,
            },
        )

        base_meta, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=total,
            next_cursor=next_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        meta: Dict[str, object] = {
            **base_meta,
            "request_id": get_request_id(),
            "tenant_id": tenant_id,
            "returned_count": len(scenarios),
            "has_more": has_more,
            "processing_time_ms": round(duration_ms, 2),
        }

        result = ScenarioListResponse(
            data=scenarios,
            meta=meta,
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
                "detail": f"Failed to list scenarios: {str(exc)}",
                "instance": "/v2/scenarios",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.post("/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario_v2(
    request: Request,
    response: Response,
    tenant_id: str = Query(..., description="Tenant ID"),
    scenario: ScenarioCreateRequest,
) -> ScenarioResponse:
    """Create a scenario with enhanced validation and error handling."""
    start_time = time.perf_counter()

    try:
        # Get scenario service
        service = await get_scenario_service()

        # Create scenario
        created_scenario = await service.create_scenario(scenario)

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = ScenarioResponse(
            **created_scenario.model_dump(),
            meta={
                "request_id": get_request_id(),
                "created_at": created_scenario.created_at,
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
                "detail": f"Failed to create scenario: {str(exc)}",
                "instance": "/v2/scenarios",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def get_scenario_v2(
    request: Request,
    response: Response,
    scenario_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
) -> ScenarioResponse:
    """Get a scenario with enhanced error handling."""
    start_time = time.perf_counter()

    try:
        # Get scenario service
        service = await get_scenario_service()

        # Get scenario
        scenario = await service.get_scenario(scenario_id, tenant_id)

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = ScenarioResponse(
            **scenario.model_dump(),
            meta={
                "request_id": get_request_id(),
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
                "detail": f"Failed to get scenario: {str(exc)}",
                "instance": f"/v2/scenarios/{scenario_id}",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.post("/scenarios/{scenario_id}/runs", response_model=ScenarioRunResponse, status_code=201)
async def create_scenario_run_v2(
    request: Request,
    response: Response,
    scenario_id: str,
    run: ScenarioRunCreateRequest,
    tenant_id: str = Query(..., description="Tenant ID"),
) -> ScenarioRunResponse:
    """Create a scenario run with enhanced validation and idempotency."""
    start_time = time.perf_counter()

    try:
        # Get scenario service
        service = await get_scenario_service()

        # Create scenario run
        created_run = await service.create_scenario_run(scenario_id, run, tenant_id)

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with metadata
        result = ScenarioRunResponse(
            **created_run.model_dump(),
            meta={
                "request_id": get_request_id(),
                "created_at": created_run.created_at,
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
                "detail": f"Failed to create scenario run: {str(exc)}",
                "instance": f"/v2/scenarios/{scenario_id}/runs",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )


@router.get("/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
async def list_scenario_runs_v2(
    request: Request,
    response: Response,
    scenario_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(10, ge=1, le=100, description="Maximum number of items to return"),
    status_filter: Optional[ScenarioRunStatus] = Query(None, description="Filter by run status"),
) -> ScenarioRunListResponse:
    """List scenario runs with enhanced pagination and filtering."""
    start_time = time.perf_counter()

    try:
        # Get scenario service
        service = await get_scenario_service()

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=limit,
            filters={"scenario_id": scenario_id, "tenant_id": tenant_id, "status_filter": status_filter},
        )

        runs = await service.list_scenario_runs(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            offset=offset,
            limit=effective_limit,
            status_filter=status_filter,
        )

        next_cursor = build_next_cursor(
            offset=offset,
            limit=effective_limit,
            has_more=len(runs) == effective_limit,
            filters={"scenario_id": scenario_id, "tenant_id": tenant_id, "status_filter": status_filter},
        )
        base_meta, links = build_pagination_envelope(
            request_url=request.url,
            offset=offset,
            limit=effective_limit,
            total=None,
            next_cursor=next_cursor,
        )

        duration_ms = (time.perf_counter() - start_time) * 1000

        # Create response with enhanced metadata
        meta_out = dict(base_meta)
        meta_out.update({
            "request_id": get_request_id(),
            "scenario_id": scenario_id,
            "tenant_id": tenant_id,
            "returned_count": len(runs),
            "processing_time_ms": round(duration_ms, 2),
        })

        result = ScenarioRunListResponse(
            data=runs,
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
                "detail": f"Failed to list scenario runs: {str(exc)}",
                "instance": f"/v2/scenarios/{scenario_id}/runs",
                "request_id": get_request_id(),
                "processing_time_ms": round(duration_ms, 2)
            }
        )
