"""v1 Scenarios API with backward compatibility.

This module provides the v1 implementation of the scenarios API with:
- Backward compatible response formats
- Legacy pagination support (offset-based)
- Simplified error handling
- Basic observability

Notes:
- Base path: `/v1/*` (see app wiring in src/aurum/api/app.py)
- Migration guidance to v2 endpoints: docs/migration-guide.md
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import StreamingResponse

from ..telemetry.context import get_request_id, get_user_id, log_structured
from ..services import ScenarioService
from ..scenario_models import (
    CreateScenarioRequest,
    ScenarioResponse,
    ScenarioData,
    ScenarioListResponse,
    ScenarioRunOptions,
    ScenarioRunResponse,
    ScenarioRunData,
    ScenarioRunListResponse,
    ScenarioOutputResponse,
    ScenarioOutputPoint,
    ScenarioOutputFilter,
    ScenarioMetricLatestResponse,
    ScenarioMetricLatest,
    ScenarioOutputListResponse,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    BulkScenarioRunDuplicate,
    ScenarioRunBulkResponse,
)
from ..scenarios.exceptions import ValidationException, NotFoundException, ForbiddenException
from ..container import get_service
from ..scenarios.routes import _resolve_tenant, _resolve_tenant_optional
from ..http import (
    decode_cursor,
    encode_cursor,
    normalize_cursor_input,
)
from ..scenarios.feature_flags import (
    ScenarioOutputFeature,
    require_scenario_output_feature,
    check_scenario_output_feature,
    enforce_scenario_output_limits,
)
from ..auth import require_permission, Permission

router = APIRouter(prefix="/v1", tags=["scenarios"])


@router.get("/scenarios", response_model=ScenarioListResponse)
async def list_scenarios_v1(
    request: Request,
    response: Response,
    tenant_id: Optional[str] = Query(None, description="Filter by tenant"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    name: Optional[str] = Query(None, description="Filter by scenario name (case-insensitive substring match)"),
    tag: Optional[str] = Query(None, description="Filter by tag"),
    created_after: Optional[datetime] = Query(None, description="Return scenarios created at or after this timestamp (ISO 8601)"),
    created_before: Optional[datetime] = Query(None, description="Return scenarios created at or before this timestamp (ISO 8601)"),
) -> ScenarioListResponse:
    """List scenarios with optional filtering (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Handle cursor-based pagination (offset is deprecated)
        effective_offset = offset or 0
        cursor_token = cursor or since_cursor
        if cursor_token:
            payload = decode_cursor(cursor_token)
            effective_offset, _cursor_after = normalize_cursor_input(payload.values)
        elif offset is not None:
            # Log deprecation warning for offset usage
            log_structured(
                "warning",
                "deprecated_offset_pagination_used",
                tenant_id=tenant_id,
                status=status,
                offset=offset,
                user_id=get_user_id(),
                request_id=get_request_id()
            )

        if created_after and created_before and created_after > created_before:
            raise ValidationException("created_after must be before created_before")

        service = ScenarioService()
        scenarios, total, meta = await service.list_scenarios(
            tenant_id=tenant_id,
            status=status,
            limit=limit,
            offset=effective_offset,
            name_contains=name,
            tag=tag,
            created_after=created_after,
            created_before=created_before,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Generate next cursor if there are more results
        next_cursor = None
        if len(scenarios) == limit:
            next_cursor = encode_cursor({"offset": effective_offset + limit})

        model = ScenarioListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(scenarios),
                "total": total,
                "offset": effective_offset,
                "limit": limit,
                "next_cursor": next_cursor,
            },
            data=scenarios,
        )

        return model

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list scenarios: {str(exc)}"
        ) from exc


@router.post("/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario_v1(
    request: Request,
    response: Response,
    scenario_data: CreateScenarioRequest,
) -> ScenarioResponse:
    """Create a new scenario (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant(request, scenario_data.tenant_id)

    # Require scenarios write permission
    require_permission(principal, Permission.SCENARIOS_WRITE, resolved_tenant)

    start_time = time.perf_counter()

    try:
        service = ScenarioService()
        scenario = await service.create_scenario(scenario_data.model_dump())

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=scenario,
        )

        return model

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scenario: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def get_scenario_v1(
    request: Request,
    response: Response,
    scenario_id: str,
) -> ScenarioResponse:
    """Get scenario by ID (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID format
        try:
            UUID(scenario_id)
        except ValueError:
            raise ValidationException(
                field="scenario_id",
                message="Invalid scenario ID format",
                request_id=get_request_id()
            )

        service = ScenarioService()
        scenario = await service.get_scenario(scenario_id)

        if not scenario:
            raise NotFoundException(
                resource_type="scenario",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=scenario,
        )

        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario: {str(exc)}"
        ) from exc


@router.delete("/scenarios/{scenario_id}", status_code=204)
async def delete_scenario_v1(
    request: Request,
    scenario_id: str,
) -> Response:
    """Delete scenario by ID (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios delete permission
    require_permission(principal, Permission.SCENARIOS_DELETE, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID format
        try:
            UUID(scenario_id)
        except ValueError:
            raise ValidationException(
                field="scenario_id",
                message="Invalid scenario ID format",
                request_id=get_request_id()
            )

        service = ScenarioService()
        success = await service.delete_scenario(scenario_id)

        if not success:
            raise NotFoundException(
                resource_type="scenario",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000
        print(f"Deleted scenario {scenario_id} in {query_time_ms:.2f}ms")
        from fastapi import Response
        return Response(status_code=204)

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete scenario: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
async def list_scenario_runs_v1(
    request: Request,
    response: Response,
    scenario_id: str,
    limit: int = Query(20, ge=1, le=100),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    state: Optional[str] = Query(None, pattern="^(QUEUED|RUNNING|SUCCEEDED|FAILED|CANCELLED)$", description="Filter runs by state"),
    created_after: Optional[datetime] = Query(None, description="Return runs queued at or after this timestamp (ISO 8601)"),
    created_before: Optional[datetime] = Query(None, description="Return runs queued at or before this timestamp (ISO 8601)"),
) -> ScenarioRunListResponse:
    """List runs for a specific scenario (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID format
        try:
            UUID(scenario_id)
        except ValueError:
            raise ValidationException(
                field="scenario_id",
                message="Invalid scenario ID format",
                request_id=get_request_id()
            )

        # Handle cursor-based pagination (offset is deprecated)
        effective_offset = offset or 0
        cursor_token = cursor or since_cursor
        if cursor_token:
            payload = decode_cursor(cursor_token)
            effective_offset, _cursor_after = normalize_cursor_input(payload.values)
        elif offset is not None:
            # Log deprecation warning for offset usage
            log_structured(
                "warning",
                "deprecated_offset_pagination_used",
                scenario_id=scenario_id,
                offset=offset,
                user_id=get_user_id(),
                request_id=get_request_id()
            )

        if created_after and created_before and created_after > created_before:
            raise ValidationException(
                field="created_after",
                message="created_after must be before created_before",
                request_id=get_request_id()
            )

        service = ScenarioService()
        runs, total, meta = await service.list_scenario_runs(
            scenario_id=scenario_id,
            limit=limit,
            offset=effective_offset,
            state=state,
            created_after=created_after,
            created_before=created_before,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Generate next cursor if there are more results
        next_cursor = None
        if len(runs) == limit:
            next_cursor = encode_cursor({"offset": effective_offset + limit})

        model = ScenarioRunListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(runs),
                "total": total,
                "offset": effective_offset,
                "limit": limit,
                "next_cursor": next_cursor,
            },
            data=runs,
        )

        return model

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list scenario runs: {str(exc)}"
        ) from exc


@router.post("/scenarios/{scenario_id}/run", response_model=ScenarioRunResponse, status_code=202)
async def create_scenario_run_v1(
    request: Request,
    response: Response,
    scenario_id: str,
    run_options: ScenarioRunOptions,
) -> ScenarioRunResponse:
    """Create and start a new scenario run (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios run permission
    require_permission(principal, Permission.SCENARIOS_RUN, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID format
        try:
            UUID(scenario_id)
        except ValueError:
            raise ValidationException(
                field="scenario_id",
                message="Invalid scenario ID format",
                request_id=get_request_id()
            )

        service = ScenarioService()
        run = await service.create_scenario_run(
            scenario_id=scenario_id,
            options=run_options.model_dump()
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

        return model

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scenario run: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}/runs/{run_id}", response_model=ScenarioRunResponse)
async def get_scenario_run_v1(
    request: Request,
    response: Response,
    scenario_id: str,
    run_id: str,
) -> ScenarioRunResponse:
    """Get scenario run by ID (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID formats
        for param_name, param_value in [("scenario_id", scenario_id), ("run_id", run_id)]:
            try:
                UUID(param_value)
            except ValueError:
                raise ValidationException(
                    field=param_name,
                    message=f"Invalid {param_name} format",
                    request_id=get_request_id()
                )

        service = ScenarioService()
        run = await service.get_scenario_run(scenario_id, run_id)

        if not run:
            raise NotFoundException(
                resource_type="scenario_run",
                resource_id=run_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario run: {str(exc)}"
        ) from exc


@router.post("/scenarios/runs/{run_id}/cancel", response_model=ScenarioRunResponse)
async def cancel_scenario_run_v1(
    request: Request,
    response: Response,
    run_id: str,
) -> ScenarioRunResponse:
    """Cancel a running scenario run (v1 compatibility)."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios delete permission
    require_permission(principal, Permission.SCENARIOS_DELETE, resolved_tenant)

    start_time = time.perf_counter()

    try:
        # Validate UUID format
        try:
            UUID(run_id)
        except ValueError:
            raise ValidationException(
                field="run_id",
                message="Invalid run ID format",
                request_id=get_request_id()
            )

        service = ScenarioService()
        run = await service.cancel_scenario_run(run_id)

        if not run:
            raise NotFoundException(
                resource_type="scenario_run",
                resource_id=run_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel scenario run: {str(exc)}"
        ) from exc
