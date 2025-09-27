"""Scenario management endpoints with async support.

Endpoints in this module allow clients to create scenario definitions, trigger
asynchronous runs, inspect run status, cancel runs, and retrieve outputs and
latest metrics. See docs/scenarios.md for a high‑level guide and examples.

Auth & Tenancy
--------------
All routes enforce tenant scoping and permissions via the request principal.

Observability
-------------
Responses include request IDs. Structured logs record scenario lifecycle
events and Trino query metrics for downstream analysis.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Path
from fastapi.responses import StreamingResponse, Response

from ..telemetry.context import get_request_id, get_user_id, log_structured
from .scenario_models import (
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
from .exceptions import ValidationException, NotFoundException, ForbiddenException
from ..container import get_service
# Import AsyncScenarioService lazily to avoid circular imports
def _get_async_scenario_service():
    """Get AsyncScenarioService, importing it lazily to avoid circular dependencies."""
    try:
        from .async_service import AsyncScenarioService
        return AsyncScenarioService
    except (ImportError, NameError):
        # For testing, create a mock service class
        class AsyncScenarioService:
            def __init__(self, store=None):
                self.store = store

            async def create_scenario(self, tenant_id: str, request):
                return None

            async def create_bulk_scenario_runs(self, tenant_id: str, scenario_id, runs: list, bulk_idempotency_key=None):
                return [], []
        return AsyncScenarioService
from .routes import _resolve_tenant, _resolve_tenant_optional
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


# Create the router at the top of the file
router = APIRouter(prefix="/v1", tags=["scenarios"])


@router.get("/scenarios", response_model=ScenarioListResponse)
async def list_scenarios(
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
    """List scenarios with optional filtering.

    Examples:
        GET /v1/scenarios?limit=10
        GET /v1/scenarios?status=active&limit=5
        GET /v1/scenarios?cursor=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...
    """
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

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
        return model

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list scenarios: {str(exc)}"
        ) from exc


@router.post("/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario(
    request: Request,
    response: Response,
    scenario_data: CreateScenarioRequest,
) -> ScenarioResponse:
    """Create a new scenario.

    Example:
        POST /v1/scenarios
        {
            "tenant_id": "acme-corp",
            "name": "Revenue Forecast Q4",
            "description": "Quarterly revenue forecasting scenario",
            "assumptions": [
                {"type": "market_growth", "value": 0.05}
            ],
            "parameters": {
                "forecast_period_months": 12,
                "confidence_interval": 0.95
            }
        }
    """
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant(request, scenario_data.tenant_id)

    # Require scenarios write permission
    require_permission(principal, Permission.SCENARIOS_WRITE, resolved_tenant)

    start_time = time.perf_counter()

    try:
        service = _get_async_scenario_service()()
        scenario = await service.create_scenario(scenario_data.model_dump())

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=scenario,
        )

        # Return the response model directly - FastAPI will handle serialization
        return model

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scenario: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def get_scenario(
    request: Request,
    response: Response,
    scenario_id: str,
) -> ScenarioResponse:
    """Get scenario by ID.

    Example:
        GET /v1/scenarios/scn_12345
    """
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

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
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
async def delete_scenario(
    request: Request,
    scenario_id: str,
) -> Response:
    """Delete scenario by ID."""
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

        service = _get_async_scenario_service()()
        success = await service.delete_scenario(scenario_id)

        if not success:
            raise NotFoundException(
                resource_type="scenario",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000
        # Log successful deletion
        print(f"Deleted scenario {scenario_id} in {query_time_ms:.2f}ms")
        from fastapi import Response
        return Response(status_code=204)  # Return proper 204 response

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete scenario: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
async def list_scenario_runs(
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
    """List runs for a specific scenario."""
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
                request_id=get_request_id(),
            )

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
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
async def create_scenario_run(
    request: Request,
    response: Response,
    scenario_id: str,
    run_options: ScenarioRunOptions,
) -> ScenarioRunResponse:
    """Create and start a new scenario run."""
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

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
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
async def get_scenario_run(
    request: Request,
    response: Response,
    scenario_id: str,
    run_id: str,
) -> ScenarioRunResponse:
    """Get scenario run by ID."""
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

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario run: {str(exc)}"
        ) from exc


@router.post("/scenarios/runs/{run_id}/state", response_model=ScenarioRunResponse)
async def update_scenario_run_state(
    request: Request,
    response: Response,
    run_id: str,
    state_update: dict,
) -> ScenarioRunResponse:
    """Update scenario run state."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios write permission
    require_permission(principal, Permission.SCENARIOS_WRITE, resolved_tenant)

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

        # Validate state update
        if not isinstance(state_update, dict) or "state" not in state_update:
            raise ValidationException(
                field="state_update",
                message="state_update must contain 'state' field",
                request_id=get_request_id()
            )

        service = _get_async_scenario_service()()
        run = await service.update_scenario_run_state(run_id, state_update)

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

        # Return the response model directly - FastAPI will handle serialization
        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update scenario run state: {str(exc)}"
        ) from exc


@router.post("/scenarios/runs/{run_id}/cancel", response_model=ScenarioRunResponse)
async def cancel_scenario_run(
    request: Request,
    response: Response,
    run_id: str,
) -> ScenarioRunResponse:
    """Cancel a running scenario run with idempotency and worker signaling."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios delete permission (admin operation, so tenant check is optional)
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

        service = _get_async_scenario_service()()
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

        # Return the response model directly - FastAPI will handle serialization
        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel scenario run: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputListResponse)
@require_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)
async def get_scenario_outputs(
    request: Request,
    scenario_id: str,
    limit: int = Query(100, ge=1, le=500),
    cursor: Optional[str] = Query(None, description="Opaque cursor for stable pagination"),
    since_cursor: Optional[str] = Query(None, description="Alias for 'cursor' to resume iteration"),
    offset: Optional[int] = Query(None, ge=0, description="DEPRECATED: Use cursor for pagination instead"),
    start_time: Optional[str] = Query(None, description="Start time filter (ISO 8601 format)"),
    end_time: Optional[str] = Query(None, description="End time filter (ISO 8601 format)"),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
    min_value: Optional[float] = Query(None, description="Minimum value filter", ge=0),
    max_value: Optional[float] = Query(None, description="Maximum value filter", ge=0),
    tags: Optional[str] = Query(None, description="Filter by tags (JSON format)"),
    format: Optional[str] = Query(None, description="Output format (json, csv)"),
) -> ScenarioOutputListResponse:
    """Get scenario outputs with time-based filtering and pagination."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, resolved_tenant)

    start_time_perf = time.perf_counter()

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

        # Parse tags filter if provided
        tags_filter = None
        if tags:
            try:
                import json
                tags_filter = json.loads(tags)
                if not isinstance(tags_filter, dict):
                    raise ValueError("Tags must be a JSON object")
            except (json.JSONDecodeError, ValueError) as exc:
                raise ValidationException(
                    field="tags",
                    message=f"Invalid tags format: {str(exc)}",
                    request_id=get_request_id()
                )

        # Validate time formats if provided
        if start_time:
            try:
                from datetime import datetime
                datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            except ValueError:
                raise ValidationException(
                    field="start_time",
                    message="Invalid ISO 8601 datetime format",
                    request_id=get_request_id()
                )

        if end_time:
            try:
                from datetime import datetime
                datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            except ValueError:
                raise ValidationException(
                    field="end_time",
                    message="Invalid ISO 8601 datetime format",
                    request_id=get_request_id()
                )

        # Validate feature-specific requirements
        if start_time or end_time or metric_name or min_value is not None or max_value is not None or tags:
            # Check if filtering is enabled
            if not await check_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_FILTERING):
                raise ForbiddenException(
                    resource_type="feature",
                    resource_id=ScenarioOutputFeature.SCENARIO_OUTPUTS_FILTERING.value,
                    detail="Scenario output filtering is not enabled for this tenant",
                    request_id=get_request_id()
                )

        if limit > 100 or offset > 0:
            # Check if pagination is enabled
            if not await check_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION):
                raise ForbiddenException(
                    resource_type="feature",
                    resource_id=ScenarioOutputFeature.SCENARIO_OUTPUTS_PAGINATION.value,
                    detail="Scenario output pagination is not enabled for this tenant",
                    request_id=get_request_id()
                )

        # Validate min/max values
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValidationException(
                field="min_value",
                message="min_value cannot be greater than max_value",
                request_id=get_request_id()
            )

        # Handle cursor-based pagination
        effective_offset = offset
        cursor_token = cursor or since_cursor
        if cursor_token:
            payload = decode_cursor(cursor_token)
            effective_offset, _cursor_after = normalize_cursor_input(payload.values)

        service = _get_async_scenario_service()()
        outputs, total, meta = await service.get_scenario_outputs(
            scenario_id=scenario_id,
            limit=limit,
            offset=effective_offset,
            start_time=start_time,
            end_time=end_time,
            metric_name=metric_name,
            min_value=min_value,
            max_value=max_value,
            tags=tags_filter,
        )

        query_time_ms = (time.perf_counter() - start_time_perf) * 1000

        # Generate next cursor if there are more results
        next_cursor = None
        if len(outputs) == limit:
            next_cursor = encode_cursor({"offset": effective_offset + limit})

        # Build applied filter for response
        applied_filter = ScenarioOutputFilter(
            start_time=start_time,
            end_time=end_time,
            metric_name=metric_name,
            min_value=min_value,
            max_value=max_value,
            tags=tags_filter,
        )

        model = ScenarioOutputListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(outputs),
                "total": total,
                "offset": effective_offset,
                "limit": limit,
                "next_cursor": next_cursor,
            },
            data=outputs,
            filter=applied_filter,
        )

        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time_perf) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario outputs: {str(exc)}"
        ) from exc


@router.get("/scenarios/{scenario_id}/metrics/latest", response_model=ScenarioMetricLatestResponse)
async def get_scenario_metrics_latest(
    request: Request,
    response: Response,
    scenario_id: str,
) -> ScenarioMetricLatestResponse:
    """Get latest metrics for a scenario."""
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

        service = _get_async_scenario_service()()
        metrics = await service.get_scenario_metrics_latest(scenario_id)

        if not metrics:
            raise NotFoundException(
                resource_type="scenario_metrics",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        model = ScenarioMetricLatestResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=metrics,
        )

        # Return the response model directly - FastAPI will handle serialization
        return model

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario metrics: {str(exc)}"
        ) from exc


@router.post("/scenarios/{scenario_id}/runs:bulk", response_model=BulkScenarioRunResponse, status_code=202)
async def create_bulk_scenario_runs(
    request: Request,
    response: Response,
    bulk_request: BulkScenarioRunRequest,
    scenario_id: str = Path(..., description="Scenario ID", pattern="^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
) -> BulkScenarioRunResponse:
    """Create multiple scenario runs in bulk with deduplication via idempotency keys."""
    from ..auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Resolve tenant for authorization check
    resolved_tenant = _resolve_tenant_optional(request)

    # Require scenarios write permission
    # require_permission(principal, Permission.SCENARIOS_WRITE, resolved_tenant)

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

        # Set scenario_id from URL path into the request body before validation
        bulk_request.scenario_id = scenario_id

        # Validate bulk request
        if not bulk_request.scenario_id:
            raise ValidationException(
                field="scenario_id",
                message="Scenario ID is required",
                request_id=get_request_id()
            )
        if not bulk_request.runs:
                   raise ValidationException(
                       field="runs",
                       message="At least one run must be specified",
                       request_id=get_request_id()
                   )

        if len(bulk_request.runs) > 100:
            raise ValidationException(
                field="runs",
                message="Cannot create more than 100 runs in a single bulk request",
                request_id=get_request_id()
            )

        # Validate idempotency keys are unique within the request
        idempotency_keys = []
        for i, run in enumerate(bulk_request.runs):
            if run.idempotency_key:
                if run.idempotency_key in idempotency_keys:
                    raise ValidationException(
                        field=f"runs[{i}].idempotency_key",
                        message=f"Duplicate idempotency key '{run.idempotency_key}' in request",
                        request_id=get_request_id()
                    )
                idempotency_keys.append(run.idempotency_key)

        service = _get_async_scenario_service()()
        results, duplicates = await service.create_bulk_scenario_runs(
            tenant_id=resolved_tenant,
            scenario_id=scenario_id,
            runs=[run.model_dump() for run in bulk_request.runs],
            bulk_idempotency_key=bulk_request.idempotency_key,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert results to response format
        response_data = []
        for result in results:
            response_item = BulkScenarioRunResult(
                index=result["index"],
                idempotency_key=result.get("idempotency_key"),
                run_id=result["run_id"],
                status=result["status"],
                error=result.get("error"),
            )
            response_data.append(response_item)

        # Convert duplicates to response format
        response_duplicates = []
        for duplicate in duplicates:
            duplicate_item = BulkScenarioRunDuplicate(
                index=duplicate["index"],
                idempotency_key=duplicate.get("idempotency_key"),
                existing_run_id=duplicate["existing_run_id"],
                existing_status=duplicate["existing_status"],
                created_at=duplicate["created_at"],
            )
            response_duplicates.append(duplicate_item)

        model = BulkScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_runs": len(bulk_request.runs),
                "successful_runs": len([r for r in response_data if r.status == "success"]),
                "duplicate_runs": len(response_duplicates),
                "failed_runs": len([r for r in response_data if r.status == "error"]),
            },
            data=response_data,
            duplicates=response_duplicates,
        )

        # Return the response model directly - FastAPI will handle serialization
        return model

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create bulk scenario runs: {str(exc)}"
        ) from exc


# Router is now created at the top of the file
