"""Scenario management endpoints with async support."""

from __future__ import annotations

import time
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from ..telemetry.context import get_request_id
from .models import (
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
    ScenarioOutputListResponse,
    ScenarioOutputFilter,
    ScenarioMetricLatestResponse,
    ScenarioMetricLatest,
    BulkScenarioRunRequest,
    BulkScenarioRunResponse,
    BulkScenarioRunResult,
    BulkScenarioRunDuplicate,
    ScenarioRunBulkResponse,
)
from .exceptions import ValidationException, NotFoundException, ForbiddenException
from .container import get_service
from .async_service import AsyncScenarioService
from ..scenarios.feature_flags import (
    ScenarioOutputFeature,
    require_scenario_output_feature,
    check_scenario_output_feature,
    enforce_scenario_output_limits,
)


router = APIRouter()


@router.get("/v1/scenarios", response_model=ScenarioListResponse)
async def list_scenarios(
    request: Request,
    tenant_id: Optional[str] = Query(None, description="Filter by tenant"),
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> ScenarioListResponse:
    """List scenarios with optional filtering."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ, tenant_id)

    start_time = time.perf_counter()

    try:
        service = get_service(AsyncScenarioService)
        scenarios, total, meta = await service.list_scenarios(
            tenant_id=tenant_id,
            status=status,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(scenarios),
                "total": total,
                "offset": offset,
                "limit": limit,
            },
            data=scenarios,
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list scenarios: {str(exc)}"
        ) from exc


@router.post("/v1/scenarios", response_model=ScenarioResponse, status_code=201)
async def create_scenario(
    request: Request,
    scenario_data: CreateScenarioRequest,
) -> ScenarioResponse:
    """Create a new scenario."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require scenarios write permission
    require_permission(principal, Permission.SCENARIOS_WRITE, scenario_data.tenant_id)

    start_time = time.perf_counter()

    try:
        # Validate request
        if not scenario_data.tenant_id:
            raise ValidationException(
                field="tenant_id",
                message="tenant_id is required",
                request_id=get_request_id()
            )

        if not scenario_data.name or not scenario_data.name.strip():
            raise ValidationException(
                field="name",
                message="scenario name is required",
                request_id=get_request_id()
            )

        service = get_service(AsyncScenarioService)
        scenario = await service.create_scenario(scenario_data.dict())

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=scenario,
        )

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scenario: {str(exc)}"
        ) from exc


@router.get("/v1/scenarios/{scenario_id}", response_model=ScenarioResponse)
async def get_scenario(
    request: Request,
    scenario_id: str,
) -> ScenarioResponse:
    """Get scenario by ID."""
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

        service = get_service(AsyncScenarioService)
        scenario = await service.get_scenario(scenario_id)

        if not scenario:
            raise NotFoundException(
                resource_type="scenario",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=scenario,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario: {str(exc)}"
        ) from exc


@router.delete("/v1/scenarios/{scenario_id}", status_code=204)
async def delete_scenario(
    request: Request,
    scenario_id: str,
) -> None:
    """Delete scenario by ID."""
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

        service = get_service(AsyncScenarioService)
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

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete scenario: {str(exc)}"
        ) from exc


@router.get("/v1/scenarios/{scenario_id}/runs", response_model=ScenarioRunListResponse)
async def list_scenario_runs(
    request: Request,
    scenario_id: str,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
) -> ScenarioRunListResponse:
    """List runs for a specific scenario."""
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

        service = get_service(AsyncScenarioService)
        runs, total, meta = await service.list_scenario_runs(
            scenario_id=scenario_id,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioRunListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(runs),
                "total": total,
                "offset": offset,
                "limit": limit,
            },
            data=runs,
        )

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list scenario runs: {str(exc)}"
        ) from exc


@router.post("/v1/scenarios/{scenario_id}/run", response_model=ScenarioRunResponse, status_code=202)
async def create_scenario_run(
    request: Request,
    scenario_id: str,
    run_options: ScenarioRunOptions,
) -> ScenarioRunResponse:
    """Create and start a new scenario run."""
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

        service = get_service(AsyncScenarioService)
        run = await service.create_scenario_run(
            scenario_id=scenario_id,
            options=run_options.dict()
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create scenario run: {str(exc)}"
        ) from exc


@router.get("/v1/scenarios/{scenario_id}/runs/{run_id}", response_model=ScenarioRunResponse)
async def get_scenario_run(
    request: Request,
    scenario_id: str,
    run_id: str,
) -> ScenarioRunResponse:
    """Get scenario run by ID."""
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

        service = get_service(AsyncScenarioService)
        run = await service.get_scenario_run(scenario_id, run_id)

        if not run:
            raise NotFoundException(
                resource_type="scenario_run",
                resource_id=run_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario run: {str(exc)}"
        ) from exc


@router.post("/v1/scenarios/runs/{run_id}/state", response_model=ScenarioRunResponse)
async def update_scenario_run_state(
    request: Request,
    run_id: str,
    state_update: dict,
) -> ScenarioRunResponse:
    """Update scenario run state."""
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

        service = get_service(AsyncScenarioService)
        run = await service.update_scenario_run_state(run_id, state_update)

        if not run:
            raise NotFoundException(
                resource_type="scenario_run",
                resource_id=run_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update scenario run state: {str(exc)}"
        ) from exc


@router.post("/v1/scenarios/runs/{run_id}/cancel", response_model=ScenarioRunResponse)
async def cancel_scenario_run(
    request: Request,
    run_id: str,
) -> ScenarioRunResponse:
    """Cancel a running scenario run with idempotency and worker signaling."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require scenarios delete permission
    require_permission(principal, Permission.SCENARIOS_DELETE)

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

        service = get_service(AsyncScenarioService)
        run = await service.cancel_scenario_run(run_id)

        if not run:
            raise NotFoundException(
                resource_type="scenario_run",
                resource_id=run_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=run,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to cancel scenario run: {str(exc)}"
        ) from exc


@router.get("/v1/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputListResponse)
@require_scenario_output_feature(ScenarioOutputFeature.SCENARIO_OUTPUTS_ENABLED)
async def get_scenario_outputs(
    request: Request,
    scenario_id: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    start_time: Optional[str] = Query(None, description="Start time filter (ISO 8601 format)"),
    end_time: Optional[str] = Query(None, description="End time filter (ISO 8601 format)"),
    metric_name: Optional[str] = Query(None, description="Filter by metric name"),
    min_value: Optional[float] = Query(None, description="Minimum value filter", ge=0),
    max_value: Optional[float] = Query(None, description="Maximum value filter", ge=0),
    tags: Optional[str] = Query(None, description="Filter by tags (JSON format)"),
    format: Optional[str] = Query(None, description="Output format (json, csv)"),
) -> ScenarioOutputListResponse:
    """Get scenario outputs with time-based filtering and pagination."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require scenarios read permission
    require_permission(principal, Permission.SCENARIOS_READ)

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

        service = get_service(AsyncScenarioService)
        outputs, total, meta = await service.get_scenario_outputs(
            scenario_id=scenario_id,
            limit=limit,
            offset=offset,
            start_time=start_time,
            end_time=end_time,
            metric_name=metric_name,
            min_value=min_value,
            max_value=max_value,
            tags=tags_filter,
        )

        query_time_ms = (time.perf_counter() - start_time_perf) * 1000

        # Build applied filter for response
        applied_filter = ScenarioOutputFilter(
            start_time=start_time,
            end_time=end_time,
            metric_name=metric_name,
            min_value=min_value,
            max_value=max_value,
            tags=tags_filter,
        )

        return ScenarioOutputListResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(outputs),
                "total": total,
                "offset": offset,
                "limit": limit,
                "has_more": (offset + limit) < total,
            },
            data=outputs,
            filter=applied_filter,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time_perf) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario outputs: {str(exc)}"
        ) from exc


@router.get("/v1/scenarios/{scenario_id}/metrics/latest", response_model=ScenarioMetricLatestResponse)
async def get_scenario_metrics_latest(
    request: Request,
    scenario_id: str,
) -> ScenarioMetricLatestResponse:
    """Get latest metrics for a scenario."""
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

        service = get_service(AsyncScenarioService)
        metrics = await service.get_scenario_metrics_latest(scenario_id)

        if not metrics:
            raise NotFoundException(
                resource_type="scenario_metrics",
                resource_id=scenario_id,
                request_id=get_request_id()
            )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        return ScenarioMetricLatestResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
            },
            data=metrics,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get scenario metrics: {str(exc)}"
        ) from exc


@router.post("/v1/scenarios/{scenario_id}/runs:bulk", response_model=BulkScenarioRunResponse, status_code=202)
@require_scenario_output_feature(ScenarioOutputFeature.BULK_SCENARIO_RUNS)
async def create_bulk_scenario_runs(
    request: Request,
    scenario_id: str,
    bulk_request: BulkScenarioRunRequest,
) -> BulkScenarioRunResponse:
    """Create multiple scenario runs in bulk with deduplication via idempotency keys."""
    from .auth import require_permission, Permission

    # Get principal from request state
    principal = getattr(request.state, "principal", None)

    # Require scenarios write permission
    require_permission(principal, Permission.SCENARIOS_WRITE)

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

        # Validate bulk request
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

        service = get_service(AsyncScenarioService)
        results, duplicates = await service.create_bulk_scenario_runs(
            scenario_id=scenario_id,
            runs=[run.dict() for run in bulk_request.runs],
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

        return BulkScenarioRunResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "total_runs": len(bulk_request.runs),
                "successful_runs": len([r for r in response_data if r.status == "created"]),
                "duplicate_runs": len(response_duplicates),
                "failed_runs": len([r for r in response_data if r.status == "failed"]),
            },
            data=response_data,
            duplicates=response_duplicates,
        )

    except ValidationException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create bulk scenario runs: {str(exc)}"
        ) from exc
