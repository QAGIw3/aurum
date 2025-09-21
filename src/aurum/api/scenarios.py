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
    ScenarioMetricLatestResponse,
    ScenarioMetricLatest,
)
from .exceptions import ValidationException, NotFoundException, ForbiddenException
from .container import get_service
from .async_service import AsyncScenarioService


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
        print(f"Deleted scenario {scenario_id} in {query_time_ms".2f"}ms")

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
    """Cancel a running scenario run."""
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


@router.get("/v1/scenarios/{scenario_id}/outputs", response_model=ScenarioOutputResponse)
async def get_scenario_outputs(
    request: Request,
    scenario_id: str,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    format: Optional[str] = Query(None, description="Output format (json, csv)"),
) -> ScenarioOutputResponse:
    """Get scenario outputs."""
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
        outputs, total, meta = await service.get_scenario_outputs(
            scenario_id=scenario_id,
            limit=limit,
            offset=offset,
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Handle CSV format
        if format == "csv":
            # This would be implemented with streaming CSV response
            pass

        return ScenarioOutputResponse(
            meta={
                "request_id": get_request_id(),
                "query_time_ms": round(query_time_ms, 2),
                "count": len(outputs),
                "total": total,
                "offset": offset,
                "limit": limit,
            },
            data=outputs,
        )

    except (ValidationException, NotFoundException):
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
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
