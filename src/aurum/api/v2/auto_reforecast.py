"""v2 Auto-reforecast orchestration API.

This module provides REST endpoints for:
- Managing forecast triggers and conditions
- Monitoring reforecast jobs and execution
- Viewing trigger events and performance metrics
- Configuring debounce and backpressure controls
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response, Depends
from pydantic import BaseModel, Field

from ..deps import get_settings
from ..services.auto_reforecast_service import (
    get_auto_reforecast_service,
    ForecastTrigger,
    TriggerEvent,
    ReforcastJob,
    TriggerCondition,
    DebounceConfig,
    BackpressureConfig
)
from ...api.v2.forecasting import ForecastRequest
from ...observability.telemetry_facade import get_telemetry_facade

router = APIRouter(prefix="/v2/auto-reforecast", tags=["auto-reforecast"])


class TriggerCreateRequest(BaseModel):
    """Request to create a forecast trigger."""

    name: str = Field(..., description="Trigger name")
    description: str = Field("", description="Trigger description")
    conditions: List[TriggerCondition] = Field(..., description="Trigger conditions")
    forecast_config: Dict[str, Any] = Field(..., description="Forecast configuration")
    priority: float = Field(1.0, description="Trigger priority")
    cooldown_minutes: int = Field(30, description="Cooldown period in minutes")
    enabled: bool = Field(True, description="Whether trigger is enabled")


class TriggerUpdateRequest(BaseModel):
    """Request to update a forecast trigger."""

    name: Optional[str] = Field(None, description="Trigger name")
    description: Optional[str] = Field(None, description="Trigger description")
    conditions: Optional[List[TriggerCondition]] = Field(None, description="Trigger conditions")
    forecast_config: Optional[Dict[str, Any]] = Field(None, description="Forecast configuration")
    priority: Optional[float] = Field(None, description="Trigger priority")
    cooldown_minutes: Optional[int] = Field(None, description="Cooldown period in minutes")
    enabled: Optional[bool] = Field(None, description="Whether trigger is enabled")


class TriggerResponse(BaseModel):
    """Response containing trigger information."""

    trigger_id: str
    name: str
    description: str
    conditions: List[TriggerCondition]
    forecast_config: Dict[str, Any]
    priority: float
    cooldown_minutes: int
    enabled: bool
    last_triggered: Optional[datetime]
    trigger_count: int
    created_at: datetime
    updated_at: datetime


class TriggerListResponse(BaseModel):
    """Response for listing triggers."""

    data: List[TriggerResponse]
    meta: Dict[str, Any]
    links: Dict[str, Any]


class JobResponse(BaseModel):
    """Response containing job information."""

    job_id: str
    trigger_event: TriggerEvent
    forecast_request: Dict[str, Any]
    priority: float
    created_at: datetime
    scheduled_for: datetime
    status: str
    attempts: int
    max_attempts: int
    completed_at: Optional[datetime]
    error_message: Optional[str]


class JobListResponse(BaseModel):
    """Response for listing jobs."""

    data: List[JobResponse]
    meta: Dict[str, Any]
    links: Dict[str, Any]


@router.get("/triggers", response_model=TriggerListResponse)
async def list_forecast_triggers(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    enabled_only: bool = Query(False, description="Only return enabled triggers")
) -> TriggerListResponse:
    """List all forecast triggers with pagination."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()

        # Get triggers (mock implementation)
        triggers = await service.list_triggers(
            enabled_only=enabled_only,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        trigger_responses = [
            TriggerResponse(
                trigger_id=trigger.trigger_id,
                name=trigger.name,
                description=trigger.description,
                conditions=trigger.conditions,
                forecast_config=trigger.forecast_config.dict() if hasattr(trigger.forecast_config, 'dict') else trigger.forecast_config,
                priority=trigger.priority,
                cooldown_minutes=trigger.cooldown_minutes,
                enabled=trigger.enabled,
                last_triggered=trigger.last_triggered,
                trigger_count=trigger.trigger_count,
                created_at=datetime.utcnow(),  # Mock
                updated_at=datetime.utcnow()   # Mock
            )
            for trigger in triggers
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_forecast_triggers",
            query_time_ms=query_time_ms,
            record_count=len(trigger_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return TriggerListResponse(
            data=trigger_responses,
            meta=meta,
            links={}  # Could add pagination links
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_forecast_triggers",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list forecast triggers: {str(exc)}"
        )


@router.post("/triggers", response_model=TriggerResponse, status_code=201)
async def create_forecast_trigger(
    request: Request,
    trigger_data: TriggerCreateRequest
) -> TriggerResponse:
    """Create a new forecast trigger."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()

        # Convert to service format
        forecast_request = ForecastRequest(**trigger_data.forecast_config)

        trigger = ForecastTrigger(
            trigger_id=str(uuid4()),
            name=trigger_data.name,
            description=trigger_data.description,
            conditions=trigger_data.conditions,
            forecast_config=forecast_request,
            priority=trigger_data.priority,
            cooldown_minutes=trigger_data.cooldown_minutes,
            enabled=trigger_data.enabled
        )

        # Save trigger (mock implementation)
        saved_trigger = await service.create_trigger(trigger)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="create_forecast_trigger",
            query_time_ms=query_time_ms
        )

        return TriggerResponse(
            trigger_id=saved_trigger.trigger_id,
            name=saved_trigger.name,
            description=saved_trigger.description,
            conditions=saved_trigger.conditions,
            forecast_config=saved_trigger.forecast_config.dict() if hasattr(saved_trigger.forecast_config, 'dict') else saved_trigger.forecast_config,
            priority=saved_trigger.priority,
            cooldown_minutes=saved_trigger.cooldown_minutes,
            enabled=saved_trigger.enabled,
            last_triggered=saved_trigger.last_triggered,
            trigger_count=saved_trigger.trigger_count,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="create_forecast_trigger",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create forecast trigger: {str(exc)}"
        )


@router.get("/triggers/{trigger_id}", response_model=TriggerResponse)
async def get_forecast_trigger(
    request: Request,
    trigger_id: str
) -> TriggerResponse:
    """Get a specific forecast trigger."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()
        trigger = await service.get_trigger(trigger_id)

        if not trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_forecast_trigger",
            query_time_ms=query_time_ms
        )

        return TriggerResponse(
            trigger_id=trigger.trigger_id,
            name=trigger.name,
            description=trigger.description,
            conditions=trigger.conditions,
            forecast_config=trigger.forecast_config.dict() if hasattr(trigger.forecast_config, 'dict') else trigger.forecast_config,
            priority=trigger.priority,
            cooldown_minutes=trigger.cooldown_minutes,
            enabled=trigger.enabled,
            last_triggered=trigger.last_triggered,
            trigger_count=trigger.trigger_count,
            created_at=datetime.utcnow(),  # Mock
            updated_at=datetime.utcnow()   # Mock
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_forecast_trigger",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get forecast trigger: {str(exc)}"
        )


@router.put("/triggers/{trigger_id}", response_model=TriggerResponse)
async def update_forecast_trigger(
    request: Request,
    trigger_id: str,
    trigger_data: TriggerUpdateRequest
) -> TriggerResponse:
    """Update a forecast trigger."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()

        # Get existing trigger
        existing_trigger = await service.get_trigger(trigger_id)
        if not existing_trigger:
            raise HTTPException(status_code=404, detail="Trigger not found")

        # Apply updates
        updates = trigger_data.dict(exclude_unset=True)
        for key, value in updates.items():
            if key == "forecast_config" and value:
                value = ForecastRequest(**value)
            setattr(existing_trigger, key, value)

        # Save updated trigger (mock implementation)
        updated_trigger = await service.update_trigger(existing_trigger)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="update_forecast_trigger",
            query_time_ms=query_time_ms
        )

        return TriggerResponse(
            trigger_id=updated_trigger.trigger_id,
            name=updated_trigger.name,
            description=updated_trigger.description,
            conditions=updated_trigger.conditions,
            forecast_config=updated_trigger.forecast_config.dict() if hasattr(updated_trigger.forecast_config, 'dict') else updated_trigger.forecast_config,
            priority=updated_trigger.priority,
            cooldown_minutes=updated_trigger.cooldown_minutes,
            enabled=updated_trigger.enabled,
            last_triggered=updated_trigger.last_triggered,
            trigger_count=updated_trigger.trigger_count,
            created_at=datetime.utcnow(),  # Mock
            updated_at=datetime.utcnow()
        )

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="update_forecast_trigger",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update forecast trigger: {str(exc)}"
        )


@router.delete("/triggers/{trigger_id}", status_code=204)
async def delete_forecast_trigger(
    request: Request,
    trigger_id: str
) -> Response:
    """Delete a forecast trigger."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()
        success = await service.delete_trigger(trigger_id)

        if not success:
            raise HTTPException(status_code=404, detail="Trigger not found")

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="delete_forecast_trigger",
            query_time_ms=query_time_ms
        )

        return Response(status_code=204)

    except HTTPException:
        raise
    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="delete_forecast_trigger",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete forecast trigger: {str(exc)}"
        )


@router.get("/jobs", response_model=JobListResponse)
async def list_reforecast_jobs(
    request: Request,
    response: Response,
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    status: Optional[str] = Query(None, description="Filter by job status")
) -> JobListResponse:
    """List reforecast jobs with pagination and filtering."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()

        # Get jobs (mock implementation)
        jobs = await service.list_jobs(
            status=status,
            limit=limit,
            offset=offset
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        # Convert to response format
        job_responses = [
            JobResponse(
                job_id=job.job_id,
                trigger_event=job.trigger_event,
                forecast_request=job.forecast_request.dict() if hasattr(job.forecast_request, 'dict') else job.forecast_request,
                priority=job.priority,
                created_at=job.created_at,
                scheduled_for=job.scheduled_for,
                status=job.status,
                attempts=job.attempts,
                max_attempts=job.max_attempts,
                completed_at=datetime.utcnow() if job.status == "completed" else None,
                error_message=None  # Mock
            )
            for job in jobs
        ]

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_reforecast_jobs",
            query_time_ms=query_time_ms,
            record_count=len(job_responses),
            pagination={"offset": offset, "limit": limit}
        )

        return JobListResponse(
            data=job_responses,
            meta=meta,
            links={}  # Could add pagination links
        )

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_reforecast_jobs",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list reforecast jobs: {str(exc)}"
        )


@router.get("/events", response_model=Dict[str, Any])
async def list_trigger_events(
    request: Request,
    response: Response,
    limit: int = Query(50, ge=1, le=500),
    since: Optional[datetime] = Query(None, description="Only events since this time")
) -> Dict[str, Any]:
    """List recent trigger events."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()

        # Get events (mock implementation)
        events = await service.list_trigger_events(
            limit=limit,
            since=since
        )

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        meta = telemetry.create_response_metadata(
            operation="list_trigger_events",
            query_time_ms=query_time_ms,
            record_count=len(events)
        )

        return {
            "meta": meta,
            "data": events
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="list_trigger_events",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list trigger events: {str(exc)}"
        )


@router.get("/config/debounce", response_model=Dict[str, Any])
async def get_debounce_config(
    request: Request,
    response: Response
) -> Dict[str, Any]:
    """Get current debounce configuration."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()
        config = await service.get_debounce_config()

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="get_debounce_config",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="get_debounce_config",
                query_time_ms=query_time_ms
            ),
            "data": config.dict() if hasattr(config, 'dict') else config
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="get_debounce_config",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get debounce config: {str(exc)}"
        )


@router.put("/config/debounce", response_model=Dict[str, Any])
async def update_debounce_config(
    request: Request,
    config: DebounceConfig
) -> Dict[str, Any]:
    """Update debounce configuration."""
    start_time = time.perf_counter()

    try:
        service = get_auto_reforecast_service()
        updated_config = await service.update_debounce_config(config)

        query_time_ms = (time.perf_counter() - start_time) * 1000

        telemetry = get_telemetry_facade()
        telemetry.record_success(
            operation="update_debounce_config",
            query_time_ms=query_time_ms
        )

        return {
            "meta": telemetry.create_response_metadata(
                operation="update_debounce_config",
                query_time_ms=query_time_ms
            ),
            "data": updated_config.dict() if hasattr(updated_config, 'dict') else updated_config
        }

    except Exception as exc:
        query_time_ms = (time.perf_counter() - start_time) * 1000
        telemetry = get_telemetry_facade()
        telemetry.record_error(
            operation="update_debounce_config",
            error=exc,
            query_time_ms=query_time_ms
        )
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update debounce config: {str(exc)}"
        )
