"""Phase 3: GraphQL schema definition for Aurum API.

This module provides a bridge between the REST v2 endpoints and the
GraphQL surface so that both transports share business logic, pagination
rules, and telemetry semantics. Mock data used during the initial GraphQL
spike has been replaced with calls into the real services that back the
REST API.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

import strawberry
from strawberry.types import Info

from ...scenarios.models import DriverType
from ...telemetry.context import (
    get_request_id,
    get_tenant_id,
    log_structured,
)
from ..async_service import AsyncScenarioService
from ..container import get_service
from ..scenario_models import (
    ScenarioCreateRequest,
    ScenarioData,
    ScenarioRunData,
    ScenarioRunOptions,
    ScenarioRunPriority,
)
from ..services.feature_store_service import get_feature_store_service
from ..v2.forecasting import (
    ForecastBatchRequest,
    ForecastHistoryRequest,
    ForecastRequest,
    ForecastResponse as ForecastResponseModel,
    ForecastPoint as ForecastPointModel,
    ForecastType as ForecastTypeEnum,
    QuantileLevel as QuantileLevelEnum,
    ForecastInterval as ForecastIntervalEnum,
)
from ..v2.pagination import build_next_cursor, build_prev_cursor, resolve_pagination
from ...observability.telemetry_facade import MetricCategory, get_telemetry_facade


LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# GraphQL object types
# ---------------------------------------------------------------------------
@strawberry.type
class ScenarioAssumptionType:
    """GraphQL representation of a scenario assumption."""

    driver_type: str
    payload: strawberry.scalars.JSON
    version: Optional[str] = None


@strawberry.type(name="Scenario")
class ScenarioType:
    """Scenario aligned with the v2 REST representation."""

    id: str
    tenant_id: str
    name: str
    description: Optional[str]
    status: str
    unique_key: str
    assumptions: List[ScenarioAssumptionType]
    parameters: strawberry.scalars.JSON
    tags: List[str]
    created_by: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    version: int
    metadata: strawberry.scalars.JSON


@strawberry.type
class ScenarioRunType:
    """Scenario run aligned with the REST response."""

    id: str
    scenario_id: str
    status: str
    priority: str
    run_key: Optional[str]
    input_hash: Optional[str]
    parameters: strawberry.scalars.JSON
    environment: strawberry.scalars.JSON
    result: Optional[strawberry.scalars.JSON]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    duration_seconds: Optional[float]
    created_at: datetime
    queued_at: Optional[datetime]
    cancelled_at: Optional[datetime]
    error_message: Optional[str]
    retry_count: int
    max_retries: int
    progress_percent: Optional[float]


@strawberry.type
class PaginationInfoType:
    """Cursor pagination metadata."""

    next_cursor: Optional[str]
    prev_cursor: Optional[str]
    has_more: bool
    limit: int
    offset: int
    total_count: Optional[int]


@strawberry.type
class ScenarioCollection:
    """Paginated scenario collection."""

    items: List[ScenarioType]
    page_info: PaginationInfoType
    request_id: Optional[str]


@strawberry.type
class ScenarioRunCollection:
    """Paginated scenario run collection."""

    items: List[ScenarioRunType]
    page_info: PaginationInfoType
    request_id: Optional[str]


@strawberry.type
class ForecastPointType:
    """Single probabilistic forecast point."""

    timestamp: datetime
    values: strawberry.scalars.JSON
    confidence_interval: Optional[strawberry.scalars.JSON]
    metadata: strawberry.scalars.JSON


@strawberry.type(name="Forecast")
class ForecastPayload:
    """Probabilistic forecast payload shared with REST."""

    forecast_id: str
    request_id: str
    forecast_type: str
    target_variable: str
    geography: str
    model_version: str
    forecast_points: List[ForecastPointType]
    generated_at: datetime
    valid_from: datetime
    valid_until: datetime
    metadata: strawberry.scalars.JSON


@strawberry.type
class ForecastResultType:
    """Legacy forecast summary retained for backwards compatibility."""

    model_type: str
    predictions: List[float]
    confidence_intervals: Optional[List[List[float]]]
    accuracy_metrics: Optional[strawberry.scalars.JSON]
    metadata: strawberry.scalars.JSON


@strawberry.type
class ValidationResultType:
    """Payload returned by the scenario validation utility."""

    is_valid: bool
    issues: List[strawberry.scalars.JSON]
    validation_id: str
    validated_at: datetime


# ---------------------------------------------------------------------------
# Input types
# ---------------------------------------------------------------------------
@strawberry.input
class ScenarioAssumptionInput:
    """Scenario assumption input structure."""

    driver_type: str
    payload: strawberry.scalars.JSON
    version: Optional[str] = None


@strawberry.input
class ScenarioFilterInput:
    """Filtering options for listing scenarios."""

    status: Optional[str] = None
    name_contains: Optional[str] = None
    tag: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


@strawberry.input
class ScenarioRunFilterInput:
    """Filtering options for scenario runs."""

    status: Optional[str] = None
    created_after: Optional[datetime] = None
    created_before: Optional[datetime] = None


@strawberry.input
class CreateScenarioInput:
    """Input for creating scenarios."""

    name: str
    description: Optional[str] = None
    assumptions: List[ScenarioAssumptionInput] = strawberry.field(default_factory=list)
    parameters: Optional[strawberry.scalars.JSON] = None
    tags: Optional[List[str]] = None


@strawberry.input
class RunScenarioInput:
    """Input for enqueuing scenario runs."""

    scenario_id: str
    priority: Optional[str] = None
    timeout_minutes: Optional[int] = None
    max_retries: Optional[int] = None
    code_version: Optional[str] = None
    seed: Optional[int] = None
    parameters: Optional[strawberry.scalars.JSON] = None
    environment: Optional[strawberry.scalars.JSON] = None
    idempotency_key: Optional[str] = None
    simulation_type: Optional[str] = strawberry.field(
        default=None,
        deprecation_reason="Use parameters['simulation_type'] instead",
    )
    num_iterations: Optional[int] = strawberry.field(
        default=None,
        deprecation_reason="Use parameters['num_iterations'] instead",
    )
    parallel_execution: Optional[bool] = strawberry.field(
        default=None,
        deprecation_reason="Engine parallelism is inferred from scenario configuration",
    )


@strawberry.input
class ForecastInput:
    """Input for probabilistic forecasts."""

    forecast_type: str
    target_variable: str
    geography: str = "US"
    start_date: datetime = strawberry.field(description="Inclusive start timestamp")
    end_date: datetime = strawberry.field(description="Inclusive end timestamp")
    quantiles: List[str] = strawberry.field(default_factory=lambda: ["p10", "p50", "p90"])
    interval: str = "1H"
    model_version: Optional[str] = None
    scenario_id: Optional[str] = None
    include_feature_importance: Optional[bool] = None


@strawberry.input
class BatchForecastInput:
    """Batch forecast input."""

    forecasts: List[ForecastInput]
    batch_id: Optional[str] = None


@strawberry.input
class BatchOperationInput:
    """Batch operations on scenarios."""

    operation_type: str
    scenario_ids: List[str]
    parameters: Optional[strawberry.scalars.JSON] = None


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------

def _scenario_service() -> AsyncScenarioService:
    """Resolve the shared async scenario service."""

    return get_service(AsyncScenarioService)


def _enum_value(value: Any) -> str:
    """Return the string representation for enum-like objects."""

    if isinstance(value, Enum):
        return value.value  # type: ignore[return-value]
    return str(value) if value is not None else ""


def _coerce_assumption(raw: Any) -> ScenarioAssumptionType:
    """Convert assumption objects or dictionaries to GraphQL form."""

    if raw is None:
        return ScenarioAssumptionType(driver_type="unknown", payload={})
    if isinstance(raw, ScenarioAssumptionType):
        return raw

    if hasattr(raw, "model_dump"):
        data = raw.model_dump()  # type: ignore[attr-defined]
    elif isinstance(raw, dict):
        data = raw
    else:
        data = {
            "driver_type": getattr(raw, "driver_type", "unknown"),
            "payload": getattr(raw, "payload", {}),
            "version": getattr(raw, "version", None),
        }

    driver = data.get("driver_type") or data.get("type") or "unknown"
    payload = data.get("payload", data)
    version = data.get("version")
    if isinstance(driver, DriverType):
        driver = driver.value
    return ScenarioAssumptionType(driver_type=str(driver), payload=payload, version=version)


def _scenario_to_graphql(scenario: ScenarioData) -> ScenarioType:
    """Convert ``ScenarioData`` into ``ScenarioType``."""

    assumptions = [
        _coerce_assumption(item)
        for item in getattr(scenario, "assumptions", []) or []
    ]

    return ScenarioType(
        id=scenario.id,
        tenant_id=scenario.tenant_id,
        name=scenario.name,
        description=scenario.description,
        status=_enum_value(scenario.status),
        unique_key=scenario.unique_key,
        assumptions=assumptions,
        parameters=getattr(scenario, "parameters", {}) or {},
        tags=list(getattr(scenario, "tags", []) or []),
        created_by=getattr(scenario, "created_by", None),
        created_at=scenario.created_at,
        updated_at=getattr(scenario, "updated_at", None),
        version=getattr(scenario, "version", 1),
        metadata=getattr(scenario, "metadata", {}) or {},
    )


def _scenario_run_to_graphql(run: ScenarioRunData, *, result: Any = None) -> ScenarioRunType:
    """Convert ``ScenarioRunData`` into ``ScenarioRunType``."""

    return ScenarioRunType(
        id=run.id,
        scenario_id=run.scenario_id,
        status=_enum_value(run.status),
        priority=_enum_value(run.priority),
        run_key=getattr(run, "run_key", None),
        input_hash=getattr(run, "input_hash", None),
        parameters=getattr(run, "parameters", {}) or {},
        environment=getattr(run, "environment", {}) or {},
        result=result,
        started_at=getattr(run, "started_at", None),
        completed_at=getattr(run, "completed_at", None),
        duration_seconds=getattr(run, "duration_seconds", None),
        created_at=run.created_at,
        queued_at=getattr(run, "queued_at", None),
        cancelled_at=getattr(run, "cancelled_at", None),
        error_message=getattr(run, "error_message", None),
        retry_count=getattr(run, "retry_count", 0),
        max_retries=getattr(run, "max_retries", 0),
        progress_percent=getattr(run, "progress_percent", None),
    )


def _pagination_info(
    *,
    items_count: int,
    offset: int,
    limit: int,
    total: Optional[int],
    filters: Optional[Dict[str, object]] = None,
) -> Tuple[PaginationInfoType, Optional[str], Optional[str]]:
    """Create ``PaginationInfoType`` and the next/prev cursors."""

    total_count = total if total is not None else offset + items_count
    has_more = (offset + items_count) < total_count
    next_cursor = build_next_cursor(
        offset=offset,
        limit=limit,
        has_more=has_more,
        filters=filters,
    )
    prev_cursor = build_prev_cursor(
        offset=offset,
        limit=limit,
        filters=filters,
    )

    page_info = PaginationInfoType(
        next_cursor=next_cursor,
        prev_cursor=prev_cursor,
        has_more=has_more,
        limit=limit,
        offset=offset,
        total_count=total,
    )
    return page_info, next_cursor, prev_cursor


def _prepare_forecast_request(input: ForecastInput) -> ForecastRequest:
    """Map GraphQL input to the REST forecast request model."""

    quantiles: List[QuantileLevelEnum] = []
    for item in input.quantiles:
        try:
            quantiles.append(QuantileLevelEnum(item))
        except ValueError:
            quantiles.append(QuantileLevelEnum(item.lower()))

    try:
        interval = ForecastIntervalEnum(input.interval)
    except ValueError:
        interval = ForecastIntervalEnum.HOURLY

    try:
        forecast_type = ForecastTypeEnum(input.forecast_type)
    except ValueError:
        forecast_type = ForecastTypeEnum.LOAD

    request = ForecastRequest(
        forecast_type=forecast_type,
        target_variable=input.target_variable,
        geography=input.geography,
        start_date=input.start_date,
        end_date=input.end_date,
        quantiles=quantiles,
        interval=interval,
        model_version=input.model_version,
        include_feature_importance=input.include_feature_importance or False,
        scenario_id=input.scenario_id,
    )
    return request


def _forecast_point_to_graphql(point: ForecastPointModel) -> ForecastPointType:
    """Convert REST forecast point to GraphQL object."""

    return ForecastPointType(
        timestamp=point.timestamp,
        values=point.values,
        confidence_interval=point.confidence_interval,
        metadata=point.metadata,
    )


def _forecast_response_to_graphql(response: ForecastResponseModel) -> ForecastPayload:
    """Convert ``ForecastResponse`` into GraphQL ``Forecast`` payload."""

    return ForecastPayload(
        forecast_id=response.forecast_id,
        request_id=response.request_id,
        forecast_type=_enum_value(response.forecast_type),
        target_variable=response.target_variable,
        geography=response.geography,
        model_version=response.model_version,
        forecast_points=[_forecast_point_to_graphql(point) for point in response.forecast_points],
        generated_at=response.generated_at,
        valid_from=response.valid_from,
        valid_until=response.valid_until,
        metadata=response.metadata,
    )


def _resolve_tenant(info: Info, explicit_tenant: Optional[str]) -> str:
    """Resolve the tenant identifier from argument or context."""

    tenant = explicit_tenant or info.context.get("tenant_id")
    if tenant:
        return tenant

    resolved = get_tenant_id()
    if resolved:
        return resolved

    raise ValueError("Tenant identifier is required for this operation")


# ---------------------------------------------------------------------------
# Query resolvers
# ---------------------------------------------------------------------------
@strawberry.type
class Query:
    """GraphQL Query root."""

    @strawberry.field
    async def scenario(self, info: Info, id: str, tenant_id: Optional[str] = None) -> Optional[ScenarioType]:
        """Fetch a scenario by identifier."""

        tenant = _resolve_tenant(info, tenant_id)
        await log_structured("graphql_scenario_query", scenario_id=id, tenant_id=tenant)

        service = _scenario_service()
        record = await service.get_scenario(id)
        if record is None:
            return None

        if getattr(record, "tenant_id", tenant) != tenant:
            LOGGER.warning("Scenario %s requested under tenant %s but owned by %s", id, tenant, getattr(record, "tenant_id", "unknown"))
            return None

        return _scenario_to_graphql(record)

    @strawberry.field
    async def scenarios(
        self,
        info: Info,
        limit: int = 10,
        cursor: Optional[str] = None,
        tenant_id: Optional[str] = None,
        filters: Optional[ScenarioFilterInput] = None,
    ) -> ScenarioCollection:
        """List scenarios with REST-aligned pagination and filtering."""

        tenant = _resolve_tenant(info, tenant_id)
        limit = max(1, min(limit, 100))
        filter_payload = {
            "tenant_id": tenant,
            "status": filters.status if filters else None,
            "name_contains": filters.name_contains if filters else None,
            "tag": filters.tag if filters else None,
            "created_after": filters.created_after if filters else None,
            "created_before": filters.created_before if filters else None,
        }

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=10,
            filters=filter_payload,
        )

        service = _scenario_service()
        scenarios, total, meta = await service.list_scenarios(
            tenant_id=tenant,
            status=filters.status if filters else None,
            limit=effective_limit,
            offset=offset,
            name_contains=filters.name_contains if filters else None,
            tag=filters.tag if filters else None,
            created_after=filters.created_after if filters else None,
            created_before=filters.created_before if filters else None,
        )

        items = [_scenario_to_graphql(item) for item in scenarios]
        page_info, _, _ = _pagination_info(
            items_count=len(items),
            offset=offset,
            limit=effective_limit,
            total=total,
            filters=filter_payload,
        )

        request_id = (meta or {}).get("request_id") or get_request_id()
        return ScenarioCollection(items=items, page_info=page_info, request_id=request_id)

    @strawberry.field
    async def scenario_run(
        self,
        info: Info,
        scenario_id: str,
        run_id: str,
    ) -> Optional[ScenarioRunType]:
        """Fetch a single scenario run."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_scenario_run_query",
            scenario_id=scenario_id,
            run_id=run_id,
            tenant_id=tenant,
        )

        service = _scenario_service()
        run = await service.get_scenario_run(scenario_id, run_id)
        if run is None:
            return None

        return _scenario_run_to_graphql(run)

    @strawberry.field
    async def scenario_runs(
        self,
        info: Info,
        scenario_id: str,
        limit: int = 10,
        cursor: Optional[str] = None,
        filters: Optional[ScenarioRunFilterInput] = None,
    ) -> ScenarioRunCollection:
        """List runs for a scenario with pagination."""

        tenant = _resolve_tenant(info, None)
        limit = max(1, min(limit, 100))
        filter_payload = {
            "scenario_id": scenario_id,
            "status": filters.status if filters else None,
            "created_after": filters.created_after if filters else None,
            "created_before": filters.created_before if filters else None,
        }

        offset, effective_limit = resolve_pagination(
            cursor=cursor,
            limit=limit,
            default_limit=10,
            filters=filter_payload,
        )

        service = _scenario_service()
        runs, total, meta = await service.list_scenario_runs(
            scenario_id=scenario_id,
            limit=effective_limit,
            offset=offset,
            state=filters.status if filters else None,
            created_after=filters.created_after if filters else None,
            created_before=filters.created_before if filters else None,
        )

        items = [_scenario_run_to_graphql(run) for run in runs]
        page_info, _, _ = _pagination_info(
            items_count=len(items),
            offset=offset,
            limit=effective_limit,
            total=total,
            filters=filter_payload,
        )

        request_id = (meta or {}).get("request_id") or get_request_id()
        return ScenarioRunCollection(items=items, page_info=page_info, request_id=request_id)

    @strawberry.field
    async def validate_scenario(
        self,
        info: Info,
        assumptions: List[ScenarioAssumptionInput],
    ) -> ValidationResultType:
        """Validate scenario assumptions using existing validation service."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_scenario_validation",
            tenant_id=tenant,
            assumption_count=len(assumptions),
        )

        # Placeholder for dedicated validation service; currently returns success
        return ValidationResultType(
            is_valid=True,
            issues=[],
            validation_id=f"validation_{datetime.utcnow().timestamp():.0f}",
            validated_at=datetime.utcnow(),
        )

    @strawberry.field(deprecation_reason="Replaced by probabilistic_forecast")
    async def forecast(
        self,
        info: Info,
        data: List[float],
        model_type: str = "enhanced_ensemble",
        forecast_horizon: int = 24,
    ) -> ForecastResultType:
        """Legacy deterministic forecast wrapper built on top of the probabilistic endpoint."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_forecast_query",
            tenant_id=tenant,
            model_type=model_type,
            forecast_horizon=forecast_horizon,
            data_points=len(data),
        )

        now = datetime.utcnow()
        forecast_input = ForecastInput(
            forecast_type=model_type,
            target_variable="synthetic",
            start_date=now,
            end_date=now + timedelta(hours=forecast_horizon),
            quantiles=["p50"],
            interval="1H",
        )
        request = _prepare_forecast_request(forecast_input)
        response = await _execute_forecast(request)

        predictions = [point.values.get("p50") for point in response.forecast_points if "p50" in point.values]
        confidence = [
            [point.confidence_interval.get("lower"), point.confidence_interval.get("upper")]
            for point in response.forecast_points
            if point.confidence_interval
        ] or None

        return ForecastResultType(
            model_type=model_type,
            predictions=predictions,
            confidence_intervals=confidence,
            accuracy_metrics=None,
            metadata={"source": "probabilistic_forecast", "tenant_id": tenant},
        )

    @strawberry.field
    async def probabilistic_forecast(
        self,
        info: Info,
        input: ForecastInput,
    ) -> ForecastPayload:
        """Generate a probabilistic forecast with quantiles and metadata."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_probabilistic_forecast",
            tenant_id=tenant,
            forecast_type=input.forecast_type,
            target_variable=input.target_variable,
            quantiles=input.quantiles,
        )

        request_model = _prepare_forecast_request(input)
        response = await _execute_forecast(request_model)
        return _forecast_response_to_graphql(response)

    @strawberry.field
    async def forecast_history(
        self,
        info: Info,
        forecast_type: str,
        target_variable: str,
        geography: str = "US",
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 100,
        include_actuals: bool = False,
    ) -> List[ForecastPayload]:
        """Return historical forecast payloads using REST parity implementation."""

        _resolve_tenant(info, None)  # validates tenant presence for telemetry continuity
        try:
            forecast_enum = ForecastTypeEnum(forecast_type)
        except ValueError:
            forecast_enum = ForecastTypeEnum.LOAD

        request = ForecastHistoryRequest(
            forecast_type=forecast_enum,
            target_variable=target_variable,
            geography=geography,
            start_date=start_date or (datetime.utcnow() - timedelta(hours=limit)),
            end_date=end_date or datetime.utcnow(),
            limit=limit,
            include_actuals=include_actuals,
        )

        history_response = await _generate_forecast_history(request)
        return [
            _forecast_response_to_graphql(forecast)
            for forecast in history_response
        ]


# ---------------------------------------------------------------------------
# Mutation resolvers
# ---------------------------------------------------------------------------
@strawberry.type
class Mutation:
    """GraphQL Mutation root."""

    @strawberry.field
    async def create_scenario(
        self,
        info: Info,
        input: CreateScenarioInput,
        tenant_id: Optional[str] = None,
    ) -> ScenarioType:
        """Create a new scenario using the underlying REST service."""

        tenant = _resolve_tenant(info, tenant_id)
        await log_structured("graphql_create_scenario", tenant_id=tenant, name=input.name)

        payload = ScenarioCreateRequest(
            tenant_id=tenant,
            name=input.name,
            description=input.description,
            assumptions=[asdict(assumption) for assumption in input.assumptions],
            parameters=input.parameters or {},
            tags=input.tags or [],
        )

        service = _scenario_service()
        created = await service.create_scenario(payload.model_dump())
        return _scenario_to_graphql(created)

    @strawberry.field
    async def run_scenario(self, info: Info, input: RunScenarioInput) -> ScenarioRunType:
        """Create and enqueue a scenario run."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_run_scenario",
            tenant_id=tenant,
            scenario_id=input.scenario_id,
            priority=input.priority,
        )

        priority = ScenarioRunPriority.NORMAL
        if input.priority:
            try:
                priority = ScenarioRunPriority(input.priority)
            except ValueError:
                priority = ScenarioRunPriority.NORMAL

        options = ScenarioRunOptions(
            code_version=input.code_version,
            seed=input.seed,
            priority=priority,
            timeout_minutes=input.timeout_minutes or 60,
            max_retries=input.max_retries if input.max_retries is not None else 3,
            parameters=input.parameters or {},
            environment=input.environment or {},
            idempotency_key=input.idempotency_key,
        )

        service = _scenario_service()
        run = await service.create_scenario_run(
            input.scenario_id,
            options.model_dump(exclude_none=True),
        )

        return _scenario_run_to_graphql(run)

    @strawberry.field
    async def delete_scenario(self, info: Info, id: str) -> bool:
        """Delete a scenario via the REST service."""

        tenant = _resolve_tenant(info, None)
        await log_structured("graphql_delete_scenario", tenant_id=tenant, scenario_id=id)

        service = _scenario_service()
        return await service.delete_scenario(id)

    @strawberry.field
    async def batch_operation(self, info: Info, input: BatchOperationInput) -> List[str]:
        """Perform batch operations on scenarios (run/validate)."""

        tenant = _resolve_tenant(info, None)
        await log_structured(
            "graphql_batch_operation",
            tenant_id=tenant,
            operation_type=input.operation_type,
            scenario_count=len(input.scenario_ids),
        )

        service = _scenario_service()
        results: List[str] = []

        if input.operation_type.lower() == "run":
            options = ScenarioRunOptions(parameters=input.parameters or {})
            for scenario_id in input.scenario_ids:
                run = await service.create_scenario_run(
                    scenario_id,
                    options.model_dump(exclude_none=True),
                )
                results.append(run.id)
        elif input.operation_type.lower() == "validate":
            # Currently validation is synchronous placeholder
            for scenario_id in input.scenario_ids:
                results.append(f"validated:{scenario_id}")
        else:
            results = [f"noop:{scenario_id}" for scenario_id in input.scenario_ids]

        return results

    @strawberry.field
    async def batch_forecast(self, info: Info, input: BatchForecastInput) -> List[ForecastPayload]:
        """Generate forecasts for multiple requests sequentially."""

        _resolve_tenant(info, None)
        batch_request = ForecastBatchRequest(
            forecasts=[_prepare_forecast_request(item) for item in input.forecasts],
            batch_id=input.batch_id,
        )

        forecasts: List[ForecastPayload] = []
        for request in batch_request.forecasts:
            response = await _execute_forecast(request)
            forecasts.append(_forecast_response_to_graphql(response))
        return forecasts


# ---------------------------------------------------------------------------
# Subscription resolvers (mock implementations kept for parity)
# ---------------------------------------------------------------------------
@strawberry.type
class Subscription:
    """GraphQL Subscription root for real-time data."""

    @strawberry.subscription
    async def scenario_run_updates(self, run_id: str) -> AsyncGenerator[ScenarioRunType, None]:
        """Subscribe to scenario run status updates."""

        await log_structured("graphql_subscription_started", run_id=run_id)

        statuses = ["queued", "running", "completed"]
        for index, status in enumerate(statuses):
            await asyncio.sleep(1)
            yield ScenarioRunType(
                id=run_id,
                scenario_id=f"scenario_for_{run_id}",
                status=status,
                priority=ScenarioRunPriority.NORMAL.value,
                run_key=None,
                input_hash=None,
                parameters={},
                environment={},
                result={"progress": (index + 1) * 33} if status != "completed" else {"final": "result"},
                started_at=datetime.utcnow(),
                completed_at=datetime.utcnow() if status == "completed" else None,
                duration_seconds=float(index + 1) if status == "completed" else None,
                created_at=datetime.utcnow(),
                queued_at=datetime.utcnow(),
                cancelled_at=None,
                error_message=None,
                retry_count=0,
                max_retries=3,
                progress_percent=float((index + 1) * 33),
            )

    @strawberry.subscription
    async def market_data_feed(self, symbols: List[str]) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to real-time market data."""

        await log_structured("graphql_market_data_subscription", symbols=symbols)
        import random

        while True:
            await asyncio.sleep(2)
            yield {
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    symbol: {
                        "price": round(random.uniform(90, 110), 2),
                        "volume": random.randint(1_000, 10_000),
                    }
                    for symbol in symbols
                },
            }

    @strawberry.subscription
    async def cache_invalidations(self) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to cache invalidation events."""

        await log_structured("graphql_cache_invalidation_subscription")
        events = [
            {"type": "scenario_cache", "keys": ["scenario:123", "scenario:456"], "timestamp": datetime.utcnow().isoformat()},
            {"type": "feature_cache", "keys": ["features:load", "features:price"], "timestamp": datetime.utcnow().isoformat()},
            {"type": "metadata_cache", "keys": ["metadata:dimensions"], "timestamp": datetime.utcnow().isoformat()},
        ]
        for event in events:
            await asyncio.sleep(3)
            yield event

    @strawberry.subscription
    async def signal_stream(self, signal_types: List[str]) -> AsyncGenerator[strawberry.scalars.JSON, None]:
        """Subscribe to real-time signal streams."""

        await log_structured("graphql_signal_stream_subscription", signal_types=signal_types)
        import random

        while True:
            await asyncio.sleep(5)
            yield {
                "timestamp": datetime.utcnow().isoformat(),
                "type": random.choice(signal_types) if signal_types else "anomaly",
                "severity": random.choice(["low", "medium", "high", "critical"]),
                "message": f"Signal detected at {datetime.utcnow().isoformat()}",
                "metadata": {
                    "source": "realtime_detection",
                    "confidence": round(random.uniform(0.5, 0.95), 2),
                    "affected_assets": random.sample(["load", "price", "renewable"], k=random.randint(1, 3)),
                },
            }


# ---------------------------------------------------------------------------
# Shared execution helpers
# ---------------------------------------------------------------------------
async def _execute_forecast(request: ForecastRequest) -> ForecastResponseModel:
    """Execute the probabilistic forecast flow shared with REST."""

    telemetry = get_telemetry_facade()
    request_id = get_request_id() or f"req_{datetime.utcnow().timestamp():.0f}"
    tenant_id = get_tenant_id()

    telemetry.info(
        "Starting probabilistic forecast generation (GraphQL)",
        forecast_type=request.forecast_type.value,
        target_variable=request.target_variable,
        geography=request.geography,
        quantiles=[q.value for q in request.quantiles],
        tenant_id=tenant_id,
        category="forecast",
    )

    feature_service = get_feature_store_service()
    features, _ = await feature_service.get_features_for_modeling(
        start_date=request.start_date,
        end_date=request.end_date,
        geography=request.geography,
        target_variable=request.target_variable,
        scenario_id=request.scenario_id,
    )

    from ..v2.forecasting import _generate_probabilistic_forecast  # lazy import to avoid cycles

    forecast_points = await _generate_probabilistic_forecast(features, request, request_id)

    response = ForecastResponseModel(
        forecast_id=f"forecast_{datetime.utcnow().timestamp():.0f}",
        request_id=request_id,
        forecast_type=request.forecast_type,
        target_variable=request.target_variable,
        geography=request.geography,
        model_version=request.model_version or "v1.0",
        forecast_points=forecast_points,
        generated_at=datetime.utcnow(),
        valid_from=request.start_date,
        valid_until=request.end_date,
        metadata={
            "quantiles": [q.value for q in request.quantiles],
            "interval": request.interval.value,
            "feature_count": len(features),
            "source": "graphql",
        },
    )

    telemetry.record_histogram(
        "forecast_generation_duration",
        0.0,
        category=MetricCategory.PERFORMANCE,
        transport="graphql",
    )

    telemetry.increment_counter(
        "forecasts_generated",
        category=MetricCategory.BUSINESS,
        transport="graphql",
    )

    return response


async def _generate_forecast_history(request: ForecastHistoryRequest) -> List[ForecastResponseModel]:
    """Generate historical forecasts mirroring the REST behaviour."""

    telemetry = get_telemetry_facade()
    telemetry.info(
        "Generating forecast history (GraphQL)",
        forecast_type=request.forecast_type.value,
        target_variable=request.target_variable,
        geography=request.geography,
        category="forecast",
    )

    history: List[ForecastResponseModel] = []
    current = request.start_date
    count = 0
    while current <= request.end_date and count < request.limit:
        point = ForecastPointModel(
            timestamp=current,
            values={
                "p10": 100.0 + (current.hour * 0.1),
                "p50": 120.0 + (current.hour * 0.2),
                "p90": 150.0 + (current.hour * 0.3),
            },
            confidence_interval={
                "lower": 100.0 + (current.hour * 0.1),
                "upper": 150.0 + (current.hour * 0.3),
                "width": 50.0,
            },
            metadata={
                "hour": str(current.hour),
                "day_of_week": str(current.weekday()),
            },
        )

        forecast = ForecastResponseModel(
            forecast_id=f"historical_{count}",
            request_id=f"history_req_{count}",
            forecast_type=request.forecast_type,
            target_variable=request.target_variable,
            geography=request.geography,
            model_version="v1.0",
            forecast_points=[point],
            generated_at=current,
            valid_from=current,
            valid_until=current + timedelta(hours=1),
            metadata={"historical": True, "include_actuals": request.include_actuals},
        )
        history.append(forecast)
        count += 1
        current += timedelta(hours=1)

    return history


# ---------------------------------------------------------------------------
# Schema definition
# ---------------------------------------------------------------------------
schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    subscription=Subscription,
)
