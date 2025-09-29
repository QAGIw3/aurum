"""Saga orchestration primitives with Timescale persistence."""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Mapping, Optional

from aurum.api.database.timescale_client import AsyncTimescaleClient, get_timescale_client
from aurum.observability import tracing

LOGGER = logging.getLogger(__name__)

SagaAction = Callable[["SagaContext"], Awaitable[None]]
SagaCompensation = Callable[["SagaContext"], Awaitable[None]]


class SagaStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"
    CANCELLED = "cancelled"


@dataclass
class SagaStepResult:
    name: str
    status: SagaStatus
    started_at: datetime
    completed_at: datetime | None = None
    error: str | None = None
    attempts: int = 0


@dataclass
class SagaContext:
    """Runtime context shared between saga steps."""

    saga_id: str
    saga_type: str
    data: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)
    status: SagaStatus = SagaStatus.PENDING
    current_step: str | None = None
    history: List[SagaStepResult] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def for_step(self, name: str) -> None:
        self.current_step = name
        self.updated_at = datetime.now(timezone.utc)

    def record_step(self, result: SagaStepResult) -> None:
        self.history.append(result)
        self.updated_at = datetime.now(timezone.utc)


@dataclass(frozen=True)
class SagaStep:
    name: str
    action: SagaAction
    compensation: SagaCompensation | None = None
    retry_limit: int = 1
    retry_backoff: float = 1.0
    timeout: timedelta | None = None


class SagaStateStore:
    """Persistence abstraction for saga runtime state."""

    async def load(self, saga_id: str) -> SagaContext | None:
        raise NotImplementedError

    async def save(self, context: SagaContext) -> None:
        raise NotImplementedError

    async def update(self, context: SagaContext) -> None:
        raise NotImplementedError


class TimescaleSagaStateStore(SagaStateStore):
    """Timescale-backed saga store using the shared event store schema."""

    def __init__(self, client: AsyncTimescaleClient | None = None) -> None:
        self._client = client or get_timescale_client()
        self._lock = asyncio.Lock()
        self._schema_ready = False

    async def load(self, saga_id: str) -> SagaContext | None:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT saga_id, saga_type, state, status, version, updated_at
            FROM event_store_sagas
            WHERE saga_id = %(saga_id)s
            """,
            {"saga_id": saga_id},
        )
        if not rows:
            return None
        row = rows[0]
        state = self._decode(row.get("state"))
        metadata = state.get("metadata", {}) if isinstance(state, dict) else {}
        history = state.get("history", []) if isinstance(state, dict) else []
        created_at = state.get("created_at") if isinstance(state, dict) else None
        context = SagaContext(
            saga_id=row["saga_id"],
            saga_type=row["saga_type"],
            data=state.get("data", {}) if isinstance(state, dict) else {},
            metadata=metadata,
            status=SagaStatus(row["status"]),
            current_step=state.get("current_step"),
            history=[self._decode_history(entry) for entry in history],
            created_at=self._parse_datetime(created_at) if created_at else datetime.now(timezone.utc),
            updated_at=row["updated_at"],
        )
        return context

    async def save(self, context: SagaContext) -> None:
        await self._ensure_schema()
        payload = self._encode_context(context)
        await self._client.execute(
            """
            INSERT INTO event_store_sagas (
                saga_id, saga_type, state, status, version, updated_at
            ) VALUES (
                %(saga_id)s, %(saga_type)s, %(state)s::jsonb, %(status)s,
                1, %(updated_at)s
            )
            ON CONFLICT (saga_id) DO UPDATE
            SET state = EXCLUDED.state,
                status = EXCLUDED.status,
                version = event_store_sagas.version + 1,
                updated_at = EXCLUDED.updated_at
            """,
            {
                "saga_id": context.saga_id,
                "saga_type": context.saga_type,
                "state": payload,
                "status": context.status.value,
                "updated_at": context.updated_at,
            },
        )

    async def update(self, context: SagaContext) -> None:
        await self.save(context)

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._lock:
            if self._schema_ready:
                return
            await self._client.execute(
                """
                CREATE TABLE IF NOT EXISTS event_store_sagas (
                    saga_id TEXT PRIMARY KEY,
                    saga_type TEXT NOT NULL,
                    state JSONB NOT NULL,
                    status TEXT NOT NULL,
                    version BIGINT NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            self._schema_ready = True

    @staticmethod
    def _encode_context(context: SagaContext) -> str:
        def _default(obj: Any) -> str:
            if isinstance(obj, datetime):
                return obj.isoformat()
            if isinstance(obj, SagaStatus):
                return obj.value
            return str(obj)

        payload = {
            "data": context.data,
            "metadata": context.metadata,
            "current_step": context.current_step,
            "history": [
                {
                    "name": result.name,
                    "status": result.status.value,
                    "started_at": result.started_at,
                    "completed_at": result.completed_at,
                    "error": result.error,
                    "attempts": result.attempts,
                }
                for result in context.history
            ],
            "created_at": context.created_at,
        }
        return json.dumps(payload, default=_default)

    @staticmethod
    def _decode(raw: Any) -> Dict[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, dict):
            return raw
        try:
            return json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _decode_history(entry: Mapping[str, Any]) -> SagaStepResult:
        started_at = TimescaleSagaStateStore._parse_datetime(entry.get("started_at"))
        completed_at = TimescaleSagaStateStore._parse_datetime(entry.get("completed_at"))
        status_raw = entry.get("status", SagaStatus.PENDING.value)
        return SagaStepResult(
            name=str(entry.get("name", "unknown")),
            status=SagaStatus(status_raw),
            started_at=started_at,
            completed_at=completed_at,
            error=entry.get("error"),
            attempts=int(entry.get("attempts", 0)),
        )

    @staticmethod
    def _parse_datetime(value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                pass
        return datetime.now(timezone.utc)


class Saga:
    """Saga definition encapsulating ordered steps."""

    def __init__(self, name: str, steps: Sequence[SagaStep]) -> None:
        if not steps:
            raise ValueError("Saga requires at least one step")
        self.name = name
        self.steps = list(steps)

    async def run(
        self,
        context: SagaContext,
        store: SagaStateStore,
    ) -> SagaContext:
        context.status = SagaStatus.RUNNING
        context.metadata.setdefault("trace_id", tracing.get_current_trace_id())
        await store.update(context)
        compensation_stack: List[SagaStep] = []

        for step in self.steps:
            context.for_step(step.name)
            step_result = SagaStepResult(
                name=step.name,
                status=SagaStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
            await store.update(context)
            attempt = 0
            while attempt <= step.retry_limit:
                attempt += 1
                step_result.attempts = attempt
                try:
                    LOGGER.debug("Executing saga step %s attempt %s", step.name, attempt)
                    if step.timeout:
                        await asyncio.wait_for(step.action(context), timeout=step.timeout.total_seconds())
                    else:
                        await step.action(context)
                    step_result.status = SagaStatus.COMPLETED
                    step_result.completed_at = datetime.now(timezone.utc)
                    context.record_step(step_result)
                    compensation_stack.append(step)
                    await store.update(context)
                    break
                except asyncio.TimeoutError as exc:
                    LOGGER.error("Saga step %s timed out", step.name)
                    step_result.error = str(exc)
                except Exception as exc:  # pragma: no cover - defensive
                    LOGGER.exception("Saga step %s failed", step.name)
                    step_result.error = str(exc)
                if attempt > step.retry_limit:
                    step_result.status = SagaStatus.FAILED
                    context.record_step(step_result)
                    await store.update(context)
                    await self._compensate(compensation_stack, context, store)
                    context.status = SagaStatus.FAILED
                    await store.update(context)
                    raise
                await asyncio.sleep(step.retry_backoff * attempt)

        context.status = SagaStatus.COMPLETED
        context.current_step = None
        context.updated_at = datetime.now(timezone.utc)
        await store.update(context)
        return context

    async def _compensate(
        self,
        executed_steps: Sequence[SagaStep],
        context: SagaContext,
        store: SagaStateStore,
    ) -> None:
        if not executed_steps:
            return
        context.status = SagaStatus.COMPENSATING
        await store.update(context)
        for step in reversed(executed_steps):
            if step.compensation is None:
                continue
            try:
                await step.compensation(context)
            except Exception as exc:  # pragma: no cover - defensive
                LOGGER.exception("Saga compensation for %s failed", step.name)
                context.history.append(
                    SagaStepResult(
                        name=f"{step.name}.compensate",
                        status=SagaStatus.FAILED,
                        started_at=datetime.now(timezone.utc),
                        completed_at=datetime.now(timezone.utc),
                        error=str(exc),
                    )
                )
                await store.update(context)
                raise
        context.status = SagaStatus.COMPENSATED
        context.updated_at = datetime.now(timezone.utc)
        await store.update(context)


class SagaOrchestrator:
    """Coordinator responsible for running sagas and tracking state."""

    def __init__(
        self,
        store: SagaStateStore | None = None,
    ) -> None:
        self._store = store or TimescaleSagaStateStore()

    async def start_saga(
        self,
        saga: Saga,
        *,
        saga_id: str | None = None,
        data: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> SagaContext:
        identifier = saga_id or str(uuid.uuid4())
        context = SagaContext(
            saga_id=identifier,
            saga_type=saga.name,
            data=dict(data or {}),
            metadata=dict(metadata or {}),
        )
        await self._store.save(context)
        try:
            return await saga.run(context, self._store)
        except Exception:
            LOGGER.exception("Saga %s (%s) failed", saga.name, identifier)
            raise

    async def resume_saga(self, saga_id: str, saga: Saga) -> SagaContext:
        context = await self._store.load(saga_id)
        if context is None:
            raise LookupError(f"Saga {saga_id} not found")
        if context.status in {SagaStatus.COMPLETED, SagaStatus.COMPENSATED}:
            return context
        return await saga.run(context, self._store)


__all__ = [
    "Saga",
    "SagaContext",
    "SagaOrchestrator",
    "SagaStateStore",
    "SagaStatus",
    "SagaStep",
    "TimescaleSagaStateStore",
]
