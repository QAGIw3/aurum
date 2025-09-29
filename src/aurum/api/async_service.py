"""Async background task service using standardized execution primitives."""
from __future__ import annotations

import asyncio
import random
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from .async_exec.monitoring import MonitoringHooks, observe_async
from .async_exec.supervisor import BackgroundTaskSupervisor, SupervisorConfig


TaskFactory = Callable[[], Awaitable[Any]]


@dataclass(slots=True)
class TaskSpec:
    name: str
    factory: TaskFactory
    restart: bool = False
    tags: Dict[str, str] = field(default_factory=dict)


class AsyncService:
    """Lifecycle manager for background tasks with observability."""

    def __init__(
        self,
        *,
        supervisor: BackgroundTaskSupervisor | None = None,
        monitoring: MonitoringHooks | None = None,
        config: SupervisorConfig | None = None,
    ) -> None:
        self._supervisor = supervisor or BackgroundTaskSupervisor(config=config)
        self._monitoring = monitoring or MonitoringHooks()
        self._tasks: dict[str, TaskSpec] = {}
        self._started = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            if self._started:
                return
            await self._supervisor.start()
            for spec in list(self._tasks.values()):
                self._spawn(spec)
            self._started = True

    async def stop(self) -> None:
        async with self._lock:
            if not self._started:
                return
            await self._supervisor.stop()
            self._started = False

    def spawn(
        self,
        name: str,
        factory: TaskFactory,
        *,
        restart: bool = False,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        spec = TaskSpec(name=name, factory=factory, restart=restart, tags=tags or {})
        self._tasks[name] = spec
        if self._started:
            self._spawn(spec)

    def schedule_periodic(
        self,
        name: str,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        interval_s: float,
        jitter_s: float = 0.0,
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        async def _runner() -> None:
            while True:
                async with observe_async(
                    hooks=self._monitoring,
                    task_name=name,
                    labels={**(tags or {}), "kind": "periodic"},
                ):
                    await coro_factory()
                sleep_for = interval_s
                if jitter_s:
                    sleep_for += random.uniform(-jitter_s, jitter_s)
                await asyncio.sleep(max(sleep_for, 0.0))

        self.spawn(name, _runner, restart=True, tags={**(tags or {}), "periodic": "true"})

    def health(self) -> dict[str, Any]:
        return self._supervisor.health()

    def _spawn(self, spec: TaskSpec) -> None:
        labels = {"task": spec.name, **spec.tags}

        async def _wrapped() -> Any:
            async with observe_async(hooks=self._monitoring, task_name=spec.name, labels=labels):
                return await spec.factory()

        self._supervisor.spawn(spec.name, _wrapped, restart=spec.restart, tags=spec.tags)


async_service = AsyncService()

# Backwards-compatible aliases expected by some tests/modules
AsyncScenarioService = AsyncService  # type: ignore
AsyncCurveService = AsyncService  # type: ignore


@asynccontextmanager
async def lifecycle() -> Awaitable[AsyncService]:
    await async_service.start()
    try:
        yield async_service
    finally:
        await async_service.stop()
