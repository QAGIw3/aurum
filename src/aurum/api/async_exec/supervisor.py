"""Background task supervision utilities."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(slots=True)
class TaskRecord:
    task: asyncio.Task[Any]
    name: str
    tags: Dict[str, str]
    restart: bool
    restarts: int = 0
    last_error: Optional[str] = None


@dataclass(slots=True)
class SupervisorConfig:
    shutdown_grace_s: float = 20.0
    restart_backoff: tuple[float, ...] = (1.0, 2.0, 5.0)


class BackgroundTaskSupervisor:
    """Manage background tasks with lifecycle and restart policies."""

    def __init__(self, *, config: SupervisorConfig | None = None) -> None:
        self._config = config or SupervisorConfig()
        self._tasks: dict[str, TaskRecord] = {}
        self._running = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        async with self._lock:
            self._running = True

    async def stop(self) -> None:
        async with self._lock:
            self._running = False
            tasks = list(self._tasks.values())
            for record in tasks:
                record.task.cancel()
            try:
                async with asyncio.timeout(self._config.shutdown_grace_s):
                    await asyncio.gather(*(record.task for record in tasks), return_exceptions=True)
            except TimeoutError:
                # Force cancellation if tasks ignore graceful timeout
                for record in tasks:
                    if not record.task.done():
                        record.task.cancel()
            finally:
                self._tasks.clear()

    def spawn(
        self,
        name: str,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        restart: bool = False,
        tags: Optional[Dict[str, str]] = None,
        on_error: Optional[Callable[[BaseException], Awaitable[None] | None]] = None,
    ) -> asyncio.Task[Any]:
        if not self._running:
            raise RuntimeError("supervisor not started")
        backoff = self._config.restart_backoff
        record = TaskRecord(task=asyncio.get_running_loop().create_task(asyncio.sleep(0)), name=name, tags=tags or {}, restart=restart)  # placeholder

        async def _runner() -> Any:
            attempt = 0
            nonlocal record
            while True:
                try:
                    return await coro_factory()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    record.last_error = str(exc)
                    if on_error:
                        maybe = on_error(exc)
                        if asyncio.iscoroutine(maybe):
                            await maybe
                    if not restart:
                        raise
                    record.restarts += 1
                    delay = backoff[min(attempt, len(backoff) - 1)] if backoff else 0.0
                    attempt += 1
                    if delay:
                        await asyncio.sleep(delay)

        task = asyncio.create_task(_runner(), name=name)
        record = TaskRecord(task=task, name=name, tags=tags or {}, restart=restart)
        self._tasks[name] = record

        def _done_callback(t: asyncio.Task[Any]) -> None:
            if t.cancelled():
                self._tasks.pop(name, None)
                return
            exc = t.exception()
            if exc:
                record.last_error = str(exc)
                if on_error:
                    maybe = on_error(exc)
                    if asyncio.iscoroutine(maybe):
                        asyncio.create_task(maybe)
            self._tasks.pop(name, None)

        task.add_done_callback(_done_callback)
        return task

    def health(self) -> dict[str, Any]:
        return {
            "running": self._running,
            "in_flight": len(self._tasks),
            "tasks": {
                name: {
                    "done": record.task.done(),
                    "cancelled": record.task.cancelled(),
                    "tags": record.tags,
                    "restart": record.restart,
                    "restarts": record.restarts,
                    "last_error": record.last_error,
                }
                for name, record in self._tasks.items()
            },
        }


__all__ = [
    "BackgroundTaskSupervisor",
    "SupervisorConfig",
]
