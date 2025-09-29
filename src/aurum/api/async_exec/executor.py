"""Async execution primitives for Aurum API."""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Sequence
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, Optional, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass(slots=True)
class ExecutorConfig:
    """Runtime configuration defaults for async execution helpers."""

    io_timeout_s: float = 30.0
    cpu_timeout_s: float = 60.0
    cpu_pool_size: Optional[int] = None
    gather_limit: int = 20


class AsyncExecutor:
    """Helper façade around asyncio primitives to enforce shared conventions."""

    def __init__(
        self,
        *,
        config: ExecutorConfig | None = None,
        cpu_executor: Executor | None = None,
    ) -> None:
        self._config = config or ExecutorConfig()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._cpu_executor = cpu_executor
        self._owns_cpu_executor = cpu_executor is None

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @property
    def cpu_executor(self) -> Executor | None:
        if self._cpu_executor is None and self._owns_cpu_executor:
            self._cpu_executor = ProcessPoolExecutor(max_workers=self._config.cpu_pool_size)
        return self._cpu_executor

    async def run_io_bound(
        self,
        fn: Callable[..., R],
        /,
        *args: Any,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> R:
        """Execute blocking I/O in a thread using `asyncio.to_thread` with timeout."""
        timeout = timeout_s or self._config.io_timeout_s
        coro = asyncio.to_thread(fn, *args, **kwargs)
        if timeout:
            async with asyncio.timeout(timeout):
                return await coro
        return await coro

    async def run_cpu_bound(
        self,
        fn: Callable[..., R],
        /,
        *args: Any,
        timeout_s: float | None = None,
        **kwargs: Any,
    ) -> R:
        """Execute CPU-intensive work in a process pool with timeout."""
        loop = self.loop
        executor = self.cpu_executor
        run = partial(fn, *args, **kwargs)
        coro = loop.run_in_executor(executor, run)
        timeout = timeout_s or self._config.cpu_timeout_s
        if timeout:
            async with asyncio.timeout(timeout):
                return await coro
        return await coro

    async def bounded_gather(
        self,
        coros: Iterable[Awaitable[T]],
        *,
        limit: int | None = None,
        return_exceptions: bool = False,
    ) -> list[T | BaseException]:
        """Gather awaitables with a concurrency semaphore."""
        limit_value = limit or self._config.gather_limit
        if limit_value <= 0:
            raise ValueError("gather concurrency limit must be positive")
        sem = asyncio.Semaphore(limit_value)

        async def _run(coro: Awaitable[T]) -> T:
            async with sem:
                return await coro

        return await asyncio.gather(
            *(_run(coro) for coro in coros),
            return_exceptions=return_exceptions,
        )

    def spawn_supervised(
        self,
        coro_factory: Callable[[], Awaitable[Any]],
        *,
        name: str | None = None,
        restart: bool = False,
        backoff: Sequence[float] | None = None,
        on_error: Callable[[BaseException], Awaitable[None] | None] | None = None,
    ) -> asyncio.Task[Any]:
        """Create a monitored task with optional restart and backoff semantics."""
        delays = list(backoff or ())

        async def _runner() -> Any:
            attempt = 0
            while True:
                try:
                    return await coro_factory()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # pragma: no cover - surfaced to caller
                    if on_error:
                        maybe = on_error(exc)
                        if asyncio.iscoroutine(maybe):
                            await maybe
                    if not restart:
                        raise
                    delay = delays[min(attempt, len(delays) - 1)] if delays else 0.0
                    attempt += 1
                    if delay:
                        await asyncio.sleep(delay)

        return asyncio.create_task(_runner(), name=name)

    async def aclose(self) -> None:
        """Clean up owned resources such as process pools."""
        if self._owns_cpu_executor and self._cpu_executor:
            executor = self._cpu_executor
            self._cpu_executor = None
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, executor.shutdown)


_default_executor = AsyncExecutor()


async def run_io_bound(
    fn: Callable[..., R],
    /,
    *args: Any,
    timeout_s: float | None = None,
    **kwargs: Any,
) -> R:
    return await _default_executor.run_io_bound(fn, *args, timeout_s=timeout_s, **kwargs)


async def run_cpu_bound(
    fn: Callable[..., R],
    /,
    *args: Any,
    timeout_s: float | None = None,
    **kwargs: Any,
) -> R:
    return await _default_executor.run_cpu_bound(fn, *args, timeout_s=timeout_s, **kwargs)


async def bounded_gather(
    coros: Iterable[Awaitable[T]],
    *,
    limit: int | None = None,
    return_exceptions: bool = False,
) -> list[T | BaseException]:
    return await _default_executor.bounded_gather(
        coros,
        limit=limit,
        return_exceptions=return_exceptions,
    )


def spawn_supervised(
    coro_factory: Callable[[], Awaitable[Any]],
    *,
    name: str | None = None,
    restart: bool = False,
    backoff: Sequence[float] | None = None,
    on_error: Callable[[BaseException], Awaitable[None] | None] | None = None,
) -> asyncio.Task[Any]:
    return _default_executor.spawn_supervised(
        coro_factory,
        name=name,
        restart=restart,
        backoff=backoff,
        on_error=on_error,
    )


__all__ = [
    "AsyncExecutor",
    "ExecutorConfig",
    "bounded_gather",
    "run_cpu_bound",
    "run_io_bound",
    "spawn_supervised",
]
