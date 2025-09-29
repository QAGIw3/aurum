"""Async queue abstractions with observability."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Generic, Optional, TypeVar

T = TypeVar("T")


@dataclass(slots=True)
class QueueStats:
    size: int
    in_flight: int
    maxsize: int
    pending_shutdown: bool


@dataclass(slots=True)
class AsyncWorkQueue(Generic[T]):
    """Wrapper around `asyncio.Queue` providing graceful shutdown and hooks."""

    name: str
    maxsize: int = 0
    monitor: Optional[Callable[[QueueStats], Awaitable[None] | None]] = None
    _queue: asyncio.Queue[T] = field(init=False, repr=False)
    _in_flight: int = field(default=0, init=False, repr=False)
    _shutdown: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)

    def __post_init__(self) -> None:
        self._queue = asyncio.Queue(maxsize=self.maxsize)

    def _emit(self) -> None:
        if not self.monitor:
            return
        stats = QueueStats(
            size=self._queue.qsize(),
            in_flight=self._in_flight,
            maxsize=self.maxsize,
            pending_shutdown=self._shutdown.is_set(),
        )
        maybe = self.monitor(stats)
        if asyncio.iscoroutine(maybe):
            asyncio.create_task(maybe)

    async def put(self, item: T) -> None:
        await self._queue.put(item)
        self._emit()

    async def put_nowait(self, item: T) -> None:
        self._queue.put_nowait(item)
        self._emit()

    async def get(self) -> T:
        item = await self._queue.get()
        self._in_flight += 1
        self._emit()
        return item

    def task_done(self) -> None:
        self._queue.task_done()
        self._in_flight = max(0, self._in_flight - 1)
        self._emit()

    async def join(self) -> None:
        await self._queue.join()

    def initiate_shutdown(self) -> None:
        self._shutdown.set()
        self._emit()

    async def consume(self, handler: Callable[[T], Awaitable[Any]]) -> None:
        """Continuously consume until shutdown and queue drained."""
        try:
            while True:
                if self._shutdown.is_set() and self._queue.empty():
                    break
                item = await self.get()
                try:
                    await handler(item)
                finally:
                    self.task_done()
        except asyncio.CancelledError:
            raise

    def snapshot(self) -> QueueStats:
        return QueueStats(
            size=self._queue.qsize(),
            in_flight=self._in_flight,
            maxsize=self.maxsize,
            pending_shutdown=self._shutdown.is_set(),
        )

    async def __aiter__(self) -> AsyncIterator[T]:
        while True:
            if self._shutdown.is_set() and self._queue.empty():
                return
            yield await self.get()


__all__ = [
    "AsyncWorkQueue",
    "QueueStats",
]
