"""Async testing utilities."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable


@asynccontextmanager
async def async_test_timeout(seconds: float) -> None:
    """Fail tests if the enclosed block exceeds the timeout."""
    async with asyncio.timeout(seconds):
        yield


async def simulate_cancellation(coro: Awaitable[Any], *, after_ms: int = 10) -> None:
    """Cancel an awaited coroutine after a delay."""
    task = asyncio.create_task(coro)
    await asyncio.sleep(after_ms / 1000)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        return


def supervisor_fixture(supervisor_cls: Callable[..., Any], **kwargs: Any):
    """Pytest fixture factory for background supervisors."""
    import pytest

    @pytest.fixture
    async def _fixture() -> Any:
        supervisor = supervisor_cls(**kwargs)
        await supervisor.start()
        yield supervisor
        await supervisor.stop()

    return _fixture


__all__ = [
    "async_test_timeout",
    "simulate_cancellation",
    "supervisor_fixture",
]
