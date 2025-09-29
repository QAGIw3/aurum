from __future__ import annotations

"""Per-request resource tracking and cleanup middleware.

This middleware installs a context-local ResourceTracker that components can
use to register acquired resources (e.g., DB connections) with an async
cleanup callback. On request completion, any unreleased resources are
automatically cleaned up and a warning is logged.
"""

import asyncio
import logging
from contextvars import ContextVar
from typing import Any, Awaitable, Callable, Dict, Optional

from starlette.types import ASGIApp
from aurum.observability.metrics import increment_resource_leaks_cleaned


LOGGER = logging.getLogger(__name__)

_current_tracker: ContextVar["ResourceTracker" | None] = ContextVar("resource_tracker", default=None)


class ResourceTracker:
    """Track resources acquired during a request for leak-proof cleanup."""

    def __init__(self) -> None:
        self._cleanup_callbacks: Dict[str, Callable[[], Awaitable[None]]] = {}
        self._released: set[str] = set()
        self._lock = asyncio.Lock()

    async def register(self, key: str, cleanup: Callable[[], Awaitable[None]]) -> None:
        """Register a resource with an async cleanup callback.

        The key should uniquely identify the resource (e.g., id(connection)).
        """

        async with self._lock:
            self._cleanup_callbacks[key] = cleanup

    async def mark_released(self, key: str) -> None:
        """Mark a resource as released so it won't be double-cleaned."""

        async with self._lock:
            self._released.add(key)
            self._cleanup_callbacks.pop(key, None)

    async def cleanup(self) -> int:
        """Cleanup any unreleased resources. Returns number of cleaned items."""

        async with self._lock:
            pending = dict(self._cleanup_callbacks)
            self._cleanup_callbacks.clear()

        leaks = 0
        for key, cb in pending.items():
            try:
                await cb()
                leaks += 1
            except Exception:
                # Best-effort cleanup; continue
                LOGGER.warning("resource_cleanup_failed", extra={"key": key}, exc_info=True)
        return leaks


def get_current_resource_tracker() -> Optional[ResourceTracker]:
    """Get the current request's ResourceTracker, if any."""

    return _current_tracker.get()


async def resource_cleanup_middleware(request, call_next):  # type: ignore[no-untyped-def]
    """HTTP middleware that ensures resource cleanup per request."""

    tracker = ResourceTracker()
    token = _current_tracker.set(tracker)
    try:
        response = await call_next(request)
        return response
    finally:
        try:
            leaks = await tracker.cleanup()
            if leaks:
                LOGGER.warning("resource_leaks_cleaned", extra={"count": leaks, "path": request.url.path})
                try:
                    await increment_resource_leaks_cleaned("api", leaks)
                except Exception:
                    pass
        finally:
            _current_tracker.reset(token)


__all__ = [
    "resource_cleanup_middleware",
    "get_current_resource_tracker",
    "ResourceTracker",
]


