"""Auto-reforecast scheduler stub.

Provides a minimal interface to be expanded for cluster scheduling.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional
from uuid import UUID

from aurum.core import AurumSettings
from aurum.api.database.auto_reforecast import get_auto_reforecast_job_repository
from aurum.api.services.auto_reforecast_shim import get_auto_reforecast_service


class AutoReforecastScheduler:
    """Simple scheduler that polls for due pending jobs and lets the service process them."""

    def __init__(self, settings: Optional[AurumSettings] = None) -> None:
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._settings = settings

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception:
                pass
            self._task = None

    @property
    def is_running(self) -> bool:
        return self._running

    async def _loop(self) -> None:
        service = get_auto_reforecast_service()
        repo = get_auto_reforecast_job_repository(self._settings)
        # The service already processes an in-memory queue. Here we can, in future,
        # fetch DB jobs and re-enqueue if needed. For now, just tick every second.
        while self._running:
            try:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5.0)


_scheduler_singleton: Optional[AutoReforecastScheduler] = None


def get_auto_reforecast_scheduler(settings: Optional[AurumSettings] = None) -> AutoReforecastScheduler:
    global _scheduler_singleton
    if _scheduler_singleton is None:
        _scheduler_singleton = AutoReforecastScheduler(settings)
    return _scheduler_singleton
