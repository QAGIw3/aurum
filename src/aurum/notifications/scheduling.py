"""Scheduling primitives for deferred and batched notifications."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Awaitable, Callable, List, Optional

from aurum.logging import LogLevel, create_logger

from .multi_channel import Notification


@dataclass(slots=True)
class ScheduledNotification:
    """Notification scheduled for future delivery."""

    notification: Notification
    trigger_at: datetime
    description: str = ""


class NotificationScheduler:
    """Simple in-memory scheduler for deferred notifications."""

    def __init__(
        self,
        enqueue_callback: Callable[[Notification], Awaitable[None]],
        *,
        poll_interval: float = 5.0,
    ) -> None:
        self._enqueue = enqueue_callback
        self._poll_interval = max(1.0, float(poll_interval))
        self._queue: List[ScheduledNotification] = []
        self._task: Optional[asyncio.Task[None]] = None
        self._running = asyncio.Event()
        self._logger = create_logger("notifications.scheduler")

    async def start(self) -> None:
        if self._task is not None:
            return
        self._running.set()
        self._task = asyncio.create_task(self._run(), name="notification-scheduler")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._running.clear()
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None

    async def schedule(self, scheduled: ScheduledNotification) -> None:
        self._queue.append(scheduled)
        self._queue.sort(key=lambda item: item.trigger_at)
        self._logger.log(
            LogLevel.INFO,
            "Notification scheduled",
            event_type="notification_schedule_create",
            notification_id=scheduled.notification.notification_id,
            trigger_at=scheduled.trigger_at.isoformat(),
        )

    async def _run(self) -> None:
        try:
            while self._running.is_set():
                now = datetime.now(timezone.utc)
                ready = [item for item in self._queue if item.trigger_at <= now]
                self._queue = [item for item in self._queue if item.trigger_at > now]
                for item in ready:
                    await self._enqueue(item.notification)
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            raise


__all__ = ["NotificationScheduler", "ScheduledNotification"]
