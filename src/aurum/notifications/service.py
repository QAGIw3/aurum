"""Thin service wrapper around the notification pipeline."""

from __future__ import annotations

import asyncio
from typing import Any, Mapping, Optional, Sequence

from .multi_channel import NotificationChannel, NotificationPriority
from .pipeline import NotificationPipelineConfig, NotificationsEventPipeline


class NotificationService:
    """High-level façade for scheduling notifications."""

    def __init__(
        self,
        pipeline: Optional[NotificationsEventPipeline] = None,
        *,
        auto_start: bool = False,
    ) -> None:
        self._pipeline = pipeline or NotificationsEventPipeline(NotificationPipelineConfig())
        self._auto_start = auto_start
        self._started = False
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        if not self._auto_start:
            return
        async with self._lock:
            if self._started:
                return
            await self._pipeline.start()
            self._started = True

    async def stop(self) -> None:
        if not self._auto_start or not self._started:
            return
        async with self._lock:
            if not self._started:
                return
            await self._pipeline.stop()
            self._started = False

    async def enqueue(
        self,
        *,
        tenant_id: str,
        template_id: str,
        recipients: Sequence[Mapping[str, Any]],
        data: Mapping[str, Any],
        priority: NotificationPriority = NotificationPriority.NORMAL,
        channels: Optional[Sequence[str | NotificationChannel]] = None,
        schedule_at=None,
        deduplication_key: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        template_locale: Optional[str] = None,
    ) -> str:
        if self._auto_start and not self._started:
            await self.start()
        return await self._pipeline.enqueue_notification(
            tenant_id=tenant_id,
            template_id=template_id,
            recipients=recipients,
            data=data,
            priority=priority,
            channels=channels,
            schedule_at=schedule_at,
            deduplication_key=deduplication_key,
            metadata=metadata,
            template_locale=template_locale,
        )

    async def record_ack(
        self,
        *,
        dispatch_id: str,
        tenant_id: str,
        recipient_id: str,
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> None:
        await self._pipeline.record_ack(
            dispatch_id=dispatch_id,
            tenant_id=tenant_id,
            recipient_id=recipient_id,
            attributes=attributes,
        )


_service: Optional[NotificationService] = None


async def get_notification_service() -> NotificationService:
    global _service
    if _service is None:
        _service = NotificationService()
    return _service


__all__ = ["NotificationService", "get_notification_service"]
