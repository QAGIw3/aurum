"""Analytics helpers for the notification system."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Mapping, Optional

from aurum.events.streaming import EventBus, EventEnvelope
from aurum.logging import LogLevel, create_logger

from .multi_channel import DeliveryResult, DeliveryStatus


class NotificationAnalyticsRecorder:
    """Publishes analytics events derived from delivery results."""

    def __init__(
        self,
        event_bus: EventBus,
        *,
        analytics_topic: str = "aurum.notifications.analytics.v1",
    ) -> None:
        self._event_bus = event_bus
        self._topic = analytics_topic
        self._logger = create_logger("notifications.analytics")

    async def record_delivery(self, result: DeliveryResult, extra: Optional[Mapping[str, str]] = None) -> None:
        event_type = self._map_status(result.status)
        payload = {
            "id": result.attempt.attempt_id,
            "dispatch_id": result.attempt.notification.notification_id,
            "tenant_id": result.attempt.notification.tenant_id,
            "recipient_id": result.attempt.destination.recipient_id,
            "channel": result.attempt.channel.value,
            "event_type": event_type,
            "event_at": self._now_micros(),
            "attributes": {**{k: str(v) for k, v in (extra or {}).items()}},
            "latency_ms": self._compute_latency(result),
        }
        envelope = EventEnvelope(
            topic=self._topic,
            payload=payload,
            key=result.attempt.notification.notification_id,
            headers={
                "notification_id": result.attempt.notification.notification_id,
                "channel": result.attempt.channel.value,
            },
            schema_subject="aurum.notifications.analytics.v1-value",
            schema_version=1,
        )
        await self._event_bus.publish(envelope)
        self._logger.log(
            LogLevel.DEBUG,
            "Recorded notification analytics event",
            event_type="notification_analytics_recorded",
            notification_id=result.attempt.notification.notification_id,
            status=result.status.value,
        )

    async def record_ack(
        self,
        *,
        dispatch_id: str,
        tenant_id: str,
        recipient_id: str,
        attributes: Optional[Mapping[str, str]] = None,
    ) -> None:
        payload = {
            "id": str(uuid.uuid4()),
            "dispatch_id": dispatch_id,
            "tenant_id": tenant_id,
            "recipient_id": recipient_id,
            "channel": "ack",
            "event_type": "acknowledged",
            "event_at": self._now_micros(),
            "attributes": {**{k: str(v) for k, v in (attributes or {}).items()}},
            "latency_ms": None,
        }
        envelope = EventEnvelope(
            topic=self._topic,
            payload=payload,
            key=dispatch_id,
            headers={
                "notification_id": dispatch_id,
                "tenant_id": tenant_id,
                "event_type": "acknowledged",
            },
            schema_subject="aurum.notifications.analytics.v1-value",
            schema_version=1,
        )
        await self._event_bus.publish(envelope)

    @staticmethod
    def _map_status(status: DeliveryStatus) -> str:
        if status == DeliveryStatus.SENT:
            return "delivered"
        if status == DeliveryStatus.DELIVERED:
            return "delivered"
        if status == DeliveryStatus.FAILED:
            return "failed"
        if status == DeliveryStatus.SUPPRESSED:
            return "suppressed"
        if status == DeliveryStatus.DEFERRED:
            return "suppressed"
        return "delivered"

    @staticmethod
    def _compute_latency(result: DeliveryResult) -> Optional[int]:
        if result.attempt.completed_at is None:
            return None
        delta = result.attempt.completed_at - result.attempt.queued_at
        return int(delta.total_seconds() * 1000)

    @staticmethod
    def _now_micros() -> int:
        return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


__all__ = ["NotificationAnalyticsRecorder"]
