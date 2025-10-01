"""Event-driven pipeline for the notification system."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional, Sequence

from aurum.events.streaming import (
    EventBus,
    IdempotencyTracker,
    KafkaEventBus,
    OutboxMessage,
    OutboxRepository,
    SchemaValidator,
    build_outbox_runtime,
)
from aurum.logging import LogLevel, create_logger

from .analytics import NotificationAnalyticsRecorder
from .intelligent_routing import RoutingContext, RoutingEngine, RoutingPreferences, Severity
from .multi_channel import (
    DeliveryResult,
    MultiChannelDispatcher,
    Notification,
    NotificationChannel,
    NotificationDestination,
    NotificationPriority,
)
from .scheduling import NotificationScheduler
from .templates import TemplateRegistry


@dataclass(slots=True)
class NotificationPipelineConfig:
    """Configuration values for the notification pipeline."""

    dispatch_topic: str = "aurum.notifications.dispatch.v1"
    delivery_topic: str = "aurum.notifications.delivery.v1"
    analytics_topic: str = "aurum.notifications.analytics.v1"
    consumer_group: str = "aurum-notifications"
    bootstrap_servers: Optional[str] = None
    batch_size: int = 100
    poll_interval: float = 1.0
    in_memory: bool = False
    dlq_topic: Optional[str] = None
    template_dir: str = "config/notifications/templates"


class NotificationsEventPipeline:
    """Wires the outbox, router, dispatcher, and analytics components."""

    def __init__(
        self,
        config: Optional[NotificationPipelineConfig] = None,
        *,
        repository: Optional[OutboxRepository] = None,
        event_bus: Optional[EventBus] = None,
        dispatcher: Optional[MultiChannelDispatcher] = None,
        routing_engine: Optional[RoutingEngine] = None,
        template_registry: Optional[TemplateRegistry] = None,
        analytics: Optional[NotificationAnalyticsRecorder] = None,
        scheduler: Optional[NotificationScheduler] = None,
        processor: Optional[KafkaProcessor] = None,
        processor_config: Optional[KafkaProcessorConfig] = None,
        idempotency_tracker: Optional[IdempotencyTracker] = None,
        schema_validator: Optional[SchemaValidator] = None,
    ) -> None:
        self.config = config or NotificationPipelineConfig()
        self._logger = create_logger("notifications.pipeline")

        if schema_validator is None and event_bus is None:
            schema_validator = SchemaValidator(enforce=False)

        resolved_event_bus = event_bus or KafkaEventBus(schema_validator=schema_validator)
        self._dispatcher = dispatcher or MultiChannelDispatcher.from_config(
            event_bus=resolved_event_bus,
            config_dir="config/notifications",
        )
        self._routing_engine = routing_engine or RoutingEngine.from_config()
        self._template_registry = template_registry or TemplateRegistry()
        if template_registry is None:
            self._template_registry.load_directory(self.config.template_dir)
        self._analytics = analytics or NotificationAnalyticsRecorder(
            resolved_event_bus, analytics_topic=self.config.analytics_topic
        )
        self._scheduler = scheduler or NotificationScheduler(self._schedule_notification)

        runtime = build_outbox_runtime(
            topic=self.config.dispatch_topic,
            consumer_group=self.config.consumer_group,
            bootstrap_servers=self.config.bootstrap_servers,
            batch_size=self.config.batch_size,
            poll_interval=self.config.poll_interval,
            in_memory=self.config.in_memory,
            repository=repository,
            event_bus=resolved_event_bus,
            processor=processor,
            processor_config=processor_config,
            idempotency_tracker=idempotency_tracker,
            schema_validator=schema_validator,
        )

        self._event_bus = runtime.event_bus
        self._repository = runtime.repository
        self._processor_config = runtime.processor_config
        self._processor = runtime.processor
        self._idempotency = runtime.idempotency_tracker

        self.consumer = runtime.consumer
        self.consumer.register_handler(self.config.dispatch_topic, self._handle_dispatch)

        self.dispatcher = runtime.dispatcher

    async def start(
        self,
        *,
        start_dispatcher: bool = True,
        start_consumer: bool = True,
    ) -> None:
        if start_dispatcher:
            await self.dispatcher.start()
        if start_consumer:
            await self.consumer.start()
        await self._scheduler.start()

    async def stop(self) -> None:
        await self.dispatcher.stop()
        await self.consumer.stop()
        await self._scheduler.stop()

    async def flush(self) -> int:
        return await self.dispatcher.drain_once()

    async def enqueue_notification(
        self,
        *,
        tenant_id: str,
        template_id: str,
        recipients: Sequence[Mapping[str, Any]],
        data: Mapping[str, Any],
        priority: NotificationPriority = NotificationPriority.NORMAL,
        channels: Optional[Sequence[str | NotificationChannel]] = None,
        schedule_at: Optional[datetime] = None,
        deduplication_key: Optional[str] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        template_locale: Optional[str] = None,
    ) -> str:
        notification_id = str(uuid.uuid4())
        payload = {
            "id": notification_id,
            "tenant_id": tenant_id,
            "priority": priority.value,
            "template_id": template_id,
            "template_locale": template_locale,
            "recipients": [self._serialise_recipient(rec) for rec in recipients],
            "channels": [self._channel_value(ch) for ch in (channels or [c.value for c in NotificationChannel])],
            "data": {str(k): str(v) for k, v in data.items()},
            "schedule_at": self._timestamp_micros(schedule_at) if schedule_at else None,
            "fingerprint": deduplication_key,
            "metadata": {str(k): str(v) for k, v in (metadata or {}).items()},
        }
        message = OutboxMessage(
            topic=self.config.dispatch_topic,
            payload=payload,
            key=notification_id,
            headers={
                "notification_id": notification_id,
                "tenant_id": tenant_id,
                "priority": priority.value,
            },
            schema_subject="aurum.notifications.dispatch.v1-value",
            schema_version=1,
            scheduled_at=schedule_at or datetime.now(timezone.utc),
        )
        await self._repository.enqueue(message)
        return notification_id

    async def _handle_dispatch(self, message) -> None:
        payload = self._extract_payload(message)
        notification = self._build_notification(payload)
        context = RoutingContext(
            severity=self._map_priority(notification.priority),
            event_time=datetime.now(timezone.utc),
            fingerprint=payload.get("fingerprint"),
            requires_ack=payload.get("requires_ack", False),
            ack_timeout=self._parse_timeout(payload.get("ack_timeout_seconds")),
            metadata=payload.get("metadata", {}),
        )
        preferences: Mapping[str, RoutingPreferences] = {}
        plan = await self._routing_engine.build_plan(notification, context, preferences)
        results = await self._dispatcher.dispatch(notification, plan)
        for result in results:
            await self._analytics.record_delivery(result)

    async def _schedule_notification(self, notification: Notification) -> None:
        payload = {
            "id": notification.notification_id,
            "tenant_id": notification.tenant_id,
            "priority": notification.priority.value,
            "template_id": notification.template_id,
            "template_locale": None,
            "recipients": [
                {
                    "id": dest.recipient_id,
                    "address": dest.address,
                    "channels": [channel.value for channel in dest.channels],
                }
                for dest in notification.destinations
            ],
            "channels": [channel.value for channel in notification.channel_content.keys()],
            "data": {"scheduled": "true"},
            "fingerprint": notification.deduplication_key,
        }
        message = OutboxMessage(
            topic=self.config.dispatch_topic,
            payload=payload,
            key=notification.notification_id,
            headers={"notification_id": notification.notification_id},
            schema_subject="aurum.notifications.dispatch.v1-value",
            schema_version=1,
        )
        await self._repository.enqueue(message)

    async def record_ack(
        self,
        *,
        dispatch_id: str,
        tenant_id: str,
        recipient_id: str,
        attributes: Optional[Mapping[str, Any]] = None,
    ) -> None:
        await self._analytics.record_ack(
            dispatch_id=dispatch_id,
            tenant_id=tenant_id,
            recipient_id=recipient_id,
            attributes=attributes,
        )

    def _build_notification(self, payload: Mapping[str, Any]) -> Notification:
        priority = self._parse_priority(payload.get("priority"))
        destinations = [
            NotificationDestination(
                recipient_id=recipient.get("id"),
                address=recipient.get("address"),
                channels=tuple(self._parse_channel_list(recipient.get("channels"))),
                metadata=recipient.get("metadata", {}),
            )
            for recipient in payload.get("recipients", [])
        ]
        channels = self._parse_channel_list(payload.get("channels"))
        context_data = dict(payload.get("data", {}))
        locale = payload.get("template_locale")
        channel_content = {}
        for channel in channels:
            channel_content[channel] = self._template_registry.render(
                payload.get("template_id"),
                channel,
                context_data,
                locale=locale,
            )
        return Notification(
            notification_id=payload.get("id"),
            tenant_id=payload.get("tenant_id"),
            priority=priority,
            template_id=payload.get("template_id"),
            destinations=tuple(destinations),
            channel_content=channel_content,
            metadata=payload.get("metadata", {}),
            deduplication_key=payload.get("fingerprint"),
        )

    def _extract_payload(self, message) -> Mapping[str, Any]:
        value = message.value
        if isinstance(value, (bytes, bytearray)):
            value = json.loads(value.decode("utf-8"))
        return value

    @staticmethod
    def _parse_priority(value: Optional[str]) -> NotificationPriority:
        try:
            if value is None:
                return NotificationPriority.NORMAL
            return NotificationPriority(value)
        except ValueError:
            return NotificationPriority.NORMAL

    @staticmethod
    def _map_priority(priority: NotificationPriority) -> Severity:
        mapping = {
            NotificationPriority.LOW: Severity.LOW,
            NotificationPriority.NORMAL: Severity.MEDIUM,
            NotificationPriority.HIGH: Severity.HIGH,
            NotificationPriority.CRITICAL: Severity.CRITICAL,
        }
        return mapping.get(priority, Severity.MEDIUM)

    @staticmethod
    def _parse_channel_list(channels: Optional[Sequence[str | NotificationChannel]]) -> Sequence[NotificationChannel]:
        if not channels:
            return (NotificationChannel.EMAIL,)
        parsed = []
        for channel in channels:
            if isinstance(channel, NotificationChannel):
                parsed.append(channel)
            else:
                try:
                    parsed.append(NotificationChannel(channel))
                except ValueError:
                    parsed.append(NotificationChannel[channel.upper()])
        return tuple(parsed)

    @staticmethod
    def _serialise_recipient(data: Mapping[str, Any]) -> Mapping[str, Any]:
        channels = data.get("channels") or []
        return {
            "id": data.get("id"),
            "address": data.get("address"),
            "channels": [str(ch if isinstance(ch, NotificationChannel) else ch) for ch in channels],
            "metadata": {str(k): str(v) for k, v in data.get("metadata", {}).items()},
        }

    @staticmethod
    def _channel_value(channel: str | NotificationChannel) -> str:
        if isinstance(channel, NotificationChannel):
            return channel.value
        return str(channel)

    @staticmethod
    def _timestamp_micros(dt: datetime) -> int:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000)

    @staticmethod
    def _parse_timeout(value: Optional[int]) -> Optional[timedelta]:
        if value is None:
            return None
        return timedelta(seconds=int(value))


__all__ = ["NotificationPipelineConfig", "NotificationsEventPipeline"]
