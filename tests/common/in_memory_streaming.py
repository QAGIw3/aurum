"""In-memory implementations for streaming components used in tests."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, MutableMapping, Sequence

from aurum.events.streaming import EventBus, EventEnvelope, OutboxMessage, OutboxRepository
from aurum.streaming.kafka_processor import KafkaProcessor


@dataclass
class _OutboxRecord:
    topic: str
    partition_key: str | None
    payload: Any
    headers: MutableMapping[str, str]
    schema_subject: str | None
    schema_version: int | None
    scheduled_at: datetime
    attempts: int
    max_attempts: int
    published_at: datetime | None = None
    last_error: str | None = None


class InMemoryOutboxRepository(OutboxRepository):
    """Simple in-memory outbox for deterministic tests."""

    def __init__(self) -> None:
        self._records: Dict[str, _OutboxRecord] = {}

    async def enqueue(self, message: OutboxMessage) -> str:
        message_id = str(uuid.uuid4())
        record = _OutboxRecord(
            topic=message.topic,
            partition_key=self._normalise_key(message.key),
            payload=message.payload,
            headers=dict(message.headers or {}),
            schema_subject=message.schema_subject,
            schema_version=message.schema_version,
            scheduled_at=message.scheduled_at,
            attempts=0,
            max_attempts=message.max_attempts,
        )
        self._records[message_id] = record
        return message_id

    async def fetch_pending(self, limit: int = 100) -> Sequence[Dict[str, Any]]:
        rows: list[Dict[str, Any]] = []
        for message_id, record in self._records.items():
            if record.published_at is not None:
                continue
            if record.attempts >= record.max_attempts:
                continue
            rows.append(
                {
                    "id": message_id,
                    "topic": record.topic,
                    "partition_key": record.partition_key,
                    "payload": record.payload,
                    "headers": record.headers,
                    "schema_subject": record.schema_subject,
                    "schema_version": record.schema_version,
                }
            )
            if len(rows) >= limit:
                break
        return rows

    async def mark_published(self, message_id: str) -> None:
        record = self._records.get(message_id)
        if record is not None:
            record.published_at = datetime.now(timezone.utc)
            record.last_error = None

    async def mark_failed(self, message_id: str, error: str) -> None:
        record = self._records.get(message_id)
        if record is not None:
            record.attempts += 1
            record.last_error = error

    async def cleanup_old_events(self, older_than_days: int = 30) -> int:
        removed = 0
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        for message_id, record in list(self._records.items()):
            if record.published_at and record.published_at < cutoff:
                removed += 1
                del self._records[message_id]
        return removed

    @staticmethod
    def _normalise_key(key: str | bytes | None) -> str | None:
        if key is None:
            return None
        if isinstance(key, bytes):
            return key.decode("utf-8")
        return key


class InMemoryEventBus(EventBus):
    """Event bus that publishes directly into an in-memory KafkaProcessor."""

    def __init__(self, processor: KafkaProcessor) -> None:
        self._processor = processor

    async def publish(self, envelope: EventEnvelope) -> None:
        await self._processor.publish(
            topic=envelope.topic,
            value=envelope.payload,
            key=envelope.key,
            headers=dict(envelope.headers or {}),
        )

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        await asyncio.gather(*(self.publish(envelope) for envelope in envelopes))


__all__ = [
    "InMemoryEventBus",
    "InMemoryOutboxRepository",
]
