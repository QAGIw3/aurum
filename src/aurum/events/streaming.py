"""Kafka streaming primitives, schema validation, and outbox dispatch."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

try:  # pragma: no cover - optional runtime dependency
    from fastavro import parse_schema as _parse_avro_schema
    from fastavro.validation import validate as _validate_avro_payload
except Exception:  # pragma: no cover - fastavro may be absent in minimal installs
    _parse_avro_schema = None  # type: ignore[assignment]
    _validate_avro_payload = None  # type: ignore[assignment]

from aurum.api.database.timescale_client import AsyncTimescaleClient, get_timescale_client
from aurum.kafka.optimized_producer import OptimizedKafkaProducer
from aurum.observability import tracing
from aurum.schema_registry import SchemaRegistryManager, SchemaRegistryConfig, SubjectContracts
from aurum.streaming.kafka_processor import KafkaMessage, KafkaProcessor, KafkaProcessorConfig

LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event envelope and validation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EventEnvelope:
    """Canonical message wrapper used for Kafka IO."""

    topic: str
    payload: Any
    key: str | bytes | None = None
    headers: Mapping[str, str] | None = None
    schema_subject: str | None = None
    schema_version: int | None = None

    def with_header(self, key: str, value: str) -> "EventEnvelope":
        headers = dict(self.headers or {})
        headers[key] = value
        return EventEnvelope(
            topic=self.topic,
            payload=self.payload,
            key=self.key,
            headers=headers,
            schema_subject=self.schema_subject,
            schema_version=self.schema_version,
        )


class SchemaValidationError(RuntimeError):
    """Raised when schema validation fails prior to publish."""


class SchemaValidator:
    """Lightweight schema validation that consults contract catalog."""

    def __init__(
        self,
        contracts: SubjectContracts | None = None,
        *,
        enforce: bool = True,
    ) -> None:
        self._contracts = contracts
        self._enforce = enforce
        self._parsed_cache: Dict[str, Any] = {}

    @classmethod
    def from_registry_manager(
        cls,
        manager: SchemaRegistryManager | None,
    ) -> "SchemaValidator":
        contracts = None
        if manager and getattr(manager, "_contracts", None):
            contracts = manager._contracts  # type: ignore[attr-defined]
        return cls(contracts=contracts, enforce=True)

    def validate(self, envelope: EventEnvelope) -> None:
        if not self._enforce:
            return
        subject = envelope.schema_subject
        if not subject:
            raise SchemaValidationError("schema_subject is required when validation is enforced")
        if self._contracts is None:
            LOGGER.debug("Schema contracts not loaded; skipping validation for %s", subject)
            return

        try:
            self._contracts.get(subject)
        except Exception as exc:
            raise SchemaValidationError(
                f"Schema subject {subject} not registered in contracts catalog"
            ) from exc

        if _parse_avro_schema is None or _validate_avro_payload is None:
            raise SchemaValidationError(
                "fastavro is required for schema validation but is not available"
            )

        parsed_schema = self._parsed_cache.get(subject)
        if parsed_schema is None:
            schema_payload = self._contracts.load_schema(subject)
            parsed_schema = _parse_avro_schema(schema_payload)
            self._parsed_cache[subject] = parsed_schema

        payload = envelope.payload
        if payload is None:
            raise SchemaValidationError(
                f"Payload for subject {subject} must be provided for Avro validation"
            )

        try:
            is_valid = _validate_avro_payload(payload, parsed_schema)
        except Exception as exc:  # pragma: no cover - fastavro supplies detailed errors
            raise SchemaValidationError(
                f"Schema validation failed for subject {subject}: {exc}"
            ) from exc

        if not is_valid:
            raise SchemaValidationError(
                f"Schema validation returned False for subject {subject}"
            )


# ---------------------------------------------------------------------------
# Event bus abstraction
# ---------------------------------------------------------------------------


class EventBus:
    """Abstract event bus contract."""

    async def publish(self, envelope: EventEnvelope) -> None:
        raise NotImplementedError

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        for envelope in envelopes:
            await self.publish(envelope)


class KafkaEventBus(EventBus):
    """Kafka-backed event bus using the optimized producer wrapper."""

    def __init__(
        self,
        producer: OptimizedKafkaProducer | None = None,
        schema_validator: SchemaValidator | None = None,
    ) -> None:
        self._producer = producer or OptimizedKafkaProducer()
        self._validator = schema_validator or SchemaValidator(enforce=False)

    def set_validator(self, validator: SchemaValidator) -> None:
        """Update the schema validator used for publish operations."""
        self._validator = validator

    async def publish(self, envelope: EventEnvelope) -> None:
        self._validator.validate(envelope)
        message_headers = self._prepare_headers(envelope.headers)
        await self._produce(
            topic=envelope.topic,
            value=envelope.payload,
            key=envelope.key,
            headers=message_headers,
            schema_subject=envelope.schema_subject,
            schema_version=envelope.schema_version,
        )

    async def publish_batch(self, envelopes: Sequence[EventEnvelope]) -> None:
        if not envelopes:
            return
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._produce_batch_sync,
            envelopes,
        )

    async def _produce(
        self,
        *,
        topic: str,
        value: Any,
        key: str | bytes | None,
        headers: Mapping[str, bytes | None],
        schema_subject: str | None,
        schema_version: int | None,
    ) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            self._producer.produce,
            topic,
            value,
            key,
            None,
            headers,
            schema_subject,
            schema_version,
        )

    def _produce_batch_sync(self, envelopes: Sequence[EventEnvelope]) -> None:
        for index, envelope in enumerate(envelopes):
            self._validator.validate(envelope)
            headers = self._prepare_headers(envelope.headers)
            self._producer.produce(
                topic=envelope.topic,
                value=envelope.payload,
                key=envelope.key,
                headers=headers,
                schema_subject=envelope.schema_subject,
                schema_version=envelope.schema_version,
            )
            if (index + 1) % 100 == 0:
                self._producer.poll(0)
        self._producer.flush(5.0)

    def _prepare_headers(
        self,
        headers: Mapping[str, str] | None,
    ) -> Mapping[str, bytes | None]:
        enriched: Dict[str, bytes | None] = {}
        trace_id = tracing.get_current_trace_id()
        span_id = tracing.get_current_span_id()
        merged = dict(headers or {})
        merged.setdefault("trace_id", trace_id or "")
        merged.setdefault("span_id", span_id or "")
        for key, value in merged.items():
            enriched[key] = value.encode("utf-8") if value is not None else None
        return enriched


# ---------------------------------------------------------------------------
# Kafka consumer helper (CQRS projections)
# ---------------------------------------------------------------------------


EventHandler = Callable[[KafkaMessage], Awaitable[None]]


class KafkaEventConsumer:
    """Wrapper around KafkaProcessor with idempotency and DLQ support."""

    def __init__(
        self,
        config: KafkaProcessorConfig,
        *,
        processor: KafkaProcessor | None = None,
        idempotency_tracker: "IdempotencyTracker" | None = None,
        dlq_bus: EventBus | None = None,
    ) -> None:
        self._processor = processor or KafkaProcessor(config)
        self._tracker = idempotency_tracker
        self._dlq_bus = dlq_bus

    def register_handler(self, topic: str, handler: EventHandler) -> None:
        async def wrapped(message: KafkaMessage) -> None:
            event_id = self._extract_event_id(message)
            if self._tracker and event_id:
                already_processed = await self._tracker.seen(event_id)
                if already_processed:
                    LOGGER.debug("Skipping duplicate event %s", event_id)
                    return
            try:
                await handler(message)
                if self._tracker and event_id:
                    await self._tracker.record(event_id)
            except Exception as exc:  # pragma: no cover - defensive path
                LOGGER.exception("Kafka handler failed for topic %s", message.topic)
                if self._dlq_bus:
                    await self._dlq_bus.publish(
                        EventEnvelope(
                            topic=f"{message.topic}.dlq",
                            key=(message.key or "").decode("utf-8") if isinstance(message.key, bytes) else message.key,
                            payload={
                                "error": str(exc),
                                "original_topic": message.topic,
                                "headers": dict(message.headers or {}),
                                "payload": message.value,
                                "trace_id": tracing.get_current_trace_id(),
                            },
                        )
                    )
                raise

        self._processor.register_handler(topic, wrapped)

    async def start(self) -> None:
        await self._processor.start()

    async def stop(self) -> None:
        await self._processor.stop()

    @staticmethod
    def _extract_event_id(message: KafkaMessage) -> str | None:
        payload = message.value
        if isinstance(payload, Mapping):
            event_id = payload.get("event_id")
            if isinstance(event_id, str):
                return event_id
        headers = message.headers or {}
        if isinstance(headers, Mapping):
            raw = headers.get("event_id")
            if isinstance(raw, bytes):
                return raw.decode("utf-8")
            if isinstance(raw, str):
                return raw
        return None


# ---------------------------------------------------------------------------
# Idempotency tracking
# ---------------------------------------------------------------------------


class IdempotencyTracker:
    """Track processed event IDs to guarantee at-least-once semantics."""

    def __init__(
        self,
        client: AsyncTimescaleClient | None = None,
        *,
        retention_days: int = 7,
    ) -> None:
        self._client = client or get_timescale_client()
        self._retention_days = retention_days
        self._schema_lock = asyncio.Lock()
        self._schema_ready = False

    async def seen(self, event_id: str) -> bool:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT 1 FROM event_store_idempotency
            WHERE event_id = %(event_id)s
            """,
            {"event_id": event_id},
        )
        return bool(rows)

    async def record(self, event_id: str) -> None:
        await self._ensure_schema()
        await self._client.execute(
            """
            INSERT INTO event_store_idempotency (event_id, recorded_at)
            VALUES (%(event_id)s, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """,
            {"event_id": event_id},
        )

    async def cleanup(self) -> None:
        await self._ensure_schema()
        await self._client.execute(
            """
            DELETE FROM event_store_idempotency
            WHERE recorded_at < NOW() - INTERVAL '%(retention)s days'
            """,
            {"retention": self._retention_days},
        )

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            await self._client.execute(
                """
                CREATE TABLE IF NOT EXISTS event_store_idempotency (
                    event_id TEXT PRIMARY KEY,
                    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            self._schema_ready = True


# ---------------------------------------------------------------------------
# Outbox support
# ---------------------------------------------------------------------------


@dataclass
class OutboxMessage:
    """Message staged for reliable publish via the outbox."""

    topic: str
    payload: Any
    key: str | bytes | None = None
    headers: Mapping[str, str] | None = None
    schema_subject: str | None = None
    schema_version: int | None = None
    scheduled_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    max_attempts: int = 5


class OutboxRepository:
    """Persistence abstraction for the outbox store."""

    async def enqueue(self, message: OutboxMessage) -> str:
        raise NotImplementedError

    async def fetch_pending(self, limit: int = 100) -> Sequence[Dict[str, Any]]:
        raise NotImplementedError

    async def mark_published(self, message_id: str) -> None:
        raise NotImplementedError

    async def mark_failed(self, message_id: str, error: str) -> None:
        raise NotImplementedError


class TimescaleOutboxRepository(OutboxRepository):
    """Timescale-backed outbox implementation with retry support."""

    _DDL = """
        CREATE TABLE IF NOT EXISTS event_store_outbox (
            id UUID PRIMARY KEY,
            topic TEXT NOT NULL,
            partition_key TEXT,
            payload JSONB NOT NULL,
            headers JSONB NOT NULL DEFAULT '{}'::jsonb,
            schema_subject TEXT,
            schema_version INTEGER,
            scheduled_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            published_at TIMESTAMPTZ,
            attempts INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 5,
            last_error TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_event_store_outbox_pending
            ON event_store_outbox (scheduled_at)
            WHERE published_at IS NULL;
    """

    def __init__(self, client: AsyncTimescaleClient | None = None) -> None:
        self._client = client or get_timescale_client()
        self._schema_lock = asyncio.Lock()
        self._schema_ready = False

    async def enqueue(self, message: OutboxMessage) -> str:
        await self._ensure_schema()
        message_id = str(uuid.uuid4())
        await self._client.execute(
            """
            INSERT INTO event_store_outbox (
                id, topic, partition_key, payload, headers,
                schema_subject, schema_version, scheduled_at, max_attempts
            ) VALUES (
                %(id)s, %(topic)s, %(partition_key)s, %(payload)s::jsonb,
                %(headers)s::jsonb, %(schema_subject)s, %(schema_version)s,
                %(scheduled_at)s, %(max_attempts)s
            )
            """,
            {
                "id": message_id,
                "topic": message.topic,
                "partition_key": self._normalise_key(message.key),
                "payload": self._json_dumps(message.payload),
                "headers": self._json_dumps(dict(message.headers or {})),
                "schema_subject": message.schema_subject,
                "schema_version": message.schema_version,
                "scheduled_at": message.scheduled_at,
                "max_attempts": message.max_attempts,
            },
        )
        return message_id

    async def fetch_pending(self, limit: int = 100) -> Sequence[Dict[str, Any]]:
        await self._ensure_schema()
        rows = await self._client.execute_query(
            """
            SELECT id, topic, partition_key, payload, headers, schema_subject,
                   schema_version, attempts, max_attempts
            FROM event_store_outbox
            WHERE published_at IS NULL
              AND scheduled_at <= NOW()
              AND attempts < max_attempts
            ORDER BY scheduled_at ASC
            LIMIT %(limit)s
            """,
            {"limit": limit},
        )
        return rows

    async def mark_published(self, message_id: str) -> None:
        await self._ensure_schema()
        await self._client.execute(
            """
            UPDATE event_store_outbox
            SET published_at = NOW(), last_error = NULL
            WHERE id = %(id)s
            """,
            {"id": message_id},
        )

    async def mark_failed(self, message_id: str, error: str) -> None:
        await self._ensure_schema()
        await self._client.execute(
            """
            UPDATE event_store_outbox
            SET attempts = attempts + 1,
                last_error = %(error)s,
                scheduled_at = NOW() + INTERVAL '%(delay)s seconds'
            WHERE id = %(id)s
            """,
            {
                "id": message_id,
                "error": error,
                "delay": self._calculate_backoff_seconds(message_id),
            },
        )

    async def _ensure_schema(self) -> None:
        if self._schema_ready:
            return
        async with self._schema_lock:
            if self._schema_ready:
                return
            for statement in filter(None, self._DDL.split(";")):
                stmt = statement.strip()
                if stmt:
                    await self._client.execute(stmt)
            self._schema_ready = True

    @staticmethod
    def _json_dumps(payload: Any) -> str:
        def _default(obj: Any) -> str:
            if isinstance(obj, datetime):
                return obj.isoformat()
            return str(obj)

        return json.dumps(payload, default=_default)

    @staticmethod
    def _normalise_key(key: str | bytes | None) -> Optional[str]:
        if key is None:
            return None
        if isinstance(key, bytes):
            return key.decode("utf-8")
        return key

    @staticmethod
    def _calculate_backoff_seconds(message_id: str) -> int:
        # Deterministic backoff between 5s and 300s using hash of id
        base = 5
        spread = 295
        hash_value = int(uuid.UUID(message_id))
        return base + (hash_value % spread)


class OutboxDispatcher:
    """Background worker that drains the outbox to Kafka."""

    def __init__(
        self,
        repository: OutboxRepository,
        event_bus: EventBus,
        *,
        batch_size: int = 100,
        poll_interval: float = 1.0,
    ) -> None:
        self._repository = repository
        self._event_bus = event_bus
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._task: asyncio.Task[None] | None = None
        self._running = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            return
        self._running.set()
        self._task = asyncio.create_task(self._run(), name="outbox-dispatcher")

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

    async def _run(self) -> None:
        while self._running.is_set():
            try:
                processed = await self.drain_once()
                if processed == 0:
                    await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive path
                LOGGER.exception("Outbox dispatcher loop failure: %s", exc)
                await asyncio.sleep(min(self._poll_interval * 5, 30))

    async def drain_once(self) -> int:
        """Process a single batch of outbox records."""
        rows = await self._repository.fetch_pending(self._batch_size)
        processed = 0
        for row in rows:
            message_id = str(row["id"])
            try:
                envelope = EventEnvelope(
                    topic=row["topic"],
                    key=row.get("partition_key"),
                    payload=self._decode_json(row.get("payload")),
                    headers=self._decode_json(row.get("headers")),
                    schema_subject=row.get("schema_subject"),
                    schema_version=row.get("schema_version"),
                )
                await self._event_bus.publish(envelope)
                await self._repository.mark_published(message_id)
                processed += 1
            except Exception as exc:  # pragma: no cover - defensive path
                LOGGER.exception("Failed to dispatch outbox event %s", message_id)
                await self._repository.mark_failed(message_id, str(exc))
        return processed

    @staticmethod
    def _decode_json(raw: Any) -> Any:
        if raw is None:
            return None
        if isinstance(raw, (dict, list)):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except json.JSONDecodeError:
                return raw
        return raw


@dataclass(slots=True)
class OutboxPipelineRuntime:
    """Aggregated runtime components for an outbox-driven pipeline."""

    repository: OutboxRepository
    event_bus: EventBus
    dlq_bus: EventBus
    processor_config: KafkaProcessorConfig
    processor: KafkaProcessor
    idempotency_tracker: IdempotencyTracker
    consumer: KafkaEventConsumer
    dispatcher: OutboxDispatcher


def build_outbox_runtime(
    *,
    topic: str,
    consumer_group: str,
    bootstrap_servers: str | None = None,
    batch_size: int = 100,
    poll_interval: float = 1.0,
    in_memory: bool = False,
    repository: OutboxRepository | None = None,
    event_bus: EventBus | None = None,
    dlq_bus: EventBus | None = None,
    processor: KafkaProcessor | None = None,
    processor_config: KafkaProcessorConfig | None = None,
    idempotency_tracker: IdempotencyTracker | None = None,
    schema_validator: SchemaValidator | None = None,
) -> OutboxPipelineRuntime:
    """Build a consistent set of streaming components for an outbox pipeline."""

    resolved_repository = repository or TimescaleOutboxRepository()

    if event_bus is None:
        resolved_event_bus = KafkaEventBus(schema_validator=schema_validator)
    else:
        resolved_event_bus = event_bus
        if schema_validator is not None and isinstance(resolved_event_bus, KafkaEventBus):
            resolved_event_bus.set_validator(schema_validator)

    resolved_dlq_bus = dlq_bus or resolved_event_bus

    resolved_config = processor_config or KafkaProcessorConfig(
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group,
        input_topics=(topic,),
        in_memory=in_memory,
        poll_interval=poll_interval,
    )

    resolved_processor = processor or KafkaProcessor(resolved_config)
    resolved_idempotency = idempotency_tracker or IdempotencyTracker()

    consumer = KafkaEventConsumer(
        resolved_config,
        processor=resolved_processor,
        idempotency_tracker=resolved_idempotency,
        dlq_bus=resolved_dlq_bus,
    )

    dispatcher = OutboxDispatcher(
        resolved_repository,
        resolved_event_bus,
        batch_size=batch_size,
        poll_interval=poll_interval,
    )

    return OutboxPipelineRuntime(
        repository=resolved_repository,
        event_bus=resolved_event_bus,
        dlq_bus=resolved_dlq_bus,
        processor_config=resolved_config,
        processor=resolved_processor,
        idempotency_tracker=resolved_idempotency,
        consumer=consumer,
        dispatcher=dispatcher,
    )


__all__ = [
    "EventEnvelope",
    "EventBus",
    "KafkaEventBus",
    "KafkaEventConsumer",
    "IdempotencyTracker",
    "OutboxDispatcher",
    "OutboxMessage",
    "OutboxRepository",
    "SchemaValidationError",
    "SchemaValidator",
    "TimescaleOutboxRepository",
    "OutboxPipelineRuntime",
    "build_outbox_runtime",
]
