"""Scenario event pipeline wiring outbox dispatch and Kafka consumers."""
from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, MutableMapping, Optional

from aurum.events.streaming import (
    EventEnvelope,
    EventBus,
    IdempotencyTracker,
    OutboxMessage,
    OutboxRepository,
    SchemaValidator,
    build_outbox_runtime,
)
from aurum.schema_registry import SubjectContracts
from aurum.streaming.kafka_processor import KafkaMessage, KafkaProcessor, KafkaProcessorConfig


@dataclass(slots=True)
class ScenarioEventPipelineConfig:
    """Configuration for the scenario event pilot."""

    lifecycle_topic: str = "aurum.scenario.output.v1"
    default_schema_subject: str = "aurum.scenario.output.v1-value"
    consumer_group: str = "aurum-scenario-event-pilot"
    batch_size: int = 100
    poll_interval: float = 1.0
    bootstrap_servers: str | None = None
    in_memory: bool = False
    dlq_topic: str | None = None

    def resolved_dlq(self) -> str:
        """Return the DLQ topic to use for this pipeline."""
        return self.dlq_topic or f"{self.lifecycle_topic}.dlq"


class ScenarioEventPipeline:
    """Outbox dispatcher + Kafka consumer pilot for scenario events."""

    def __init__(
        self,
        config: ScenarioEventPipelineConfig | None = None,
        *,
        repository: OutboxRepository | None = None,
        event_bus: EventBus | None = None,
        dlq_bus: EventBus | None = None,
        processor: KafkaProcessor | None = None,
        processor_config: KafkaProcessorConfig | None = None,
        idempotency_tracker: IdempotencyTracker | None = None,
        schema_validator: SchemaValidator | None = None,
    ) -> None:
        self.config = config or ScenarioEventPipelineConfig()
        if schema_validator is None and event_bus is None:
            schema_validator = self._build_schema_validator()

        runtime = build_outbox_runtime(
            topic=self.config.lifecycle_topic,
            consumer_group=self.config.consumer_group,
            bootstrap_servers=self.config.bootstrap_servers,
            batch_size=self.config.batch_size,
            poll_interval=self.config.poll_interval,
            in_memory=self.config.in_memory,
            repository=repository,
            event_bus=event_bus,
            dlq_bus=dlq_bus,
            processor=processor,
            processor_config=processor_config,
            idempotency_tracker=idempotency_tracker,
            schema_validator=schema_validator,
        )

        self._repository = runtime.repository
        self._event_bus = runtime.event_bus
        self._dlq_bus = runtime.dlq_bus
        self._processor_config = runtime.processor_config
        self._processor = runtime.processor
        self._idempotency = runtime.idempotency_tracker
        self.consumer = runtime.consumer
        self.dispatcher = runtime.dispatcher

    @staticmethod
    def _contracts_path() -> Path:
        return Path(__file__).resolve().parents[3] / "kafka" / "schemas" / "contracts.yml"

    def _build_schema_validator(self) -> SchemaValidator:
        contracts = SubjectContracts(self._contracts_path())
        return SchemaValidator(contracts=contracts, enforce=True)

    def register_lifecycle_handler(self, handler: Callable[[KafkaMessage], Any]) -> None:
        """Register a handler for scenario lifecycle events."""
        self.consumer.register_handler(self.config.lifecycle_topic, handler)

    async def start(
        self,
        *,
        start_dispatcher: bool = True,
        start_consumer: bool = True,
    ) -> None:
        if start_consumer:
            await self.consumer.start()
        if start_dispatcher:
            await self.dispatcher.start()

    async def stop(self) -> None:
        await self.dispatcher.stop()
        await self.consumer.stop()

    async def record_event(
        self,
        *,
        event_type: str,
        aggregate_id: str,
        payload: Mapping[str, Any],
        schema_subject: str | None = None,
        headers: Mapping[str, str] | None = None,
        max_attempts: int = 5,
    ) -> str:
        """Stage a scenario lifecycle event in the outbox."""
        event_id = str(uuid.uuid4())
        prepared_headers: MutableMapping[str, str] = {
            "event_id": event_id,
            "event_type": event_type,
            "aggregate_id": aggregate_id,
        }
        prepared_headers.update(headers or {})

        message = OutboxMessage(
            topic=self.config.lifecycle_topic,
            payload=dict(payload),
            key=aggregate_id,
            headers=prepared_headers,
            schema_subject=schema_subject or self.config.default_schema_subject,
            schema_version=1,
            max_attempts=max_attempts,
        )
        await self._repository.enqueue(message)
        return event_id

    async def flush(self) -> int:
        """Manually drain one batch from the outbox."""
        return await self.dispatcher.drain_once()

    @property
    def repository(self) -> OutboxRepository:
        return self._repository

    @property
    def processor(self) -> KafkaProcessor:
        return self._processor

    @property
    def event_bus(self) -> EventBus:
        return self._event_bus


__all__ = [
    "ScenarioEventPipeline",
    "ScenarioEventPipelineConfig",
]
