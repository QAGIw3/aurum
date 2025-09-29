from __future__ import annotations

import asyncio
from datetime import date, datetime, timezone

import pytest

from aurum.scenarios.event_pipeline import ScenarioEventPipeline, ScenarioEventPipelineConfig
from aurum.streaming.kafka_processor import KafkaProcessor, KafkaProcessorConfig
from tests.common.in_memory_streaming import InMemoryEventBus, InMemoryOutboxRepository


class _NullIdempotencyTracker:
    async def seen(self, event_id: str) -> bool:  # noqa: D401 - interface shim
        return False

    async def record(self, event_id: str) -> None:  # noqa: D401 - interface shim
        return None

    async def cleanup(self) -> None:  # noqa: D401 - interface shim
        return None


def _days_since_epoch(value: date) -> int:
    epoch = date(1970, 1, 1)
    return (value - epoch).days


@pytest.mark.integration
@pytest.mark.asyncio
async def test_event_pipeline_dispatches_scenario_event() -> None:
    config = ScenarioEventPipelineConfig(
        lifecycle_topic="aurum.scenario.output.v1",
        default_schema_subject="aurum.scenario.output.v1-value",
        in_memory=True,
    )
    processor_config = KafkaProcessorConfig(
        input_topics=(config.lifecycle_topic,),
        in_memory=True,
        group_id="scenario-event-pipeline-test",
    )
    processor = KafkaProcessor(processor_config)
    event_bus = InMemoryEventBus(processor)
    repository = InMemoryOutboxRepository()
    idempotency = _NullIdempotencyTracker()

    pipeline = ScenarioEventPipeline(
        config=config,
        repository=repository,
        event_bus=event_bus,
        processor=processor,
        processor_config=processor_config,
        idempotency_tracker=idempotency,
        schema_validator=None,
    )

    received: list[dict[str, object]] = []
    processed = asyncio.Event()

    async def _capture(message) -> None:
        received.append(message.value)
        processed.set()

    pipeline.register_lifecycle_handler(_capture)

    await pipeline.start(start_dispatcher=False)

    payload = {
        "scenario_id": "scenario-001",
        "tenant_id": "tenant-123",
        "run_id": None,
        "asof_date": _days_since_epoch(date(2024, 9, 28)),
        "curve_key": "power.ny.iso.dayahead",
        "tenor_type": "monthly",
        "contract_month": None,
        "tenor_label": "2024-10",
        "metric": "midpoint",
        "value": 42.5,
        "band_lower": None,
        "band_upper": None,
        "attribution": [
            {"component": "base", "delta": 12.34},
        ],
        "version_hash": "hash-001",
        "computed_ts": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
    }

    await pipeline.record_event(
        event_type="scenario.output.generated",
        aggregate_id="scenario-001",
        payload=payload,
    )

    await pipeline.flush()
    await asyncio.wait_for(processed.wait(), timeout=2)

    await pipeline.stop()

    assert received, "Expected lifecycle handler to capture an event"
    assert received[0]["scenario_id"] == "scenario-001"
    assert received[0]["metric"] == "midpoint"
