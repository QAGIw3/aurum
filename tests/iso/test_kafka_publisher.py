from __future__ import annotations

from typing import Any, Dict, Mapping

import pytest

from aurum.iso import IsoDataType, IsoKafkaPublisher, ISO_SUBJECTS


class _StubProducer:
    def __init__(self) -> None:
        self.produce_calls: list[Dict[str, Any]] = []
        self.flush_calls = 0

    def produce(self, *, topic: str, value: Mapping[str, Any], value_schema: Mapping[str, Any]) -> None:
        self.produce_calls.append({
            "topic": topic,
            "value": dict(value),
            "schema": value_schema,
        })

    def flush(self) -> None:
        self.flush_calls += 1


def _publisher(iso_code: str) -> tuple[IsoKafkaPublisher, Dict[str, _StubProducer]]:
    holder: Dict[str, _StubProducer] = {}

    def factory(config: Dict[str, str]) -> Any:  # noqa: ANN401 - signature mirrors confluent factory
        assert "bootstrap.servers" in config
        assert "schema.registry.url" in config
        stub = _StubProducer()
        holder["producer"] = stub
        return stub

    pub = IsoKafkaPublisher(
        iso_code,
        bootstrap_servers="kafka:9092",
        schema_registry_url="http://schema-registry:8081",
        producer_factory=factory,
    )
    return pub, holder


@pytest.mark.parametrize("iso_code", sorted(ISO_SUBJECTS))
def test_supported_data_types_match_subject_map(iso_code: str) -> None:
    publisher, _ = _publisher(iso_code)
    assert set(publisher.supported_data_types()) == set(ISO_SUBJECTS[iso_code])


def test_emit_reuses_single_producer_across_data_types() -> None:
    publisher, holder = _publisher("iso.caiso")

    records_rt = [{"series_id": "CAISO-1", "value": 42.0}]
    count_rt = publisher.emit(IsoDataType.LMP, records_rt, cadence="realtime")

    records_daily = [{"series_id": "CAISO-1", "value": 420.0}]
    count_daily = publisher.emit(IsoDataType.LOAD, records_daily, cadence="daily")

    stub = holder["producer"]

    assert count_rt == 1
    assert count_daily == 1
    assert stub.flush_calls == 2
    # two produce calls routed to different topics but same underlying producer
    produced_topics = [call["topic"] for call in stub.produce_calls]
    assert produced_topics == [
        ISO_SUBJECTS["iso.caiso"][IsoDataType.LMP].topic,
        ISO_SUBJECTS["iso.caiso"][IsoDataType.LOAD].topic,
    ]


def test_emit_without_records_short_circuits() -> None:
    publisher, _ = _publisher("iso.miso")

    emitted = publisher.emit(IsoDataType.GENERATION_MIX, [])

    assert emitted == 0
    # Producer should not be initialised when there is nothing to emit
    assert getattr(publisher, "_producer") is None


def test_emit_raises_for_unsupported_data_type() -> None:
    publisher, _ = _publisher("iso.nyiso")
    with pytest.raises(ValueError, match="does not configure Kafka subjects"):
        publisher.emit(IsoDataType.ANCILLARY_SERVICES, [{"series_id": "NYISO", "value": 1.0}])
