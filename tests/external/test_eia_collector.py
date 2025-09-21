from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


from aurum.external.collect import Checkpoint
from aurum.external.collect.base import CollectorConfig, ExternalCollector
from aurum.external.providers.eia import EiaApiClient, EiaCollector, EiaDatasetConfig


class DummyProducer:
    def __init__(self) -> None:
        self.calls: List[Dict[str, Any]] = []
        self.flush_calls = 0

    def produce(
        self,
        *,
        topic: str,
        value: Any,
        key: Optional[Any] = None,
        headers: Optional[Iterable[Tuple[str, bytes]]] = None,
    ) -> None:
        self.calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": list(headers or []),
            }
        )

    def flush(self) -> None:
        self.flush_calls += 1


def http_response(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "status_code": 200,
        "headers": {"Content-Type": "application/json"},
        "content": json.dumps(payload).encode("utf-8"),
        "url": "https://api.test/eia",
    }


class StubHttpCollector(ExternalCollector):
    def __init__(self, responses: Iterable[Dict[str, Any]]) -> None:
        config = CollectorConfig(
            provider="eia",
            base_url="https://api.test/",
            kafka_topic="noop",
        )
        super().__init__(config, producer=DummyProducer())
        self._responses = list(responses)
        self._index = 0

    def request(self, request):  # type: ignore[override]
        if self._index >= len(self._responses):
            raise AssertionError("No more stub responses configured")
        payload = self._responses[self._index]
        self._index += 1
        return type(self). _to_http_response(payload)  # reuse helper from base class

    @staticmethod
    def _to_http_response(payload: Dict[str, Any]):
        from aurum.external.collect.base import HttpResponse

        return HttpResponse(
            status_code=payload["status_code"],
            headers=payload["headers"],
            content=payload["content"],
            url=payload["url"],
        )


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str], Checkpoint] = {}

    def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:
        return self._store.get((provider, series_id))

    def set(self, checkpoint: Checkpoint) -> None:
        self._store[(checkpoint.provider, checkpoint.series_id)] = checkpoint


def make_collector(
    *,
    responses: Iterable[Dict[str, Any]],
    catalog_producer: DummyProducer,
    observation_producer: DummyProducer,
    checkpoint_store,
) -> Tuple[EiaCollector, StubHttpCollector]:
    http_collector = StubHttpCollector(responses)
    api_client = EiaApiClient(http_collector, api_key="test-key")

    catalog_collector = ExternalCollector(
        CollectorConfig(
            provider="eia",
            base_url="https://api.test/",
            kafka_topic="aurum.ext.series_catalog.upsert.v1",
        ),
        producer=catalog_producer,
    )
    observation_collector = ExternalCollector(
        CollectorConfig(
            provider="eia",
            base_url="https://api.test/",
            kafka_topic="aurum.ext.timeseries.obs.v1",
        ),
        producer=observation_producer,
    )

    dataset_config = EiaDatasetConfig(
        source_name="eia_sample",
        data_path="sample/data",
        catalog_path="sample/series",
        series_id_field="series_id",
        period_field="period",
        value_field="value",
        dataset_code="sample",
        frequency_code="DAILY",
        default_frequency="DAILY",
        default_unit="MW",
    )

    collector = EiaCollector(
        dataset_config,
        api_client=api_client,
        catalog_collector=catalog_collector,
        observation_collector=observation_collector,
        checkpoint_store=checkpoint_store,
        now=lambda: datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return collector, http_collector


def test_sync_catalog_emits_records() -> None:
    responses = [
        http_response(
            {
                "response": {
                    "data": [
                        {"series_id": "S1", "name": "Series One", "unit": "BBL"},
                        {"series_id": "S2", "name": "Series Two"},
                    ],
                    "next": {"offset": 100},
                }
            }
        ),
        http_response(
            {
                "response": {
                    "data": [
                        {"series_id": "S3", "description": "Third"},
                    ],
                }
            }
        ),
    ]

    catalog_producer = DummyProducer()
    observation_producer = DummyProducer()
    store = InMemoryCheckpointStore()
    collector, _ = make_collector(
        responses=responses,
        catalog_producer=catalog_producer,
        observation_producer=observation_producer,
        checkpoint_store=store,
    )

    emitted = collector.sync_catalog()

    assert emitted == 3
    assert catalog_producer.flush_calls == 1
    titles = [call["value"]["title"] for call in catalog_producer.calls]
    assert titles == ["Series One", "Series Two", "S3"]


def test_ingest_observations_respects_checkpoint() -> None:
    first_page = http_response(
        {
            "response": {
                "data": [
                    {"series_id": "S1", "period": "2023-12-31", "value": "9"},
                    {"series_id": "S1", "period": "2024-01-01", "value": "10"},
                ],
            }
        }
    )
    responses = [first_page]

    catalog_producer = DummyProducer()
    observation_producer = DummyProducer()
    store = InMemoryCheckpointStore()
    # Existing checkpoint for S1 should skip the 2023-12-31 record
    store.set(
        Checkpoint(
            provider="EIA",
            series_id="S1",
            last_timestamp=datetime(2023, 12, 31, tzinfo=timezone.utc),
        )
    )

    collector, http_collector = make_collector(
        responses=responses,
        catalog_producer=catalog_producer,
        observation_producer=observation_producer,
        checkpoint_store=store,
    )

    emitted = collector.ingest_observations()

    assert emitted == 1
    assert len(observation_producer.calls) == 1
    record = observation_producer.calls[0]["value"]
    assert record["series_id"] == "S1"
    assert record["value"] == 10.0
    # Checkpoint advanced to 2024-01-01
    updated = store.get("EIA", "S1")
    assert updated is not None
    assert updated.last_timestamp == datetime(2024, 1, 1, tzinfo=timezone.utc)
