from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aurum.external.collect import Checkpoint
from aurum.external.collect.base import CollectorConfig, ExternalCollector
from aurum.external.providers.worldbank import (
    WorldBankCollector,
    WorldBankDatasetConfig,
)


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


class InMemoryCheckpointStore:
    def __init__(self) -> None:
        self._store: Dict[Tuple[str, str], Checkpoint] = {}

    def get(self, provider: str, series_id: str) -> Optional[Checkpoint]:
        return self._store.get((provider, series_id))

    def set(self, checkpoint: Checkpoint) -> None:
        self._store[(checkpoint.provider, checkpoint.series_id)] = checkpoint


class StubWorldBankApiClient:
    def __init__(self, pages: List[List[Dict[str, Any]]], metadata: Dict[str, Any]) -> None:
        self.pages = pages
        self.metadata = metadata
        self.iter_calls: List[Tuple[str, Tuple[str, ...]]] = []

    def get_indicator_metadata(self, indicator_id: str) -> Dict[str, Any]:
        return self.metadata

    def iter_indicator_observations(
        self,
        indicator_id: str,
        *,
        countries: Tuple[str, ...],
        start_year: Optional[int],
        end_year: Optional[int],
        per_page: int,
    ):
        self.iter_calls.append((indicator_id, countries))
        for page in self.pages:
            yield page


def make_collector(pages: List[List[Dict[str, Any]]]) -> Tuple[WorldBankCollector, DummyProducer, DummyProducer, InMemoryCheckpointStore]:
    dataset = WorldBankDatasetConfig(
        source_name="worldbank_test",
        indicator_id="TEST.IND",
        description="Test indicator",
        countries=("USA", "CAN"),
        default_unit="USD",
        frequency="ANNUAL",
        start_year=2000,
        per_page=100,
    )
    metadata = {
        "name": "Test Indicator",
        "unit": "USD",
        "sourceOrganization": "World Bank",
        "topics": [{"value": "Economy"}],
    }
    api_client = StubWorldBankApiClient(pages, metadata)
    catalog_producer = DummyProducer()
    observation_producer = DummyProducer()
    catalog_collector = ExternalCollector(
        CollectorConfig(
            provider="worldbank",
            base_url="https://api.worldbank.org/v2",
            kafka_topic="aurum.ext.series_catalog.upsert.v1",
        ),
        producer=catalog_producer,
    )
    observation_collector = ExternalCollector(
        CollectorConfig(
            provider="worldbank",
            base_url="https://api.worldbank.org/v2",
            kafka_topic="aurum.ext.timeseries.obs.v1",
        ),
        producer=observation_producer,
    )
    store = InMemoryCheckpointStore()
    collector = WorldBankCollector(
        dataset,
        api_client=api_client,
        catalog_collector=catalog_collector,
        observation_collector=observation_collector,
        checkpoint_store=store,
        now=lambda: datetime(2024, 1, 10, tzinfo=timezone.utc),
    )
    return collector, catalog_producer, observation_producer, store


def test_sync_catalog_emits_per_country() -> None:
    collector, catalog_producer, _, _ = make_collector([])
    emitted = collector.sync_catalog()
    assert emitted == 2
    series_ids = [call["value"]["series_id"] for call in catalog_producer.calls]
    assert series_ids == ["TEST.IND:USA", "TEST.IND:CAN"]
    metadata = catalog_producer.calls[0]["value"]["metadata"]
    assert metadata["topics"] == "Economy"


def test_ingest_observations_updates_checkpoints() -> None:
    pages = [
        [
            {
                "countryiso3code": "USA",
                "date": "2022",
                "value": "100",
                "indicator": {"id": "TEST.IND"},
                "country": {"value": "United States"},
            },
            {
                "countryiso3code": "CAN",
                "date": "2023",
                "value": None,
                "indicator": {"id": "TEST.IND"},
                "country": {"value": "Canada"},
            },
        ],
    ]
    collector, _, observation_producer, store = make_collector(pages)
    emitted = collector.ingest_observations()
    assert emitted == 2
    series_ids = [call["value"]["series_id"] for call in observation_producer.calls]
    assert "TEST.IND:USA" in series_ids
    usa_checkpoint = store.get("WorldBank", "TEST.IND:USA")
    assert usa_checkpoint is not None
    assert usa_checkpoint.last_timestamp == datetime(2022, 12, 31, tzinfo=timezone.utc)
    can_checkpoint = store.get("WorldBank", "TEST.IND:CAN")
    assert can_checkpoint is not None
    assert can_checkpoint.last_timestamp == datetime(2023, 12, 31, tzinfo=timezone.utc)

