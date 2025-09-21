from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from aurum.external.collect import Checkpoint
from aurum.external.collect.base import CollectorConfig, ExternalCollector
from aurum.external.providers.fred import FredCollector, FredDatasetConfig


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


class FakeFredApiClient:
    def __init__(self, *, series_payload: Optional[Dict[str, Any]] = None, pages: Optional[List[List[Dict[str, Any]]]] = None) -> None:
        self.series_payload = series_payload
        self.pages = pages or []

    def get_series(self, series_id: str) -> Optional[Dict[str, Any]]:  # pragma: no cover - simple passthrough
        return self.series_payload

    def iter_observations(
        self,
        series_id: str,
        *,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        realtime_start: Optional[str] = None,
        realtime_end: Optional[str] = None,
        page_size: int,
    ):
        for page in self.pages:
            yield page


def make_collectors(api_client: FakeFredApiClient) -> Tuple[FredCollector, DummyProducer, DummyProducer, InMemoryCheckpointStore]:
    catalog_producer = DummyProducer()
    observation_producer = DummyProducer()
    dataset = FredDatasetConfig(
        source_name="fred_test",
        series_id="TEST",
        description="Test series",
        frequency="DAILY",
        default_unit="Percent",
        seasonal_adjustment="Not Seasonally Adjusted",
        window_hours=24,
        window_days=None,
        window_months=None,
        window_years=None,
    )

    catalog_collector = ExternalCollector(
        CollectorConfig(
            provider="fred",
            base_url="https://api.stlouisfed.org/",
            kafka_topic="aurum.ext.series_catalog.upsert.v1",
        ),
        producer=catalog_producer,
    )
    observation_collector = ExternalCollector(
        CollectorConfig(
            provider="fred",
            base_url="https://api.stlouisfed.org/",
            kafka_topic="aurum.ext.timeseries.obs.v1",
        ),
        producer=observation_producer,
    )
    store = InMemoryCheckpointStore()
    collector = FredCollector(
        dataset,
        api_client=api_client,
        catalog_collector=catalog_collector,
        observation_collector=observation_collector,
        checkpoint_store=store,
        now=lambda: datetime(2024, 1, 10, tzinfo=timezone.utc),
    )
    return collector, catalog_producer, observation_producer, store


def test_sync_catalog_emits_single_record() -> None:
    api_client = FakeFredApiClient(
        series_payload={
            "id": "TEST",
            "title": "Test Title",
            "notes": "Detailed description",
            "units": "Percent",
            "observation_start": "2000-01-01",
            "observation_end": "2024-01-01",
            "last_updated": "2024-01-05T12:30:00Z",
            "popularity": 99,
        }
    )
    collector, catalog_producer, _, _ = make_collectors(api_client)

    emitted = collector.sync_catalog()

    assert emitted == 1
    assert len(catalog_producer.calls) == 1
    record = catalog_producer.calls[0]["value"]
    assert record["provider"] == "FRED"
    assert record["series_id"] == "TEST"
    assert record["unit_code"] == "Percent"
    assert record["metadata"]["seasonal_adjustment"] == "Not Seasonally Adjusted"
    assert record["start_ts"] == int(datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)


def test_ingest_observations_maps_values_and_updates_checkpoint() -> None:
    pages = [
        [
            {
                "date": "2024-01-01",
                "value": ".",
                "realtime_start": "2024-01-02",
                "realtime_end": "2024-01-15",
                "status": "A",
            },
            {
                "date": "2024-01-02",
                "value": "2.5",
                "realtime_end": "2024-01-16",
            },
        ]
    ]
    api_client = FakeFredApiClient(pages=pages)
    collector, _, observation_producer, store = make_collectors(api_client)
    store.set(
        Checkpoint(
            provider="FRED",
            series_id="TEST",
            last_timestamp=datetime(2023, 12, 31, tzinfo=timezone.utc),
        )
    )

    emitted = collector.ingest_observations()

    assert emitted == 2
    assert len(observation_producer.calls) == 2
    first = observation_producer.calls[0]["value"]
    assert first["value"] is None
    assert first["value_raw"] is None
    asof_expected = (date(2024, 1, 15) - date(1970, 1, 1)).days
    assert first["asof_date"] == asof_expected
    assert first["source_event_id"] == "TEST:2024-01-01:2024-01-15"

    second = observation_producer.calls[1]["value"]
    assert second["value"] == 2.5
    updated_checkpoint = store.get("FRED", "TEST")
    assert updated_checkpoint is not None
    assert updated_checkpoint.last_timestamp == datetime(2024, 1, 2, tzinfo=timezone.utc)

