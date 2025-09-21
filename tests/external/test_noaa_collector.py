from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pytest

from aurum.external.collect import Checkpoint
from aurum.external.collect.base import CollectorConfig, ExternalCollector
from aurum.external.providers.noaa import (
    DailyQuota,
    NoaaCollector,
    NoaaDatasetConfig,
    NoaaRateLimiter,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleep_calls: List[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.now += seconds


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


class StubNoaaApiClient:
    def __init__(self, pages: List[List[Dict[str, Any]]]) -> None:
        self.pages = pages
        self.requests: List[str] = []

    def iterate(self, path: str, params: Dict[str, Any], *, page_limit: int):
        self.requests.append(path)
        if path == "/data":
            for page in self.pages:
                yield page
        else:
            yield from []


def make_collectors(pages: List[List[Dict[str, Any]]]) -> Tuple[NoaaCollector, DummyProducer, DummyProducer, InMemoryCheckpointStore]:
    dataset = NoaaDatasetConfig(
        dataset_id="daily",
        dataset="GHCND",
        stations=["GHCND:123"],
        datatypes=["TMAX"],
        window_days=2,
    )
    api_client = StubNoaaApiClient(pages)
    catalog_producer = DummyProducer()
    observation_producer = DummyProducer()
    catalog_collector = ExternalCollector(
        CollectorConfig(
            provider="noaa",
            base_url="https://example.com",
            kafka_topic="aurum.ext.series_catalog.upsert.v1",
        ),
        producer=catalog_producer,
    )
    observation_collector = ExternalCollector(
        CollectorConfig(
            provider="noaa",
            base_url="https://example.com",
            kafka_topic="aurum.ext.timeseries.obs.v1",
        ),
        producer=observation_producer,
    )
    store = InMemoryCheckpointStore()
    collector = NoaaCollector(
        dataset,
        api_client=api_client,
        catalog_collector=catalog_collector,
        observation_collector=observation_collector,
        checkpoint_store=store,
        now=lambda: datetime(2024, 1, 10, tzinfo=timezone.utc),
    )
    return collector, catalog_producer, observation_producer, store


def test_rate_limiter_enforces_interval() -> None:
    clock = FakeClock()
    limiter = NoaaRateLimiter(rate_per_sec=5.0, monotonic=clock.monotonic, sleep=clock.sleep)

    limiter.acquire()
    limiter.acquire()

    assert clock.sleep_calls == [pytest.approx(0.2, rel=1e-6)]


def test_daily_quota_blocks_after_limit() -> None:
    quota = DailyQuota(limit=2, today_fn=lambda: date(2024, 1, 1))
    quota.consume()
    quota.consume()
    with pytest.raises(RuntimeError):
        quota.consume()


def test_noaa_collector_ingests_and_updates_checkpoint() -> None:
    pages = [
        [
            {"date": "2024-01-09T00:00:00", "value": 10},
            {"date": "2024-01-10T00:00:00", "value": 12},
        ]
    ]
    collector, catalog_producer, observation_producer, store = make_collectors(pages)
    emitted = collector.ingest_observations()

    assert emitted == 2
    assert len(observation_producer.calls) == 2
    last = store.get("NOAA", "GHCND:GHCND:123:TMAX")
    assert last is not None
    assert last.last_timestamp == datetime(2024, 1, 10, tzinfo=timezone.utc)

