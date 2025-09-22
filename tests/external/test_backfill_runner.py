import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import pytest

from aurum.external.backfill import (
    BackfillManager,
    BackfillResult,
    BackfillRunner,
    ProviderBackfillConfig,
    load_backfill_settings,
)
from aurum.external.providers.fred import FredDatasetConfig


def test_backfill_runner_respects_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_payload = {
        "providers": [
            {
                "name": "fred",
                "datasets": ["DGS10", "UNRATE"],
                "start_date": "2020-01-01",
                "end_date": "2020-02-01",
                "priority": "high",
                "max_workers": 1,
                "batch_size": 100,
                "rate_limit_rps": 2.0,
            },
            {
                "name": "worldbank",
                "datasets": ["NY.GDP.MKTP.CD"],
                "start_date": "2020-01-01",
                "end_date": "2020-02-01",
                "priority": "low",
                "max_workers": 1,
                "batch_size": 100,
                "rate_limit_rps": 1.0,
            },
        ],
        "scheduling": {
            "max_concurrent_backfills": 1,
            "priority_order": ["high", "low"],
            "retry_failed_datasets": True,
            "resume_from_checkpoint": True,
        },
        "quality": {},
        "performance": {},
    }
    config_path = tmp_path / "backfill.json"
    config_path.write_text(json.dumps(config_payload), encoding="utf-8")

    settings = load_backfill_settings(config_path)

    execution_order: List[Tuple[str, str]] = []

    class DummyManager:
        def __init__(self, provider_cfg, dataset, **kwargs):
            self.provider_cfg = provider_cfg
            self.dataset = dataset

        async def run(self) -> BackfillResult:
            execution_order.append((self.provider_cfg.slug, self.dataset))
            return BackfillResult(
                provider=self.provider_cfg.slug,
                dataset=self.dataset,
                status="success",
            )

    monkeypatch.setattr("aurum.external.backfill.BackfillManager", DummyManager)

    runner = BackfillRunner(settings)
    results = asyncio.run(runner.run())

    assert execution_order == [("fred", "DGS10"), ("fred", "UNRATE"), ("worldbank", "NY.GDP.MKTP.CD")]
    assert all(result.status == "success" for result in results)


def test_backfill_manager_runs_fred(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_cfg = FredDatasetConfig.from_dict(
        {
            "source_name": "DGS10",
            "series_id": "DGS10",
            "description": "10Y Treasury",
            "frequency": "daily",
            "default_units": "PCT",
            "seasonal_adjustment": "NSA",
            "window_days": 7,
            "page_limit": 1000,
        }
    )

    monkeypatch.setenv("FRED_API_KEY", "test-key")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://schema:8081")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    class DummyCollector:
        def flush(self) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(
        "aurum.external.backfill._build_kafka_collector",
        lambda *args, **kwargs: DummyCollector(),
    )
    monkeypatch.setattr(
        "aurum.external.backfill._build_http_collector",
        lambda *args, **kwargs: object(),
    )

    class DummyStore:
        def delete(self, provider: str, series_id: str) -> None:
            self.deleted = (provider, series_id)

    store = DummyStore()
    monkeypatch.setattr(
        "aurum.external.backfill._build_checkpoint_store",
        lambda: store,
    )
    monkeypatch.setattr(
        "aurum.external.backfill.load_fred_dataset_configs",
        lambda: [dataset_cfg],
    )

    emitted: dict[str, datetime] = {}

    class DummyFredCollector:
        def __init__(self, *args, **kwargs):
            pass

        def sync_catalog(self) -> int:
            return 1

        def ingest_observations(self, *, start: datetime, end: datetime) -> int:
            emitted["start"] = start
            emitted["end"] = end
            return 5

    monkeypatch.setattr("aurum.external.backfill.FredCollector", DummyFredCollector)
    monkeypatch.setattr("aurum.external.backfill.FredApiClient", lambda *args, **kwargs: object())

    provider_cfg = ProviderBackfillConfig(
        name="fred",
        datasets=("DGS10",),
        start_date=datetime(2020, 1, 1, tzinfo=timezone.utc),
        end_date=datetime(2020, 1, 2, tzinfo=timezone.utc),
        priority="high",
        max_workers=1,
        batch_size=500,
        rate_limit_rps=2.0,
    )

    manager = BackfillManager(provider_cfg, "DGS10", checkpoint_store=store)
    result = asyncio.run(manager.run())

    assert result.status == "success"
    assert result.catalog_records == 1
    assert result.observation_records == 5
    assert emitted["start"] == provider_cfg.start_date
    assert emitted["end"] == provider_cfg.end_date
