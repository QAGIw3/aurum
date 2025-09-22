import asyncio
import json
from pathlib import Path

import pytest

from aurum.external.incremental import (
    IncrementalRunner,
    _FredIncrementalProcessor,
)
from aurum.external.providers.fred import FredDatasetConfig


def test_incremental_runner_passes_dataset_filters(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config_payload = {
        "providers": [
            {
                "name": "fred",
                "datasets": ["DGS10", "UNRATE"],
                "update_frequency_minutes": 120,
                "rate_limit_rps": 4,
                "daily_quota": 1000,
                "batch_size": 250,
            }
        ],
        "global_settings": {"max_concurrent_providers": 1},
        "monitoring": {},
    }
    config_path = tmp_path / "incremental.json"
    config_path.write_text(json.dumps(config_payload), encoding="utf-8")

    calls = []

    async def fake_run(self, provider: str):
        calls.append((self.config.provider, self.config.datasets, provider))
        return {"provider": provider, "status": "ok"}

    monkeypatch.setattr(
        "aurum.external.incremental.IncrementalProcessor.run_incremental_update",
        fake_run,
        raising=False,
    )
    monkeypatch.setattr(
        "aurum.external.incremental._build_checkpoint_store",
        lambda: object(),
    )

    runner = IncrementalRunner.from_file(config_path)
    results = asyncio.run(runner.run())

    assert results == [{"provider": "fred", "status": "ok"}]
    assert calls == [("fred", ("DGS10", "UNRATE"), "fred")]


def test_fred_incremental_processor_filters(monkeypatch: pytest.MonkeyPatch) -> None:
    dataset_cfg = FredDatasetConfig.from_dict(
        {
            "source_name": "fred_dgs10",
            "series_id": "DGS10",
            "description": "10Y Treasury",
            "frequency": "daily",
            "default_units": "PCT",
            "seasonal_adjustment": "NSA",
            "window_days": 7,
            "page_limit": 1000,
        }
    )
    other_cfg = FredDatasetConfig.from_dict(
        {
            "source_name": "fred_unrate",
            "series_id": "UNRATE",
            "description": "Unemployment",
            "frequency": "monthly",
            "default_units": "PCT",
            "seasonal_adjustment": "SA",
            "window_days": 7,
            "page_limit": 1000,
        }
    )

    monkeypatch.setattr(
        "aurum.external.incremental.load_fred_dataset_configs",
        lambda: [dataset_cfg, other_cfg],
    )

    processor = _FredIncrementalProcessor(
        catalog_collector=object(),
        obs_collector=object(),
        checkpoint_store=object(),
        datasets=["DGS10"],
    )

    filtered = processor._load_datasets()

    assert [cfg.series_id for cfg in filtered] == ["DGS10"]
