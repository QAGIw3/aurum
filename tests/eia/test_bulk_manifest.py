from __future__ import annotations

import json
from pathlib import Path

from scripts.eia.bulk_manifest import summarise_datasets, update_config_with_timestamps


def test_summarise_datasets(tmp_path: Path) -> None:
    manifest = {
        "dataset": {
            "ONE": {
                "accessURL": "https://www.eia.gov/opendata/bulk/EBA.zip",
                "last_updated": "2024-01-01T00:00:00-05:00",
                "title": "EBA Archive",
            }
        }
    }
    config_path = tmp_path / "eia_bulk.json"
    config_path.write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "source_name": "eia_bulk_eba",
                        "url": "https://api.eia.gov/bulk/EBA.zip",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    summary = summarise_datasets(manifest, config_path)
    assert summary["eia_bulk_eba"]["last_updated"] == "2024-01-01T00:00:00-05:00"


def test_update_config_with_timestamps(tmp_path: Path) -> None:
    config_path = tmp_path / "eia_bulk.json"
    config_path.write_text(
        json.dumps(
            {
                "datasets": [
                    {
                        "source_name": "eia_bulk_eba",
                        "url": "https://api.eia.gov/bulk/EBA.zip",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    summary = {
        "eia_bulk_eba": {"last_updated": "2024-01-02T12:00:00-05:00"}
    }

    update_config_with_timestamps(config_path, summary)

    payload = json.loads(config_path.read_text(encoding="utf-8"))
    assert payload["datasets"][0]["last_modified"] == "2024-01-02T12:00:00-05:00"
