from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = REPO_ROOT / "config" / "eia_ingest_datasets.json"


@pytest.fixture(scope="module")
def datasets() -> dict[str, dict[str, object]]:
    payload = json.loads(CONFIG_PATH.read_text())
    return {entry["path"]: entry for entry in payload["datasets"]}


@pytest.mark.parametrize(
    "dataset_path",
    [
        "natural-gas/stor/wkly",
        "electricity/rto/daily-region-data",
    ],
)
def test_generated_dataset_renders_seatunnel_config(tmp_path: Path, datasets: dict[str, dict[str, object]], dataset_path: str) -> None:
    entry = datasets[dataset_path]
    env = os.environ.copy()
    env.update(
        {
            "EIA_API_KEY": "test-key",
            "EIA_SERIES_PATH": entry["path"],
            "EIA_SERIES_ID": entry.get("series_id", "MULTI_SERIES"),
            "EIA_SERIES_ID_EXPR": entry["series_id_expr"],
            "EIA_FREQUENCY": entry["frequency"],
            "EIA_TOPIC": entry["default_topic"],
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "SCHEMA_REGISTRY_URL": "http://schema-registry:8081",
            "EIA_UNITS": entry["default_units"],
            "EIA_CANONICAL_UNIT": entry.get("canonical_unit", "") or "",
            "EIA_CANONICAL_CURRENCY": entry.get("canonical_currency", "") or "",
            "EIA_UNIT_CONVERSION_JSON": json.dumps(entry.get("unit_conversion")) if entry.get("unit_conversion") is not None else "null",
            "EIA_SEASONAL_ADJUSTMENT": entry.get("seasonal_adjustment", "UNKNOWN") or "UNKNOWN",

            "EIA_AREA_EXPR": entry["area_expr"],
            "EIA_SECTOR_EXPR": entry["sector_expr"],
            "EIA_DESCRIPTION_EXPR": entry["description_expr"],
            "EIA_SOURCE_EXPR": entry["source_expr"],
            "EIA_DATASET_EXPR": entry["dataset_expr"],
            "EIA_METADATA_EXPR": entry["metadata_expr"],
            "EIA_FILTER_EXPR": entry["filter_expr"],
            "EIA_PARAM_OVERRIDES_JSON": json.dumps(entry.get("param_overrides") or []),
            "EIA_PERIOD_COLUMN": entry.get("period_column", "period"),
            "EIA_DATE_FORMAT": entry.get("date_format") or "",
            "EIA_LIMIT": str(entry.get("page_limit", 5000)),
            "EIA_DLQ_TOPIC": entry.get("dlq_topic", "aurum.ref.eia.series.dlq.v1"),
            "EIA_DLQ_SUBJECT": f"{entry.get('dlq_topic', 'aurum.ref.eia.series.dlq.v1')}-value",
            "SEATUNNEL_OUTPUT_DIR": str(tmp_path),
        }
    )
    env.setdefault("EIA_WINDOW_END", "2024-01-02T00:00:00Z")
    for key in ("window_hours", "window_days", "window_months", "window_years"):
        value = entry.get(key)
        if value is not None:
            env[f"EIA_{key.upper()}"] = str(value)

    result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "seatunnel" / "run_job.sh"), "eia_series_to_kafka", "--render-only"],
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )

    rendered = (tmp_path / "eia_series_to_kafka.conf").read_text()
    assert entry["default_topic"] in rendered
    assert "canonical_value" in rendered
    assert "period_start" in rendered
    assert result.stdout  # command prints rendered config
