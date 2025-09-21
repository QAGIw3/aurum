from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from scripts.test_fixtures import load_external_fixtures as loader


def test_build_values_clause_formats_types() -> None:
    columns = ["provider", "asof_date", "value", "metadata", "tags"]
    rows = [
        {
            "provider": "FRED",
            "asof_date": "2024-05-01",
            "value": 4.12,
            "metadata": {"release": "H.15"},
            "tags": ["rates", "treasury"],
        }
    ]
    type_map = {
        "asof_date": "date",
        "value": "double",
        "metadata": "json",
        "tags": "array_str",
    }

    rendered = loader.build_values_clause(rows, columns, type_map)
    assert "DATE '2024-05-01'" in rendered
    assert "4.12" in rendered
    assert "JSON '{\"release\": \"H.15\"}'" in rendered
    assert "ARRAY['rates', 'treasury']" in rendered


def test_render_merge_sql_replaces_placeholder(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Create a minimal merge template to avoid coupling with production files.
    template = tmp_path / "merge.sql"
    template.write_text(
        "MERGE INTO iceberg.external.series_catalog AS target\n"
        "USING staging_external_series_catalog AS source\n"
        "ON target.provider = source.provider;\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(loader, "MERGE_DIR", tmp_path)

    rows = [
        {
            "provider": "EIA",
            "series_id": "NG",
            "dataset_code": "STEO",
            "title": "Test",
            "description": None,
            "unit_code": None,
            "frequency_code": None,
            "geo_id": None,
            "status": None,
            "category": None,
            "source_url": None,
            "notes": None,
            "start_ts": None,
            "end_ts": None,
            "last_observation_ts": None,
            "asof_date": "2024-05-01",
            "tags": ["gas"],
            "metadata": {"source": "test"},
            "version": 1,
            "updated_at": "2024-05-01T00:00:00Z",
            "created_at": "2024-05-01T00:00:00Z",
            "ingest_ts": "2024-05-01T00:00:00Z",
        }
    ]

    sql = loader.render_merge_sql(
        filename="merge.sql",
        placeholder="USING staging_external_series_catalog AS source",
        columns=loader.SERIES_CATALOG_COLUMNS,
        type_map=loader.SERIES_CATALOG_TYPES,
        rows=rows,
        catalog="iceberg",
        schema="external",
    )

    assert "staging_external_series_catalog" not in sql
    assert "VALUES" in sql
    assert "iceberg.external.series_catalog" in sql
    assert "ARRAY['gas']" in sql


def test_load_series_catalog_fixture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"provider": "FRED", "series_id": "DGS10"}
    fixture_dir = tmp_path / "testdata" / "external"
    fixture_dir.mkdir(parents=True)
    target = fixture_dir / "series_catalog.jsonl"
    target.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    monkeypatch.setattr(loader, "FIXTURE_DIR", fixture_dir)

    rows = loader.load_series_catalog_fixture(target)
    assert rows == [payload]


def test_sql_literal_handles_timestamp() -> None:
    ts = "2024-05-01T12:30:00Z"
    rendered = loader.sql_literal(ts, "ts")
    assert rendered == "from_iso8601_timestamp('2024-05-01T12:30:00Z')"

