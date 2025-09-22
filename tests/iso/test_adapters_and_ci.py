from __future__ import annotations

import json
from pathlib import Path

from aurum.external.adapters.miso import MisoAdapter
from aurum.external.adapters.caiso import CaisoAdapter
from aurum.external.adapters.isone import IsoneAdapter
from aurum.external.adapters.base import IsoRequestChunk
from unittest.mock import patch, MagicMock


def _mk_chunk():
    import datetime as dt
    start = dt.datetime(2025, 1, 22, 0, 0, tzinfo=dt.timezone.utc)
    end = dt.datetime(2025, 1, 22, 1, 0, tzinfo=dt.timezone.utc)
    return IsoRequestChunk(start=start, end=end)


def test_parse_pages_with_golden_samples(tmp_path: Path) -> None:
    # Mock the PostgresCheckpointStore to avoid database connection
    with patch('aurum.external.collect.PostgresCheckpointStore') as mock_store_class:
        mock_store = MagicMock()
        mock_store_class.return_value = mock_store
        mock_store._ensure_table = MagicMock()  # Mock the table creation

        # MISO
        miso = MisoAdapter(series_id="test.miso.lmp", kafka_topic="aurum.iso.miso.lmp.v1")
        payload = json.loads((Path(__file__).parents[2] / "testdata/iso/miso_lmp_sample.json").read_text())
        records, next_cur = miso.parse_page(payload)
        assert isinstance(records, list) and len(records) == 1
        assert records[0]["record_hash"]
        assert next_cur is None or isinstance(next_cur, str)

        # CAISO
        caiso = CaisoAdapter(series_id="test.caiso.lmp", kafka_topic="aurum.iso.caiso.lmp.v1")
        payload = json.loads((Path(__file__).parents[2] / "testdata/iso/caiso_lmp_sample.json").read_text())
        records, next_cur = caiso.parse_page(payload)
        assert len(records) == 1
        assert records[0]["timezone"]

        # ISO-NE
        isone = IsoneAdapter(series_id="test.isone.lmp", kafka_topic="aurum.iso.isone.lmp.v1")
        payload = json.loads((Path(__file__).parents[2] / "testdata/iso/isone_lmp_sample.json").read_text())
        records, next_cur = isone.parse_page(payload)
        assert len(records) == 1
        assert records[0]["price_total"] == 28.5


def test_contracts_include_isone_subjects() -> None:
    """Ensure ISO-NE subjects are present for load/genmix/lmp."""
    subjects_path = Path(__file__).resolve().parents[2] / "kafka/schemas/subjects.json"
    subjects = json.loads(subjects_path.read_text())
    assert "aurum.iso.isone.lmp.v1-value" in subjects
    assert "aurum.iso.isone.load.v1-value" in subjects
    assert "aurum.iso.isone.genmix.v1-value" in subjects


def test_openapi_pinning_for_miso() -> None:
    pin_path = Path(__file__).resolve().parents[2] / "openapi/miso_spec.pin.json"
    data = json.loads(pin_path.read_text())
    assert data["rtdb_endpoint"].startswith("/MISORTDBService/rest/data/")

