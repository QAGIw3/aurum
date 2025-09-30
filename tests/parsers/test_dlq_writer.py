from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd

from aurum.parsers.dlq_writer import DlqAwareIcebergWriter, write_dlq_json


def test_write_dlq_json_creates_file(tmp_path: Path) -> None:
    records = [{"error": "missing_units", "source": "demo.xlsx"}]
    path = write_dlq_json(records, tmp_path, date(2025, 1, 1))
    assert path.exists()
    content = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(content) == 1
    assert "missing_units" in content[0]


def test_dlq_writer_persists_data_and_dlq(tmp_path: Path) -> None:
    clean_df = pd.DataFrame(
        {
            "curve_key": ["ck1"],
            "asof_date": [pd.Timestamp("2025-01-01")],
        }
    )
    quarantine_df = pd.DataFrame(
        {
            "curve_key": ["ck2"],
            "asof_date": [pd.Timestamp("2025-01-01")],
            "sheet_name": ["Fixed Prices - Mid"],
            "units_raw": ["USD/MWh"],
            "currency": ["USD"],
            "per_unit": ["MWh"],
            "price_type": ["MID"],
            "tenor_type": ["MONTHLY"],
            "tenor_label": ["2025-01"],
            "iso": ["PJM"],
            "market": ["DA"],
            "region": ["US"],
            "quarantine_reason": ["missing_measurement"],
        }
    )

    payloads = []

    def fake_writer(frame: pd.DataFrame, table: str | None, branch: str | None) -> None:
        payloads.append({
            "rows": len(frame),
            "table": table,
            "branch": branch,
        })

    dlq_dir = tmp_path / "dlq"
    writer = DlqAwareIcebergWriter(
        table="iceberg.market.curve_observation",
        branch="dev",
        dlq_dir=dlq_dir,
        iceberg_writer=fake_writer,
    )

    result = writer.write(clean_df, quarantine_df, as_of=date(2025, 1, 1))

    assert payloads == [{"rows": 1, "table": "iceberg.market.curve_observation", "branch": "dev"}]
    assert result.iceberg_rows == 1
    assert result.dlq_records == 1
    assert result.dlq_path is not None and result.dlq_path.exists()
    text = result.dlq_path.read_text(encoding="utf-8")
    assert "missing_measurement" in text


def test_dlq_writer_skips_when_no_data(tmp_path: Path) -> None:
    writer = DlqAwareIcebergWriter(dlq_dir=tmp_path / "dlq", iceberg_writer=lambda *_, **__: None)
    result = writer.write(pd.DataFrame(), pd.DataFrame(), as_of=None)
    assert result.iceberg_rows == 0
    assert result.dlq_records == 0
    assert result.dlq_path is None

