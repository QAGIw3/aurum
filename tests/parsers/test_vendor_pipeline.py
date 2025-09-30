from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from aurum.external.collect.checkpoints import Checkpoint, CheckpointStore
from aurum.parsers.dlq_writer import IcebergWriteResult
from aurum.parsers.vendor_pipeline import VendorIngestionRunner


class _InMemoryCheckpointStore(CheckpointStore):
    def __init__(self) -> None:
        self._store: dict[tuple[str, str], Checkpoint] = {}

    def get(self, provider: str, series_id: str) -> Checkpoint | None:
        return self._store.get((provider, series_id))

    def set(self, checkpoint: Checkpoint) -> None:
        self._store[(checkpoint.provider, checkpoint.series_id)] = checkpoint


class _StubWriter:
    def __init__(self) -> None:
        self.calls: list[IcebergWriteResult] = []

    def write(self, clean_df: pd.DataFrame, quarantine_df: pd.DataFrame, *, as_of=None, dlq_records=None):
        result = IcebergWriteResult(
            iceberg_rows=len(clean_df),
            dlq_records=len(dlq_records) if dlq_records else len(quarantine_df),
            dlq_path=None,
        )
        self.calls.append(result)
        return result


class _StubPublisher:
    def __init__(self) -> None:
        self.calls: list[int] = []

    def publish_dataframe(self, df: pd.DataFrame) -> int:
        self.calls.append(len(df))
        return len(df)


def _build_sample_workbook(path: Path) -> None:
    data = [
        ["ISO:", None, "PJM"],
        ["Market:", None, "DA"],
        ["Hours:", None, "ON_PEAK"],
        ["Location:", None, "AECO"],
        ["Product:", None, "power"],
        ["Units:", None, "USD/MWh"],
        [None, pd.Timestamp("2025-01-01"), 45.5],
    ]
    df = pd.DataFrame(data)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:  # type: ignore[arg-type]
        df.to_excel(writer, index=False, header=False, sheet_name="Fixed Prices - Mid")


def test_vendor_runner_processes_new_files(tmp_path: Path) -> None:
    workbook = tmp_path / "EOD_PW_20250101.xlsx"
    _build_sample_workbook(workbook)

    store = _InMemoryCheckpointStore()
    writer = _StubWriter()
    publisher = _StubPublisher()

    runner = VendorIngestionRunner(
        vendor="PW",
        pattern="EOD_PW_*.xlsx",
        drop_dir=tmp_path,
        output_dir=None,
        output_format="parquet",
        checkpoint_store=store,
        iceberg_writer=writer,
        kafka_publisher=publisher,
    )

    result = runner.run()

    assert result.processed_files == 1
    assert writer.calls[0].iceberg_rows == 1
    assert publisher.calls == [1]
    checkpoint = store.get("vendor_eod", "PW")
    assert checkpoint is not None
    assert checkpoint.metadata["file"] == "EOD_PW_20250101.xlsx"

    resume = runner.run()
    assert resume.processed_files == 0
    assert len(writer.calls) == 1
