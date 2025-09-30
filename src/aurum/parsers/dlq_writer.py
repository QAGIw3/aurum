"""Helpers for writing canonical curve data to Iceberg with DLQ support."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Optional, Sequence

import json
import logging

import pandas as pd

from .enrichment import build_dlq_records


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class IcebergWriteResult:
    """Summary of an Iceberg write operation."""

    iceberg_rows: int
    dlq_records: int
    dlq_path: Optional[Path]


def write_dlq_json(records: Sequence[dict], directory: Path | str, as_of: Optional[date]) -> Path:
    """Persist DLQ payloads as JSONL files on the local filesystem."""

    if not records:
        raise ValueError("DLQ record sequence is empty")

    original = directory
    directory_path = Path(directory)
    if isinstance(original, str) and original.startswith("s3://"):
        raise RuntimeError("DLQ JSON output only supports local filesystem paths")
    directory_path.mkdir(parents=True, exist_ok=True)
    asof_segment = as_of.strftime("%Y%m%d") if as_of else datetime.utcnow().strftime("%Y%m%d")
    path = directory_path / f"aurum.curve.observation.dlq_{asof_segment}.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, sort_keys=True))
            fh.write("\n")
    return path


class DlqAwareIcebergWriter:
    """Wrapper around Iceberg append that can also emit DLQ payloads."""

    def __init__(
        self,
        *,
        table: Optional[str] = None,
        branch: Optional[str] = None,
        dlq_dir: Path | str | None = None,
        iceberg_writer: Callable[[pd.DataFrame, Optional[str], Optional[str]], None] | None = None,
        dlq_json_writer: Callable[[Sequence[dict], Path | str, Optional[date]], Path] | None = None,
        dlq_record_builder: Callable[[pd.DataFrame], Sequence[dict]] | None = None,
    ) -> None:
        self.table = table
        self.branch = branch
        self.dlq_dir = dlq_dir
        self._iceberg_writer = iceberg_writer or self._default_iceberg_writer
        self._dlq_writer = dlq_json_writer or write_dlq_json
        self._dlq_record_builder = dlq_record_builder or build_dlq_records

    @staticmethod
    def _default_iceberg_writer(df: pd.DataFrame, table: Optional[str], branch: Optional[str]) -> None:
        from .iceberg_writer import write_to_iceberg

        write_to_iceberg(df, table=table, branch=branch)

    def write(
        self,
        clean_df: pd.DataFrame,
        quarantine_df: pd.DataFrame,
        *,
        as_of: Optional[date] = None,
        dlq_records: Sequence[dict] | None = None,
    ) -> IcebergWriteResult:
        iceberg_rows = 0
        dlq_count = 0
        dlq_path: Optional[Path] = None

        if not clean_df.empty:
            LOGGER.debug(
                "Writing %s rows to Iceberg table=%s branch=%s",
                len(clean_df),
                self.table,
                self.branch,
            )
            self._iceberg_writer(clean_df, self.table, self.branch)
            iceberg_rows = len(clean_df)
        else:
            LOGGER.debug("No canonical rows to append to Iceberg")

        target_dir = self.dlq_dir
        records: Sequence[dict] = dlq_records or ()
        if target_dir is not None:
            if not records:
                records = tuple(self._dlq_record_builder(quarantine_df))
            if records:
                LOGGER.debug("Writing %s DLQ records to %s", len(records), target_dir)
                dlq_path = self._dlq_writer(records, target_dir, as_of)
                dlq_count = len(records)
            else:
                LOGGER.debug("No DLQ records to emit")

        return IcebergWriteResult(iceberg_rows=iceberg_rows, dlq_records=dlq_count, dlq_path=dlq_path)


__all__ = ["DlqAwareIcebergWriter", "IcebergWriteResult", "write_dlq_json"]
