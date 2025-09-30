"""Vendor workbook ingestion pipeline used by Airflow orchestrators."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

import logging
import os

import pandas as pd

from aurum.external.collect.checkpoints import Checkpoint, CheckpointStore

from .dlq_writer import DlqAwareIcebergWriter, IcebergWriteResult
from .curve_kafka_publisher import CurveKafkaPublisher
from .runner import (
    parse_files,
    enrich_units_currency,
    partition_quarantine,
    write_output,
    QUARANTINE_COLUMN,
)

LOG = logging.getLogger(__name__)


def _infer_asof(df: pd.DataFrame) -> date:
    if df.empty or "asof_date" not in df.columns:
        return date.today()
    series = pd.to_datetime(df["asof_date"], errors="coerce")
    if series.notna().any():
        return series.dropna().iloc[0].date()
    return date.today()


@dataclass
class VendorIngestionResult:
    processed_files: int
    rows_written: int
    kafka_records: int
    dlq_records: int
    last_checkpoint: Optional[str]


class VendorIngestionRunner:
    """Process vendor workbooks into Iceberg and Kafka with checkpointing."""

    def __init__(
        self,
        *,
        vendor: str,
        pattern: str,
        drop_dir: Path,
        output_dir: Optional[Path],
        output_format: str,
        checkpoint_store: CheckpointStore,
        iceberg_writer: DlqAwareIcebergWriter,
        kafka_publisher: CurveKafkaPublisher,
        quarantine_format: str = "parquet",
        quarantine_dir: Optional[Path | str] = None,
    ) -> None:
        self.vendor = vendor
        self.pattern = pattern
        self.drop_dir = drop_dir
        self.output_dir = output_dir
        self.output_format = output_format
        self.checkpoint_store = checkpoint_store
        self.iceberg_writer = iceberg_writer
        self.kafka_publisher = kafka_publisher
        self.quarantine_format = quarantine_format
        self.quarantine_dir = quarantine_dir

    def run(self) -> VendorIngestionResult:
        files = self._discover_files()
        if not files:
            LOG.info("No vendor workbooks found", extra={"vendor": self.vendor})
            return VendorIngestionResult(0, 0, 0, 0, None)

        checkpoint = self.checkpoint_store.get("vendor_eod", self.vendor)
        last_timestamp = checkpoint.last_timestamp if checkpoint else None

        processed = 0
        total_rows = 0
        total_kafka = 0
        total_dlq = 0
        last_checkpoint_file: Optional[str] = None

        for path in files:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            if last_timestamp and mtime <= last_timestamp:
                LOG.debug(
                    "Skipping workbook already processed",
                    extra={"vendor": self.vendor, "file": path.name, "checkpoint": last_timestamp.isoformat()},
                )
                continue

            parsed_df = parse_files([path])
            if parsed_df.empty:
                LOG.warning("Parsed workbook produced no rows", extra={"vendor": self.vendor, "file": path.name})
                self._update_checkpoint(mtime, path.name)
                last_timestamp = mtime
                last_checkpoint_file = path.name
                continue

            enriched_df = enrich_units_currency(parsed_df)
            clean_df, quarantine_df = partition_quarantine(enriched_df)
            canonical_df = clean_df.drop(columns=[QUARANTINE_COLUMN], errors="ignore")
            selected_asof = _infer_asof(canonical_df if not canonical_df.empty else enriched_df)

            if self.output_dir is not None:
                target = self.output_dir / self.vendor.lower()
                write_output(canonical_df, target, as_of=selected_asof, fmt=self.output_format)
                if not quarantine_df.empty:
                    write_output(
                        quarantine_df,
                        target,
                        as_of=selected_asof,
                        fmt=self.quarantine_format,
                        prefix="quarantine_curves",
                    )

            writer_result = self.iceberg_writer.write(
                canonical_df,
                quarantine_df,
                as_of=selected_asof,
            )

            kafka_records = self.kafka_publisher.publish_dataframe(canonical_df)

            processed += 1
            total_rows += writer_result.iceberg_rows
            total_kafka += kafka_records
            total_dlq += writer_result.dlq_records
            self._update_checkpoint(mtime, path.name, metadata={"rows": writer_result.iceberg_rows})
            last_timestamp = mtime
            last_checkpoint_file = path.name

        return VendorIngestionResult(
            processed_files=processed,
            rows_written=total_rows,
            kafka_records=total_kafka,
            dlq_records=total_dlq,
            last_checkpoint=last_checkpoint_file,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _discover_files(self) -> List[Path]:
        if not self.drop_dir.exists():
            LOG.warning("Drop directory does not exist", extra={"vendor": self.vendor, "drop_dir": str(self.drop_dir)})
            return []
        files = sorted(self.drop_dir.glob(self.pattern))
        return [path for path in files if path.is_file()]

    def _update_checkpoint(self, timestamp: datetime, filename: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        payload = metadata or {}
        payload.update({"file": filename})
        checkpoint = Checkpoint(
            provider="vendor_eod",
            series_id=self.vendor,
            last_timestamp=timestamp,
            metadata=payload,
        )
        self.checkpoint_store.set(checkpoint)


__all__ = ["VendorIngestionRunner", "VendorIngestionResult"]
