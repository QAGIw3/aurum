"""Airflow DAGs for vendor EOD ingestion with Kafka triggers."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback
from aurum.external.collect.checkpoints import PostgresCheckpointStore

from aurum.parsers.dlq_writer import DlqAwareIcebergWriter
from aurum.parsers.curve_kafka_publisher import CurveKafkaPublisher
from aurum.parsers.vendor_pipeline import VendorIngestionRunner


DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
}


def _resolve_checkpoint_store() -> PostgresCheckpointStore:
    dsn = (
        os.getenv("AURUM_COLLECTOR_CHECKPOINT_DSN")
        or os.getenv("AURUM_APP_DB_DSN")
    )
    if not dsn:
        raise RuntimeError(
            "Postgres checkpoint store DSN not configured; set AURUM_COLLECTOR_CHECKPOINT_DSN or AURUM_APP_DB_DSN"
        )
    return PostgresCheckpointStore(dsn=dsn)


def _vendor_ingest_task(vendor: str, pattern: str, **context: Any) -> Dict[str, Any]:
    drop_dir = Path(os.getenv("AURUM_VENDOR_DROP_DIR", "/opt/airflow/data/vendor"))
    output_env = os.getenv("AURUM_PARSED_OUTPUT_DIR")
    output_dir = Path(output_env) if output_env else None
    output_format = os.getenv("AURUM_OUTPUT_FORMAT", "parquet")
    quarantine_dir: Optional[Path | str] = os.getenv("AURUM_QUARANTINE_DIR")
    quarantine_format = os.getenv("AURUM_QUARANTINE_FORMAT", "parquet")

    writer = DlqAwareIcebergWriter(
        table=os.getenv("AURUM_ICEBERG_TABLE"),
        branch=os.getenv("AURUM_ICEBERG_BRANCH"),
        dlq_dir=quarantine_dir,
    )
    publisher = CurveKafkaPublisher()
    checkpoint_store = _resolve_checkpoint_store()

    runner = VendorIngestionRunner(
        vendor=vendor,
        pattern=pattern,
        drop_dir=drop_dir,
        output_dir=output_dir,
        output_format=output_format,
        checkpoint_store=checkpoint_store,
        iceberg_writer=writer,
        kafka_publisher=publisher,
        quarantine_format=quarantine_format,
        quarantine_dir=quarantine_dir,
    )

    result = runner.run()
    return {
        "vendor": vendor,
        "processed_files": result.processed_files,
        "rows_written": result.rows_written,
        "kafka_records": result.kafka_records,
        "dlq_records": result.dlq_records,
        "last_checkpoint": result.last_checkpoint,
    }


VENDORS = {
    "PW": "EOD_PW_*.xlsx",
    "EUGP": "EOD_EUGP_*.xlsx",
    "RP": "EOD_RP_*.xlsx",
}


def _build_vendor_dag(vendor: str, pattern: str) -> DAG:
    dag = DAG(
        dag_id=f"vendor_ingest_{vendor.lower()}",
        description=f"Vendor EOD ingestion for {vendor}",
        default_args=DEFAULT_ARGS,
        schedule_interval=os.getenv(f"AURUM_VENDOR_{vendor}_SCHEDULE", "15 12 * * 1-5"),
        start_date=datetime(2024, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["vendor", "curves", vendor.lower()],
    )

    with dag:
        start = EmptyOperator(task_id="start")

        ingest = PythonOperator(
            task_id="ingest_vendor_curves",
            python_callable=_vendor_ingest_task,
            op_kwargs={"vendor": vendor, "pattern": pattern},
        )

        end = EmptyOperator(task_id="end")

        chain(start, ingest, end)

    dag.on_failure_callback = build_failure_callback(source=f"aurum.vendor.{vendor.lower()}")
    return dag


for _vendor, _pattern in VENDORS.items():
    globals()[f"vendor_ingest_{_vendor.lower()}"] = _build_vendor_dag(_vendor, _pattern)


__all__ = [f"vendor_ingest_{vendor.lower()}" for vendor in VENDORS]

