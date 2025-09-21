"""Airflow DAG to maintain Iceberg tables by expiring snapshots and compacting files."""
from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

from aurum.iceberg import maintenance

RETENTION_DAYS = int(os.getenv("AURUM_ICEBERG_RETENTION_DAYS", "14"))
TARGET_FILE_MB = int(os.getenv("AURUM_ICEBERG_TARGET_FILE_MB", "128"))
SCHEDULE = os.getenv("AURUM_ICEBERG_MAINTENANCE_SCHEDULE", "0 4 * * *")
TABLES_ENV = os.getenv("AURUM_ICEBERG_TABLES")
DEFAULT_TABLES = [
    "iceberg.raw.curve_landing",
    "iceberg.market.curve_observation",
    "iceberg.market.curve_observation_quarantine",
    "iceberg.market.scenario_output",
]
TABLES = [table.strip() for table in TABLES_ENV.split(",")] if TABLES_ENV else DEFAULT_TABLES
ORPHAN_RETENTION_HOURS = int(os.getenv("AURUM_ICEBERG_ORPHAN_RETAIN_HOURS", "24") or 24)
SLA_MINUTES = int(os.getenv("AURUM_ICEBERG_SLA_MINUTES", "45") or 45)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

LOGGER = logging.getLogger(__name__)

_ALERT_TOPIC = os.getenv("AURUM_ALERT_TOPIC", "aurum.alert.v1")
_ALERT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "alert.v1.avsc"


def _emit_alert(severity: str, message: str, *, context: Optional[Dict[str, Any]] = None) -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        LOGGER.debug("Kafka bootstrap not configured; skipping alert: %s", message)
        return
    try:  # pragma: no cover - requires confluent stack
        from confluent_kafka import avro  # type: ignore
        from confluent_kafka.avro import AvroProducer  # type: ignore
    except Exception:
        LOGGER.warning("confluent-kafka not available; cannot emit maintenance alert")
        return

    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    try:
        value_schema = avro.load(str(_ALERT_SCHEMA_PATH))
    except Exception as exc:
        LOGGER.warning("Failed to load alert schema: %s", exc)
        return

    try:
        producer = AvroProducer(
            {"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url},
            default_value_schema=value_schema,
        )
        payload = {
            "alert_id": str(uuid.uuid4()),
            "tenant_id": None,
            "category": "OPERATIONS",
            "severity": severity,
            "source": "aurum.airflow.iceberg_maintenance",
            "message": message,
            "payload": json.dumps(context or {}),
            "created_ts": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
        }
        producer.produce(topic=_ALERT_TOPIC, value=payload)
        producer.flush(2)
    except Exception as exc:
        LOGGER.warning("Unable to emit maintenance alert: %s", exc)


def _dag_failure_callback(context: Dict[str, Any]) -> None:
    dag_run = context.get("dag_run")
    message = "Iceberg maintenance DAG failed"
    details = {
        "dag_id": context.get("dag_id"),
        "run_id": getattr(dag_run, "run_id", None),
    }
    _emit_alert("CRITICAL", message, context=details)


def _check_sla(**context):
    dag_run = context.get("dag_run")
    start_date = getattr(dag_run, "start_date", None)
    if start_date is None:
        return
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)
    duration = now - start_date
    minutes = duration.total_seconds() / 60.0
    if minutes > SLA_MINUTES:
        _emit_alert(
            "WARN",
            f"Iceberg maintenance exceeded SLA ({minutes:.2f} minutes)",
            context={"duration_minutes": round(minutes, 2)},
        )
    return {"duration_minutes": round(minutes, 2)}

with DAG(
    dag_id="iceberg_maintenance",
    description="Expire stale Iceberg snapshots and compact small files",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=SCHEDULE,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["iceberg", "maintenance"],
) as dag:
    dag.on_failure_callback = _dag_failure_callback
    previous_task = None
    for table_name in TABLES:
        suffix = table_name.replace(".", "_")

        expire_task = PythonOperator(
            task_id=f"expire_snapshots_{suffix}",
            python_callable=maintenance.expire_snapshots,
            op_kwargs={
                "table_name": table_name,
                "older_than_days": RETENTION_DAYS,
                "dry_run": False,
            },
        )

        compact_task = PythonOperator(
            task_id=f"compact_files_{suffix}",
            python_callable=maintenance.rewrite_data_files,
            op_kwargs={
                "table_name": table_name,
                "target_file_size_mb": TARGET_FILE_MB,
            },
        )

        purge_task = PythonOperator(
            task_id=f"purge_orphans_{suffix}",
            python_callable=maintenance.purge_orphan_files,
            op_kwargs={
                "table_name": table_name,
                "older_than_hours": ORPHAN_RETENTION_HOURS,
            },
        )

        expire_task >> compact_task >> purge_task

        if previous_task is not None:
            previous_task >> expire_task
        previous_task = purge_task

    sla_task = PythonOperator(
        task_id="check_sla",
        python_callable=_check_sla,
        trigger_rule="all_done",
    )

    if previous_task is not None:
        previous_task >> sla_task

__all__ = ["dag"]
