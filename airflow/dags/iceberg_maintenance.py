"""Airflow DAG to maintain Iceberg tables by expiring snapshots and compacting files."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, emit_alert
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
        emit_alert(
            "WARN",
            f"Iceberg maintenance exceeded SLA ({minutes:.2f} minutes)",
            source="aurum.airflow.iceberg_maintenance",
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
    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.iceberg_maintenance")
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
