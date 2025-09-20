"""Airflow DAG to maintain Iceberg tables by expiring snapshots and compacting files."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from aurum.iceberg import maintenance

RETENTION_DAYS = int(os.getenv("AURUM_ICEBERG_RETENTION_DAYS", "14"))
TARGET_FILE_MB = int(os.getenv("AURUM_ICEBERG_TARGET_FILE_MB", "128"))
SCHEDULE = os.getenv("AURUM_ICEBERG_MAINTENANCE_SCHEDULE", "0 4 * * *")
TABLES = [
    "iceberg.market.curve_observation",
    "iceberg.market.scenario_output",
]

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

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

        expire_task >> compact_task

        if previous_task is not None:
            previous_task >> expire_task
        previous_task = compact_task

__all__ = ["dag"]
