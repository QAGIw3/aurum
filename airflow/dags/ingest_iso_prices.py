"""Airflow DAG for CAISO and ERCOT ingestion using helper scripts."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.db import register_ingest_source, update_ingest_watermark

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


def _register_sources() -> None:
    for name, description in (
        ("caiso_helper", "CAISO PRC_LMP helper ingestion"),
        ("ercot_helper", "ERCOT MIS helper ingestion"),
    ):
        try:
            register_ingest_source(
                name,
                description=description,
                schedule="30 * * * *",
                target="kafka",
            )
        except Exception as exc:  # pragma: no cover
            print(f"Failed to register ingest source {name}: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        update_ingest_watermark(source_name, "logical_date", watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


def build_helper_task(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{command}"
        ),
    )


with DAG(
    dag_id="ingest_iso_prices",
    description="Ingest CAISO and ERCOT ISO prices using Python helper scripts",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "caiso", "ercot", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    caiso_task = build_helper_task(
        "caiso_prc_lmp_helper",
        (
            "python scripts/ingest/caiso_prc_lmp_to_kafka.py "
            "--start {{ data_interval_start.in_timezone('UTC').isoformat() }} "
            "--end {{ data_interval_end.in_timezone('UTC').isoformat() }} "
            "--market {{ var.value.get('aurum_caiso_market', 'RTPD') }}"
        ),
    )

    ercot_task = build_helper_task(
        "ercot_mis_helper",
        (
            "python scripts/ingest/ercot_mis_to_kafka.py "
            "--url {{ var.value.get('aurum_ercot_mis_url') }}"
        ),
    )

    caiso_watermark = PythonOperator(
        task_id="caiso_watermark",
        python_callable=lambda **ctx: _update_watermark("caiso_helper", ctx["logical_date"]),
    )

    ercot_watermark = PythonOperator(
        task_id="ercot_watermark",
        python_callable=lambda **ctx: _update_watermark("ercot_helper", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources
    register_sources >> caiso_task >> caiso_watermark
    register_sources >> ercot_task >> ercot_watermark
    [caiso_watermark, ercot_watermark] >> end
