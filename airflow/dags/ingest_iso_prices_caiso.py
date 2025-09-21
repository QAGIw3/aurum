"""Airflow DAG to ingest CAISO PRC_LMP data via staged SeaTunnel job."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


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
STAGING_DIR = os.environ.get("AURUM_STAGING_DIR", "files/staging")


def _register_sources() -> None:
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "caiso_prc_lmp",
            description="CAISO PRC_LMP ingestion",
            schedule="30 * * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source caiso_prc_lmp: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "logical_date", watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


with DAG(
    dag_id="ingest_iso_prices_caiso",
    description="Download CAISO PRC_LMP data, stage to JSON, and publish to Kafka via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "caiso", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    staging_path = f"{STAGING_DIR}/caiso/{{{{ ds }}}}.json"

    stage_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + staging_path + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                "python scripts/ingest/caiso_prc_lmp_to_kafka.py "
                "--start {{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}T00:00-0000 "
                "--end {{ macros.ds_format(ds, '%Y-%m-%d', '%Y-%m-%d') }}T23:59-0000 "
                "--market {{ var.value.get('aurum_caiso_market', 'RTPD') }} "
                f"--output-json {staging_path} --no-kafka"
            ),
        ]
    )

    stage_caiso = BashOperator(task_id="stage_caiso_lmp", bash_command=stage_command)

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    seatunnel_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                f"AURUM_EXECUTE_SEATUNNEL=0 CAISO_INPUT_JSON={staging_path} "
                f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
                f"SCHEMA_REGISTRY_URL='{schema_registry}' "
                "scripts/seatunnel/run_job.sh caiso_lmp_to_kafka --render-only"
            ),
        ]
    )

    seatunnel_task = BashOperator(task_id="caiso_lmp_seatunnel", bash_command=seatunnel_command)
    exec_k8s = BashOperator(
        task_id="caiso_execute_k8s",
        bash_command=(
            "set -euo pipefail\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "python scripts/k8s/run_seatunnel_job.py --job-name caiso_lmp_to_kafka --wait --timeout 600"
        ),
    )

    end = EmptyOperator(task_id="end")

    caiso_watermark = PythonOperator(
        task_id="caiso_watermark",
        python_callable=lambda **ctx: _update_watermark("caiso_prc_lmp", ctx["logical_date"]),
    )

    start >> register_sources >> stage_caiso >> seatunnel_task >> exec_k8s >> caiso_watermark >> end
