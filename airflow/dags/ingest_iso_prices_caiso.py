"""Airflow DAG to ingest CAISO PRC_LMP data via staged SeaTunnel job."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


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
                "scripts/scripts/seatunnel/run_job.sh caiso_lmp_to_kafka --render-only"
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

    start >> stage_caiso >> seatunnel_task >> exec_k8s >> end
