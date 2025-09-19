"""Airflow DAG to ingest SPP Marketplace LMP files via staged SeaTunnel jobs."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Tuple

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


def _token_flag() -> str:
    return "{% if var.value.get('aurum_spp_token', '') %} --token {{ var.value.get('aurum_spp_token') }}{% endif %}"


def build_pipeline(prefix: str, report_var: str, interval_minutes: int) -> Tuple[BashOperator, BashOperator]:
    staging_path = f"{STAGING_DIR}/spp/{prefix}/{{{{ ds }}}}.json"
    report_value = f"{{{{ var.value.get('{report_var}') }}}}"

    token_flag = _token_flag()

    stage_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + staging_path + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                "python scripts/ingest/spp_file_api_to_kafka.py "
                f"--base-url {{ var.value.get('aurum_spp_base_url') }} --report {report_value} "
                "--date {{ data_interval_start.strftime('%Y-%m-%d') }} "
                f"--interval-minutes {interval_minutes} {token_flag} "
                f"--output-json {staging_path} --no-kafka"
            ),
        ]
    )

    stage_task = BashOperator(task_id=f"stage_spp_{prefix}", bash_command=stage_command)

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    seatunnel_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                f"SPP_INPUT_JSON={staging_path} "
                f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
                f"SCHEMA_REGISTRY_URL='{schema_registry}' "
                "scripts/seatunnel/run_job.sh spp_lmp_to_kafka"
            ),
        ]
    )

    seatunnel_task = BashOperator(task_id=f"spp_{prefix}_seatunnel", bash_command=seatunnel_command)

    return stage_task, seatunnel_task


with DAG(
    dag_id="ingest_iso_prices_spp",
    description="Download SPP LMP reports, stage to JSON, and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "spp", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    da_stage, da_seatunnel = build_pipeline("da", "aurum_spp_da_report", interval_minutes=60)
    rt_stage, rt_seatunnel = build_pipeline("rt", "aurum_spp_rt_report", interval_minutes=5)

    end = EmptyOperator(task_id="end")

    start >> [da_stage, rt_stage]
    da_stage >> da_seatunnel >> end
    rt_stage >> rt_seatunnel >> end
