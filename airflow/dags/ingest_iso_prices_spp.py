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
    # Airflow Variable-based token (Vault-provided env handled in bash below)
    return "{% if var.value.get('aurum_spp_token', '') %} --token {{ var.value.get('aurum_spp_token') }}{% endif %}"


def build_pipeline(prefix: str, report_var: str, interval_minutes: int) -> Tuple[BashOperator, BashOperator, BashOperator]:
    staging_path = f"{STAGING_DIR}/spp/{prefix}/{{{{ ds }}}}.json"
    report_value = f"{{{{ var.value.get('{report_var}') }}}}"

    token_flag = _token_flag()

    # Optionally pull SPP token from Vault into SPP_TOKEN
    pull_cmd = (
        "eval \"$(VAULT_ADDR=${AURUM_VAULT_ADDR:-http://127.0.0.1:8200} "
        "VAULT_TOKEN=${AURUM_VAULT_TOKEN:-aurum-dev-token} "
        "PYTHONPATH=${PYTHONPATH:-}:" + PYTHONPATH_ENTRY + " "
        + os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python") +
        " scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/spp:token=SPP_TOKEN --format shell)\" || true"
    )

    # Build the stage command carefully to preserve Jinja expressions (avoid f-strings around {{ }})
    stage_command = "\n".join([
        "set -euo pipefail",
        "cd /opt/airflow",
        "mkdir -p $(dirname '" + staging_path + "')",
        f"export PATH=\"{BIN_PATH}\"",
        f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
        pull_cmd,
        (
            "python scripts/ingest/spp_file_api_to_kafka.py "
            "--base-url {{ var.value.get('aurum_spp_base_url') }} "
            "--report " + report_value + " "
            "--date {{ data_interval_start.strftime('%Y-%m-%d') }} "
            f"--interval-minutes {interval_minutes} "
            + token_flag + " ${SPP_TOKEN:+--token ${SPP_TOKEN}} "
            f"--output-json {staging_path} --no-kafka"
        ),
    ])

    stage_task = BashOperator(task_id=f"stage_spp_{prefix}", bash_command=stage_command)

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    seatunnel_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            "export ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/scripts/kafka/schemas/iso.lmp.v1.avsc",
            (
                f"AURUM_EXECUTE_SEATUNNEL=0 SPP_INPUT_JSON={staging_path} "
                f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
                f"SCHEMA_REGISTRY_URL='{schema_registry}' "
                "scripts/scripts/seatunnel/run_job.sh spp_lmp_to_kafka --render-only"
            ),
        ]
    )

    seatunnel_task = BashOperator(task_id=f"spp_{prefix}_seatunnel", bash_command=seatunnel_command)

    run_k8s_command = "\n".join([
        "set -euo pipefail",
        "cd /opt/airflow",
        f"export PATH=\"{BIN_PATH}\"",
        f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
        "python scripts/k8s/run_seatunnel_job.py --job-name spp_lmp_to_kafka --wait --timeout 600",
    ])
    run_k8s_task = BashOperator(task_id=f"spp_{prefix}_execute_k8s", bash_command=run_k8s_command)

    return stage_task, seatunnel_task, run_k8s_task


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

    da_stage, da_seatunnel, da_exec = build_pipeline("da", "aurum_spp_da_report", interval_minutes=60)
    rt_stage, rt_seatunnel, rt_exec = build_pipeline("rt", "aurum_spp_rt_report", interval_minutes=5)

    end = EmptyOperator(task_id="end")

    start >> [da_stage, rt_stage]
    da_stage >> da_seatunnel >> da_exec >> end
    rt_stage >> rt_seatunnel >> rt_exec >> end
