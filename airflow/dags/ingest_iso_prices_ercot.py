"""Airflow DAG to ingest ERCOT MIS LMP data via staged SeaTunnel job."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=45),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
STAGING_DIR = os.environ.get("AURUM_STAGING_DIR", "files/staging")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


def _ercot_token_flag() -> str:
    # Prefer Vault-provided env ERCOT_BEARER_TOKEN if present (set below),
    # otherwise allow Airflow Variable-based token.
    # The bash command will include: ${ERCOT_BEARER_TOKEN:+--bearer-token ${ERCOT_BEARER_TOKEN}}
    return "{% if var.value.get('aurum_ercot_bearer_token', '') %} --bearer-token {{ var.value.get('aurum_ercot_bearer_token') }}{% endif %}"


with DAG(
    dag_id="ingest_iso_prices_ercot",
    description="Fetch ERCOT MIS data, stage to JSON, and publish to Kafka via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="45 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "ercot", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_ercot_mis_url",
            ),
            warn_only_variables=("aurum_ercot_bearer_token",),
        ),
    )

    staging_path = f"{STAGING_DIR}/ercot/{{{{ ds }}}}.json"

    token_flag = _ercot_token_flag()

    # Optionally pull ERCOT bearer token from Vault into ERCOT_BEARER_TOKEN
    pull_cmd = (
        "eval \"$(VAULT_ADDR=${AURUM_VAULT_ADDR:-http://127.0.0.1:8200} "
        "VAULT_TOKEN=${AURUM_VAULT_TOKEN:-aurum-dev-token} "
        "PYTHONPATH=${PYTHONPATH:-}:" + PYTHONPATH_ENTRY + " "
        + VENV_PYTHON +
        " scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/ercot:token=ERCOT_BEARER_TOKEN --format shell)\" || true"
    )

    stage_command = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + staging_path + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            pull_cmd,
            (
                "python scripts/ingest/ercot_mis_to_kafka.py "
                "--url {{ var.value.get('aurum_ercot_mis_url') }} "
                f"{token_flag} "
                "${ERCOT_BEARER_TOKEN:+--bearer-token ${ERCOT_BEARER_TOKEN}} "
                f"--output-json {staging_path} --no-kafka"
            ),
        ]
    )

    stage_ercot = BashOperator(
        task_id="stage_ercot_lmp",
        bash_command=stage_command,
        execution_timeout=timedelta(minutes=20),
        pool="api_ercot",
    )

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    seatunnel_command = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
            "cd /opt/airflow",
            f"if [ \"${{AURUM_DEBUG:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe ercot_lmp_to_kafka; fi",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                f"AURUM_EXECUTE_SEATUNNEL=0 ERCOT_INPUT_JSON={staging_path} "
                f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
                f"SCHEMA_REGISTRY_URL='{schema_registry}' "
                "scripts/seatunnel/run_job.sh ercot_lmp_to_kafka --render-only"
            ),
        ]
    )

    seatunnel_task = BashOperator(
        task_id="ercot_lmp_seatunnel",
        bash_command=seatunnel_command,
        execution_timeout=timedelta(minutes=15),
    )
    exec_k8s = BashOperator(
        task_id="ercot_execute_k8s",
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "python scripts/k8s/run_seatunnel_job.py --job-name ercot_lmp_to_kafka --wait --timeout 600"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> stage_ercot >> seatunnel_task >> exec_k8s >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_ercot")
