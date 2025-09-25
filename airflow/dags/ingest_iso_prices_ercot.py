"""Airflow DAG to ingest ERCOT MIS LMP data via staged SeaTunnel job."""
from __future__ import annotations

import os
from datetime import timedelta
import sys
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils


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
SOURCES = (
    iso_utils.IngestSource(
        "ercot_mis_lmp",
        description="ERCOT MIS LMP ingestion",
        schedule="45 * * * *",
        target="kafka",
    ),
)


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

    render_task, exec_k8s, watermark = iso_utils.create_seatunnel_ingest_chain(
        "ercot_lmp",
        job_name="ercot_lmp_to_kafka",
        source_name="ercot_mis_lmp",
        env_entries=[f"ERCOT_INPUT_JSON={staging_path}"],
        pool="api_ercot",
        watermark_policy="hour",
    )

    end = EmptyOperator(task_id="end")

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    start >> preflight >> register_sources >> stage_ercot >> render_task >> exec_k8s >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_ercot")
