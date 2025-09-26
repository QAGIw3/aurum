"""Airflow DAG to ingest SPP Marketplace LMP files via staged SeaTunnel jobs."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
import sys
from typing import Any, Tuple

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

SPP_SOURCES = (
    iso_utils.IngestSource(
        "spp_da_lmp",
        description="SPP day-ahead LMP ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "spp_rt_lmp",
        description="SPP real-time LMP ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
)


def _register_sources() -> None:
    iso_utils.register_sources(SPP_SOURCES)


def _token_flag() -> str:
    # Airflow Variable-based token (Vault-provided env handled in bash below)
    return "{% if var.value.get('aurum_spp_token', '') %} --token {{ var.value.get('aurum_spp_token') }}{% endif %}"


def build_pipeline(
    prefix: str,
    report_var: str,
    interval_minutes: int,
    *,
    source_name: str,
) -> Tuple[BashOperator, BashOperator, BashOperator, PythonOperator]:
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
        "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
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

    stage_task = BashOperator(
        task_id=f"stage_spp_{prefix}",
        bash_command=stage_command,
        execution_timeout=timedelta(minutes=20),
        pool="api_spp",
    )

    render_task, run_k8s_task, watermark_task = iso_utils.create_seatunnel_ingest_chain(
        f"spp_{prefix}",
        job_name="spp_lmp_to_kafka",
        source_name=source_name,
        env_entries=[
            f"SPP_INPUT_JSON={staging_path}",
            # Retain schema path if template needs it
            "ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc",
        ],
        pool="api_spp",
        watermark_policy="hour",
    )

    return stage_task, render_task, run_k8s_task, watermark_task


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

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_spp_file_market_url",
            ),
            optional_variables=(
                "aurum_spp_topic",
                "aurum_spp_bearer_token",
            ),
        ),
    )

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    da_stage, da_seatunnel, da_exec, da_watermark = build_pipeline(
        "da",
        "aurum_spp_da_report",
        interval_minutes=60,
        source_name="spp_da_lmp",
    )
    rt_stage, rt_seatunnel, rt_exec, rt_watermark = build_pipeline(
        "rt",
        "aurum_spp_rt_report",
        interval_minutes=5,
        source_name="spp_rt_lmp",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> [da_stage, rt_stage]
    da_stage >> da_seatunnel >> da_exec >> da_watermark >> end
    rt_stage >> rt_seatunnel >> rt_exec >> rt_watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_spp")
