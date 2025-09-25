"""Airflow DAG to ingest PJM load and generation mix via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
import sys
from typing import Any, Iterable, Tuple

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
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


SOURCES = (
    iso_utils.IngestSource(
        "pjm_load",
        description="PJM load ingestion",
        schedule="40 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "pjm_genmix",
        description="PJM generation mix ingestion",
        schedule="40 * * * *",
        target="kafka",
    ),
)


def _build_job(task_prefix: str, job_name: str, source_name: str, *, env_entries: Iterable[str], pool: str | None = None):
    mapping_flags = "--mapping secret/data/aurum/pjm:token=PJM_API_KEY"
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name=job_name,
        source_name=source_name,
        env_entries=list(env_entries),
        pre_lines=[pull_cmd],
        pool=pool,
        watermark_policy="hour",
    )


with DAG(
    dag_id="ingest_iso_metrics_pjm",
    description="Ingest PJM load and generation mix via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="40 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "pjm", "load", "genmix"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
            ),
            optional_variables=(
                "aurum_pjm_row_limit",
                "aurum_pjm_load_endpoint",
                "aurum_pjm_genmix_endpoint",
                "aurum_pjm_load_topic",
                "aurum_pjm_genmix_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    # PJM expects EPT (Eastern) for interval parameters
    load_env = [
        "PJM_LOAD_ENDPOINT='{{ var.value.get('aurum_pjm_load_endpoint', 'https://api.pjm.com/api/v1/inst_load') }}'",
        "PJM_ROW_LIMIT='{{ var.value.get('aurum_pjm_row_limit', '10000') }}'",
        "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
        "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
        "PJM_LOAD_TOPIC='{{ var.value.get('aurum_pjm_load_topic', 'aurum.iso.pjm.load.v1') }}'",
        "PJM_LOAD_SUBJECT='{{ var.value.get('aurum_pjm_load_subject', 'aurum.iso.pjm.load.v1-value') }}'",
    ]
    load_render, load_exec, load_watermark = _build_job(
        "pjm_load",
        "pjm_load_to_kafka",
        "pjm_load",
        env_entries=load_env,
        pool="api_pjm",
    )

    genmix_env = [
        "PJM_GENMIX_ENDPOINT='{{ var.value.get('aurum_pjm_genmix_endpoint', 'https://api.pjm.com/api/v1/gen_by_fuel') }}'",
        "PJM_ROW_LIMIT='{{ var.value.get('aurum_pjm_row_limit', '10000') }}'",
        "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
        "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
        "PJM_GENMIX_TOPIC='{{ var.value.get('aurum_pjm_genmix_topic', 'aurum.iso.pjm.genmix.v1') }}'",
        "PJM_GENMIX_SUBJECT='{{ var.value.get('aurum_pjm_genmix_subject', 'aurum.iso.pjm.genmix.v1-value') }}'",
    ]
    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "pjm_genmix",
        "pjm_genmix_to_kafka",
        "pjm_genmix",
        env_entries=genmix_env,
        pool="api_pjm",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_metrics_pjm")
