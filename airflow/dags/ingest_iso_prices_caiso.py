"""Airflow DAG to ingest CAISO PRC_LMP data via staged SeaTunnel job."""
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


SOURCES = (
    iso_utils.IngestSource(
        "caiso_prc_lmp",
        description="CAISO PRC_LMP ingestion",
        schedule="30 * * * *",
        target="kafka",
    ),
)


def _register_sources() -> None:
    iso_utils.register_sources(SOURCES)


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

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_caiso_market",
            ),
            optional_variables=(
                "aurum_caiso_topic",
                "aurum_caiso_prc_lmp_url",
            ),
        ),
    )

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    staging_path = f"{STAGING_DIR}/caiso/{{{{ ds }}}}.json"

    stage_command = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
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

    stage_caiso = BashOperator(
        task_id="stage_caiso_lmp",
        bash_command=stage_command,
        execution_timeout=timedelta(minutes=20),
        pool="api_caiso",
    )

    render_task, exec_k8s, caiso_watermark = iso_utils.create_seatunnel_ingest_chain(
        "caiso_lmp",
        job_name="caiso_lmp_to_kafka",
        source_name="caiso_prc_lmp",
        env_entries=[
            f"CAISO_INPUT_JSON={staging_path}",
        ],
        pool="api_caiso",
        watermark_policy="hour",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> stage_caiso >> render_task >> exec_k8s >> caiso_watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_caiso")
