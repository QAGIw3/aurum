"""Airflow DAG to ingest ISO-NE and SPP feeds via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
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
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")

SOURCES = (
    iso_utils.IngestSource(
        "isone_ws",
        description="ISO-NE web services ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "spp_file",
        description="SPP market file ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
)


def _isone_chain():
    mapping_flags = [
        "secret/data/aurum/isone:username=ISONE_USERNAME",
        "secret/data/aurum/isone:password=ISONE_PASSWORD",
    ]
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py "
        + " ".join(f"--mapping {m}" for m in mapping_flags)
        + " --format shell)\" || true"
    )
    return iso_utils.create_seatunnel_ingest_chain(
        "isone_lmp",
        job_name="isone_lmp_to_kafka",
        source_name="isone_ws",
        env_entries=[
            "ISONE_URL=\"{{ var.value.get('aurum_isone_endpoint') }}\"",
            "ISONE_START=\"{{ data_interval_start.in_timezone('UTC').isoformat() }}\"",
            "ISONE_END=\"{{ data_interval_end.in_timezone('UTC').isoformat() }}\"",
            "ISONE_MARKET=\"{{ var.value.get('aurum_isone_market', 'DA') }}\"",
        ],
        pre_lines=[pull_cmd],
        pool="api_isone",
        watermark_policy="hour",
    )

def _spp_chain(staging_path: str):
    return iso_utils.create_seatunnel_ingest_chain(
        "spp_lmp",
        job_name="spp_lmp_to_kafka",
        source_name="spp_file",
        env_entries=[
            f"SPP_INPUT_JSON={staging_path}",
            "SPP_TOPIC=\"{{ var.value.get('aurum_spp_topic', 'aurum.iso.spp.lmp.v1') }}\"",
            "ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc",
        ],
        extra_lines=["export ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc"],
        pool="api_spp",
        watermark_policy="hour",
    )


with DAG(
    dag_id="ingest_iso_prices_isone_spp",
    description="Ingest ISO-NE and SPP feeds via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["aurum", "isone", "spp", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
            optional_variables=(
                "aurum_isone_ws_username",
                "aurum_isone_ws_password",
                "aurum_spp_file_base_url",
                "aurum_spp_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    isone_render, isone_exec, isone_watermark = _isone_chain()

    # SPP: stage JSON via Python helper, then publish via SeaTunnel LocalFile source
    spp_staging = os.environ.get("AURUM_STAGING_DIR", "files/staging") + "/spp/{{ ds }}.json"

    stage_spp_cmd = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + spp_staging + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                "python scripts/ingest/spp_file_api_to_kafka.py "
                "--base-url {{ var.value.get('aurum_spp_base_url') }} "
                "--report {{ var.value.get('aurum_spp_rt_report') }} "
                "--date {{ ds }} "
                f"--output-json {spp_staging} --no-kafka"
            ),
        ]
    )
    stage_spp = BashOperator(
        task_id="stage_spp_lmp",
        bash_command=stage_spp_cmd,
        execution_timeout=timedelta(minutes=20),
    )

    spp_render, spp_exec, spp_watermark = _spp_chain(spp_staging)

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> isone_render >> isone_exec >> isone_watermark
    register_sources >> stage_spp >> spp_render >> spp_exec >> spp_watermark
    [isone_watermark, spp_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_isone_spp")
