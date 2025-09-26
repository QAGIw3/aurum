"""Airflow DAG to ingest MISO Ancillary Services market prices via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
import sys
from typing import Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils


DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=30),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

SOURCES = (
    iso_utils.IngestSource(
        "miso_asm_exante",
        description="MISO ASM ex-ante prices",
        schedule="30 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "miso_asm_expost",
        description="MISO ASM ex-post prices",
        schedule="30 * * * *",
        target="kafka",
    ),
)


def build_asm_task(
    task_prefix: str,
    *,
    url_var: str,
    market: str,
    source_name: str,
    pool: str | None = None,
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    env_entries = [
        f"MISO_ASM_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        f"MISO_ASM_MARKET=\"{market}\"",
        "MISO_ASM_TOPIC=\"{{ var.value.get('aurum_miso_asm_topic', 'aurum.iso.miso.asm.v1') }}\"",
        "MISO_ASM_SUBJECT=\"{{ var.value.get('aurum_miso_asm_subject', 'aurum.iso.miso.asm.v1-value') }}\"",
    ]
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name="miso_asm_to_kafka",
        source_name=source_name,
        env_entries=env_entries,
        pool=pool,
        watermark_policy="hour",
    )


with DAG(
    dag_id="ingest_iso_asm_miso",
    description="Ingest MISO ancillary services market prices via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "asm"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_asm_exante_url",
                "aurum_miso_asm_expost_url",
            ),
            optional_variables=(
                "aurum_miso_asm_topic",
                "aurum_miso_asm_subject",
                "aurum_miso_asm_warn_threshold",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    exante_render, exante_exec, exante_watermark = build_asm_task(
        "miso_asm_exante",
        url_var="aurum_miso_asm_exante_url",
        market="DAY_AHEAD_EXANTE",
        source_name="miso_asm_exante",
        pool="api_miso",
    )

    expost_render, expost_exec, expost_watermark = build_asm_task(
        "miso_asm_expost",
        url_var="aurum_miso_asm_expost_url",
        market="DAY_AHEAD_EXPOST",
        source_name="miso_asm_expost",
        pool="api_miso",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> exante_render >> exante_exec >> exante_watermark
    register_sources >> expost_render >> expost_exec >> expost_watermark
    [exante_watermark, expost_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_asm_miso")

__all__ = ["dag"]
