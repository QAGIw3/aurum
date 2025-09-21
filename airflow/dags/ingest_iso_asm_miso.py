"""Airflow DAG to ingest MISO Ancillary Services market prices via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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
    env_parts = [
        f"MISO_ASM_URL='{{{{ var.value.get('{url_var}') }}}}'",
        f"MISO_ASM_MARKET='{market}'",
        "MISO_ASM_TOPIC='{{ var.value.get('aurum_miso_asm_topic', 'aurum.iso.miso.asm.v1') }}'",
        "MISO_ASM_SUBJECT='{{ var.value.get('aurum_miso_asm_subject', 'aurum.iso.miso.asm.v1-value') }}'",
        "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
        "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
    ]
    env_line = " ".join(env_parts)

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=iso_utils.build_render_command(
            "miso_asm_to_kafka",
            env_assignments=f"AURUM_EXECUTE_SEATUNNEL=0 {env_line}",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            debug_dump_env=True,
        ),
        execution_timeout=timedelta(minutes=10),
        pool=pool,
    )

    exec_task = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=iso_utils.build_k8s_command(
            "miso_asm_to_kafka",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            timeout=600,
        ),
        execution_timeout=timedelta(minutes=20),
        pool=pool,
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=iso_utils.make_watermark_callable(source_name),
    )

    return render, exec_task, watermark


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
