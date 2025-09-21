"""Airflow DAG to ingest CAISO load and generation mix via SeaTunnel."""
from __future__ import annotations

import os
from datetime import timedelta
from typing import Any, Iterable, Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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


SOURCES = (
    iso_utils.IngestSource(
        "caiso_load",
        description="CAISO load ingestion",
        schedule="30 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "caiso_genmix",
        description="CAISO generation mix ingestion",
        schedule="30 * * * *",
        target="kafka",
    ),
)


def _build_job(
    task_prefix: str,
    job_name: str,
    source_name: str,
    *,
    env_entries: Iterable[str],
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_line = " ".join(
        list(env_entries)
        + [
            f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}'",
            f"SCHEMA_REGISTRY_URL='{schema_registry}'",
        ]
    )

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=iso_utils.build_render_command(
            job_name,
            env_assignments=f"AURUM_EXECUTE_SEATUNNEL=0 {env_line}",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            debug_dump_env=True,
        ),
        execution_timeout=timedelta(minutes=10),
        pool="api_caiso",
    )

    exec_job = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=iso_utils.build_k8s_command(
            job_name,
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            timeout=600,
        ),
        execution_timeout=timedelta(minutes=20),
        pool="api_caiso",
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=iso_utils.make_watermark_callable(source_name),
    )

    return render, exec_job, watermark


with DAG(
    dag_id="ingest_iso_metrics_caiso",
    description="Ingest CAISO load and generation mix observations via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "caiso", "load", "genmix"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_caiso_load_endpoint",
                "aurum_caiso_genmix_endpoint",
            ),
            optional_variables=(
                "aurum_caiso_token",
                "aurum_caiso_load_topic",
                "aurum_caiso_genmix_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    token = "{{ var.value.get(\"aurum_caiso_token\", \"\") }}"

    load_env = [
        "CAISO_LOAD_ENDPOINT=\"{{ var.value.get(\"aurum_caiso_load_endpoint\") }}\"",
        "CAISO_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_LOAD_TOPIC=\"{{ var.value.get(\"aurum_caiso_load_topic\", \"aurum.iso.caiso.load.v1\") }}\"",
        f"CAISO_LOAD_AUTH_HEADER=\"Bearer {token}\"",
    ]

    load_render, load_exec, load_watermark = _build_job(
        "caiso_load",
        "caiso_load_to_kafka",
        "caiso_load",
        env_entries=load_env,
    )

    genmix_env = [
        "CAISO_GENMIX_ENDPOINT=\"{{ var.value.get(\"aurum_caiso_genmix_endpoint\") }}\"",
        "CAISO_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_GENMIX_TOPIC=\"{{ var.value.get(\"aurum_caiso_genmix_topic\", \"aurum.iso.caiso.genmix.v1\") }}\"",
        f"CAISO_GENMIX_AUTH_HEADER=\"Bearer {token}\"",
    ]

    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "caiso_genmix",
        "caiso_genmix_to_kafka",
        "caiso_genmix",
        env_entries=genmix_env,
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_metrics_caiso")
