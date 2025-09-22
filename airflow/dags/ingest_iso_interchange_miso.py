"""Airflow DAG to ingest MISO Interchange data into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Tuple

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
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=45),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

SOURCES = (
    iso_utils.IngestSource(
        "miso_interchange",
        description="MISO interchange data ingestion",
        schedule="*/5 * * * *",
        target="kafka",
    ),
)


def build_miso_interchange_task(
    task_prefix: str,
    *,
    url_var: str,
    interval_seconds: int,
    source_name: str,
    pool: str | None = None,
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_parts = [
        f"MISO_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        "MISO_TOPIC=\"aurum.iso.miso.interchange.v1\"",
        f"MISO_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "MISO_TIME_FORMAT=\"{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "MISO_TIME_COLUMN=\"{{ var.value.get('aurum_miso_interchange_time_column', 'Time') }}\"",
        "MISO_INTERFACE_COLUMN=\"{{ var.value.get('aurum_miso_interchange_interface_column', 'Interface') }}\"",
        "MISO_INTERCHANGE_COLUMN=\"{{ var.value.get('aurum_miso_interchange_column', 'Interchange') }}\"",
        f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}'",
        f"SCHEMA_REGISTRY_URL='{schema_registry}'",
    ]
    env_line = " ".join(env_parts)

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=iso_utils.build_render_command(
            "miso_interchange_to_kafka",
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
            "miso_interchange_to_kafka",
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
    dag_id="ingest_iso_interchange_miso",
    description="Download MISO interchange data and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "interchange"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_rt_url",
            ),
            optional_variables=(
                "aurum_miso_time_format",
                "aurum_miso_interchange_column",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    interchange_render, interchange_exec, interchange_watermark = build_miso_interchange_task(
        "miso_interchange",
        url_var="aurum_miso_rt_url",
        interval_seconds=300,
        source_name="miso_interchange",
        pool="api_miso",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> interchange_render >> interchange_exec >> interchange_watermark
    interchange_watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_interchange_miso")
