"""Airflow DAG to ingest MISO Interchange data into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
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
    env_entries = [
        f"MISO_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        "MISO_TOPIC=\"aurum.iso.miso.interchange.v1\"",
        f"MISO_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "MISO_TIME_FORMAT=\"{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "MISO_TIME_COLUMN=\"{{ var.value.get('aurum_miso_interchange_time_column', 'Time') }}\"",
        "MISO_INTERFACE_COLUMN=\"{{ var.value.get('aurum_miso_interchange_interface_column', 'Interface') }}\"",
        "MISO_INTERCHANGE_COLUMN=\"{{ var.value.get('aurum_miso_interchange_column', 'Interchange') }}\"",
    ]
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name="miso_interchange_to_kafka",
        source_name=source_name,
        env_entries=env_entries,
        pool=pool,
        watermark_policy="hour",
    )


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
