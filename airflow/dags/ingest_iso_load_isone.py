"""Airflow DAG to ingest ISO-NE Load and Generation data into Kafka via SeaTunnel."""
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
        "isone_load",
        description="ISO-NE load data ingestion",
        schedule="*/5 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "isone_generation",
        description="ISO-NE generation data ingestion",
        schedule="0 8 * * *",
        target="kafka",
    ),
)


def build_isone_load_task(
    task_prefix: str,
    *,
    data_type: str,
    url_var: str,
    interval_seconds: int,
    source_name: str,
    pool: str | None = None,
):
    env_entries = [
        f"ISONE_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        f"ISONE_DATA_TYPE=\"{data_type}\"",
        f"ISONE_TOPIC=\"aurum.iso.isone.{data_type}.v1\"",
        f"ISONE_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "ISONE_TIME_FORMAT=\"{{ var.value.get('aurum_isone_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "ISONE_LOCATION_COLUMN=\"{{ var.value.get('aurum_isone_location_column', 'LocationID') }}\"",
        f"ISONE_VALUE_COLUMN=\"{{{{ var.value.get('aurum_isone_{data_type}_column', '{data_type}') }}}}\"",
    ]
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name=f"isone_{data_type}_to_kafka",
        source_name=source_name,
        env_entries=env_entries,
        pool=pool,
        watermark_policy="hour" if interval_seconds < 3600 else "day",
    )


with DAG(
    dag_id="ingest_iso_load_isone",
    description="Download ISO-NE load and generation data and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "isone", "load", "generation"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_isone_rt_url",
                "aurum_isone_generation_url",
            ),
            optional_variables=(
                "aurum_isone_time_format",
                "aurum_isone_load_column",
                "aurum_isone_generation_column",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    load_render, load_exec, load_watermark = build_isone_load_task(
        "isone_load",
        data_type="load",
        url_var="aurum_isone_rt_url",
        interval_seconds=300,
        source_name="isone_load",
        pool="api_isone",
    )

    generation_render, generation_exec, generation_watermark = build_isone_load_task(
        "isone_generation",
        data_type="generation_mix",
        url_var="aurum_isone_generation_url",
        interval_seconds=3600,
        source_name="isone_generation",
        pool="api_isone",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> generation_render >> generation_exec >> generation_watermark
    [load_watermark, generation_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_load_isone")
