"""Airflow DAG to ingest MISO Forecast data into Kafka via SeaTunnel."""
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
        "miso_forecast_load",
        description="MISO load forecast data ingestion",
        schedule="0 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "miso_forecast_generation",
        description="MISO generation forecast data ingestion",
        schedule="0 * * * *",
        target="kafka",
    ),
)


def build_miso_forecast_task(
    task_prefix: str,
    *,
    data_type: str,
    url_var: str,
    interval_seconds: int,
    source_name: str,
    pool: str | None = None,
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    env_entries = [
        f"MISO_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        f"MISO_DATA_TYPE=\"{data_type}\"",
        f"MISO_TOPIC=\"aurum.iso.miso.{data_type}.v1\"",
        f"MISO_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "MISO_TIME_FORMAT=\"{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "MISO_TIME_COLUMN=\"{{ var.value.get('aurum_miso_forecast_time_column', 'Time') }}\"",
        f"MISO_FORECAST_COLUMN=\"{{{{ var.value.get('aurum_miso_{data_type}_column', '{data_type}') }}}}\"",
    ]
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name=f"miso_{data_type}_to_kafka",
        source_name=source_name,
        env_entries=env_entries,
        pool=pool,
        watermark_policy="hour" if interval_seconds < 3600 else "day",
    )


with DAG(
    dag_id="ingest_iso_forecasts_miso",
    description="Download MISO forecast data and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "forecast"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_da_url",
            ),
            optional_variables=(
                "aurum_miso_time_format",
                "aurum_miso_forecast_load_column",
                "aurum_miso_forecast_generation_column",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    load_forecast_render, load_forecast_exec, load_forecast_watermark = build_miso_forecast_task(
        "miso_load_forecast",
        data_type="forecast_load",
        url_var="aurum_miso_da_url",
        interval_seconds=3600,
        source_name="miso_forecast_load",
        pool="api_miso",
    )

    gen_forecast_render, gen_forecast_exec, gen_forecast_watermark = build_miso_forecast_task(
        "miso_gen_forecast",
        data_type="forecast_generation",
        url_var="aurum_miso_da_url",
        interval_seconds=3600,
        source_name="miso_forecast_generation",
        pool="api_miso",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_forecast_render >> load_forecast_exec >> load_forecast_watermark
    register_sources >> gen_forecast_render >> gen_forecast_exec >> gen_forecast_watermark
    [load_forecast_watermark, gen_forecast_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_forecasts_miso")
