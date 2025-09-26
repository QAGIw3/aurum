"""Airflow DAG to ingest ISO-NE LMP data into Kafka via SeaTunnel."""
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
        "isone_da_lmp",
        description="ISO-NE day-ahead LMP ingestion",
        schedule="15 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "isone_rt_lmp",
        description="ISO-NE real-time LMP ingestion",
        schedule="*/5 * * * *",
        target="kafka",
    ),
)


def _isone_env_entries(*, url_var: str, market: str, interval_seconds: int) -> list[str]:
    return [
        f"ISONE_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        f"ISONE_MARKET=\"{market}\"",
        "ISONE_TOPIC=\"aurum.iso.isone.lmp.v1\"",
        f"ISONE_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "ISONE_TIME_FORMAT=\"{{ var.value.get('aurum_isone_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "ISONE_LOCATION_COLUMN=\"{{ var.value.get('aurum_isone_location_column', 'LocationID') }}\"",
        "ISONE_LMP_COLUMN=\"{{ var.value.get('aurum_isone_lmp_column', 'LMP') }}\"",
        "ISONE_ENERGY_COLUMN=\"{{ var.value.get('aurum_isone_energy_column', 'EnergyComponent') }}\"",
        "ISONE_CONGESTION_COLUMN=\"{{ var.value.get('aurum_isone_congestion_column', 'CongestionComponent') }}\"",
        "ISONE_LOSS_COLUMN=\"{{ var.value.get('aurum_isone_loss_column', 'LossComponent') }}\"",
    ]


with DAG(
    dag_id="ingest_iso_prices_isone",
    description="Download ISO-NE LMP data (DA/RT) and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "isone", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_isone_da_url",
                "aurum_isone_rt_url",
            ),
            optional_variables=(
                "aurum_isone_time_format",
                "aurum_isone_lmp_column",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    da_render, da_exec, da_watermark = iso_utils.create_seatunnel_ingest_chain(
        "isone_da",
        job_name="isone_lmp_to_kafka",
        source_name="isone_da_lmp",
        env_entries=_isone_env_entries(url_var="aurum_isone_da_url", market="DAY_AHEAD", interval_seconds=3600),
        pool="api_isone",
        watermark_policy="hour",
    )

    rt_render, rt_exec, rt_watermark = iso_utils.create_seatunnel_ingest_chain(
        "isone_rt",
        job_name="isone_lmp_to_kafka",
        source_name="isone_rt_lmp",
        env_entries=_isone_env_entries(url_var="aurum_isone_rt_url", market="REAL_TIME", interval_seconds=300),
        pool="api_isone",
        watermark_policy="hour",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> da_render >> da_exec >> da_watermark
    register_sources >> rt_render >> rt_exec >> rt_watermark
    [da_watermark, rt_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_isone")
