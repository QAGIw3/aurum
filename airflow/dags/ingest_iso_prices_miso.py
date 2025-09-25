"""Airflow DAG to ingest MISO Market Reports into Kafka via SeaTunnel."""
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
        "miso_da_lmp",
        description="MISO day-ahead LMP ingestion",
        schedule="15 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "miso_rt_lmp",
        description="MISO real-time LMP ingestion",
        schedule="*/5 * * * *",
        target="kafka",
    ),
)


def _miso_env_entries(*, url_var: str, market: str, interval_seconds: int) -> list[str]:
    return [
        f"MISO_URL=\"{{{{ var.value.get('{url_var}') }}}}\"",
        f"MISO_MARKET=\"{market}\"",
        "MISO_TOPIC=\"aurum.iso.miso.lmp.v1\"",
        f"MISO_INTERVAL_SECONDS=\"{interval_seconds}\"",
        "MISO_TIME_FORMAT=\"{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
        "MISO_TIME_COLUMN=\"{{ var.value.get('aurum_miso_time_column', 'Time') }}\"",
        "MISO_NODE_COLUMN=\"{{ var.value.get('aurum_miso_node_column', 'CPNode') }}\"",
        "MISO_NODE_ID_COLUMN=\"{{ var.value.get('aurum_miso_node_id_column', 'CPNode ID') }}\"",
        "MISO_LMP_COLUMN=\"{{ var.value.get('aurum_miso_lmp_column', 'LMP') }}\"",
        "MISO_CONGESTION_COLUMN=\"{{ var.value.get('aurum_miso_congestion_column', 'MCC') }}\"",
        "MISO_LOSS_COLUMN=\"{{ var.value.get('aurum_miso_loss_column', 'MLC') }}\"",
    ]


with DAG(
    dag_id="ingest_iso_prices_miso",
    description="Download MISO market reports (DA/RT) and publish LMP records via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "miso", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_miso_da_url",
                "aurum_miso_rt_url",
            ),
            optional_variables=(
                "aurum_miso_time_format",
                "aurum_miso_lmp_column",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    da_render, da_exec, da_watermark = iso_utils.create_seatunnel_ingest_chain(
        "miso_da",
        job_name="miso_lmp_to_kafka",
        source_name="miso_da_lmp",
        env_entries=_miso_env_entries(url_var="aurum_miso_da_url", market="DAY_AHEAD", interval_seconds=3600),
        pool="api_miso",
        watermark_policy="hour",
    )

    rt_render, rt_exec, rt_watermark = iso_utils.create_seatunnel_ingest_chain(
        "miso_rt",
        job_name="miso_lmp_to_kafka",
        source_name="miso_rt_lmp",
        env_entries=_miso_env_entries(url_var="aurum_miso_rt_url", market="REAL_TIME", interval_seconds=300),
        pool="api_miso",
        watermark_policy="hour",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> da_render >> da_exec >> da_watermark
    register_sources >> rt_render >> rt_exec >> rt_watermark
    [da_watermark, rt_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_miso")
