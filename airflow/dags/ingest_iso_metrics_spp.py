"""Airflow DAG to ingest SPP load and generation mix via SeaTunnel."""
from __future__ import annotations

import os
from datetime import timedelta
import sys
from typing import Any, Iterable, Tuple

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
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


SOURCES = (
    iso_utils.IngestSource(
        "spp_load",
        description="SPP load ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
    iso_utils.IngestSource(
        "spp_genmix",
        description="SPP generation mix ingestion",
        schedule="20 * * * *",
        target="kafka",
    ),
)


def _build_job(task_prefix: str, job_name: str, source_name: str, *, env_entries: Iterable[str]):
    return iso_utils.create_seatunnel_ingest_chain(
        task_prefix,
        job_name=job_name,
        source_name=source_name,
        env_entries=list(env_entries),
        pool="api_spp",
        watermark_policy="hour",
    )


with DAG(
    dag_id="ingest_iso_metrics_spp",
    description="Ingest SPP load and generation mix observations via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "spp", "load", "genmix"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_spp_load_endpoint",
                "aurum_spp_genmix_endpoint",
            ),
            optional_variables=(
                "aurum_spp_token",
                "aurum_spp_load_topic",
                "aurum_spp_genmix_topic",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    token = "{{ var.value.get(\"aurum_spp_token\", \"\") }}"

    load_env = [
        "SPP_LOAD_ENDPOINT=\"{{ var.value.get(\"aurum_spp_load_endpoint\") }}\"",
        "SPP_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "SPP_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "SPP_LOAD_TOPIC=\"{{ var.value.get(\"aurum_spp_load_topic\", \"aurum.iso.spp.load.v1\") }}\"",
        f"SPP_LOAD_AUTH_HEADER=\"Bearer {token}\"",
    ]

    load_render, load_exec, load_watermark = _build_job(
        "spp_load",
        "spp_load_to_kafka",
        "spp_load",
        env_entries=load_env,
    )

    genmix_env = [
        "SPP_GENMIX_ENDPOINT=\"{{ var.value.get(\"aurum_spp_genmix_endpoint\") }}\"",
        "SPP_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "SPP_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "SPP_GENMIX_TOPIC=\"{{ var.value.get(\"aurum_spp_genmix_topic\", \"aurum.iso.spp.genmix.v1\") }}\"",
        f"SPP_GENMIX_AUTH_HEADER=\"Bearer {token}\"",
    ]

    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "spp_genmix",
        "spp_genmix_to_kafka",
        "spp_genmix",
        env_entries=genmix_env,
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_metrics_spp")
