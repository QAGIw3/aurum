"""Airflow DAG to ingest comprehensive ISO-NE web services data into Kafka via SeaTunnel."""

from __future__ import annotations

import os
from datetime import datetime, timedelta
import sys
from typing import Any

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


# Define ISO-NE data sources with their specific schedules
ISONE_SOURCES = (
    # Real-time LMP data (5-minute intervals)
    iso_utils.IngestSource(
        "isone_lmp_rtm",
        description="ISO-NE Real-Time LMP data (5-min intervals)",
        schedule="*/5 * * * *",
        target="kafka",
    ),

    # Day-ahead LMP data (daily)
    iso_utils.IngestSource(
        "isone_lmp_dam",
        description="ISO-NE Day-Ahead LMP data (daily)",
        schedule="0 6 * * *",
        target="kafka",
    ),

    # Real-time Load data (5-minute intervals)
    iso_utils.IngestSource(
        "isone_load_rtm",
        description="ISO-NE Real-Time Load data (5-min intervals)",
        schedule="*/5 * * * *",
        target="kafka",
    ),

    # Generation mix data (daily)
    iso_utils.IngestSource(
        "isone_generation_mix",
        description="ISO-NE Generation Fuel Mix data (daily)",
        schedule="0 8 * * *",
        target="kafka",
    ),

    # Ancillary services data (daily)
    iso_utils.IngestSource(
        "isone_ancillary_services",
        description="ISO-NE Ancillary Services data (daily)",
        schedule="0 6 * * *",
        target="kafka",
    ),
)


def build_isone_task(source_name: str, data_type: str, market: str, *, extra_lines: list[str] | None = None):
    """Build standard render/execute/watermark chain for ISO-NE, with Vault preflight."""
    pull_cmd = (
        "eval \"$(VAULT_ADDR=${AURUM_VAULT_ADDR:-http://127.0.0.1:8200} "
        "VAULT_TOKEN=${AURUM_VAULT_TOKEN:-aurum-dev-token} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        + os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python") +
        " scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/isone:username=ISONE_USERNAME --mapping secret/data/aurum/isone:password=ISONE_PASSWORD --format shell)\" || true"
    )
    env_entries = [
        "ISONE_URL=\"{{ var.value.get('aurum_isone_endpoint') }}\"",
        "ISONE_START=\"{{ data_interval_start.in_timezone('UTC').isoformat() }}\"",
        "ISONE_END=\"{{ data_interval_end.in_timezone('UTC').isoformat() }}\"",
        f"ISONE_SOURCE={source_name}",
        f"ISONE_DATA_TYPE={data_type}",
        f"ISONE_MARKET={market}",
    ]
    return iso_utils.create_seatunnel_ingest_chain(
        f"seatunnel_{source_name}",
        job_name=source_name,
        source_name=source_name,
        env_entries=env_entries,
        pre_lines=[pull_cmd],
        extra_lines=extra_lines or None,
        watermark_policy="hour",
    )


with DAG(
    dag_id="ingest_isone_comprehensive",
    description="Comprehensive ISO-NE web services data collection with multiple schedules",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Manual trigger only - individual sources have their own schedules
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "isone", "lmp", "load", "interchange", "asm", "genmix", "nodes", "generators"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
            optional_variables=(
                "aurum_isone_ws_username",
                "aurum_isone_ws_password",
                "aurum_isone_endpoint",
            ),
            warn_only_variables=("aurum_isone_timezone",),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(ISONE_SOURCES),
    )

    # Define data type mapping for each source
    SOURCE_DATA_TYPE_MAP = {
        "isone_lmp_rtm": ("lmp", "RTM"),
        "isone_lmp_dam": ("lmp", "DAM"),
        "isone_load_rtm": ("load", "RTM"),
        "isone_generation_mix": ("generation_mix", "RTM"),
        "isone_ancillary_services": ("ancillary_services", "DAM"),
    }

    # Create tasks for each ISO-NE data source
    task_map = {}
    for source in ISONE_SOURCES:
        source_name = source.name
        data_type, market = SOURCE_DATA_TYPE_MAP.get(source_name, ("lmp", "ALL"))

        # Create task with source-specific parameters
        render_task, exec_task, watermark_task = build_isone_task(
            source_name,
            data_type,
            market,
            extra_lines=[
                f"export ISONE_TOPIC='aurum.iso.isone.{data_type}.v1'",
            ]
        )

        start >> preflight >> register_sources >> render_task >> exec_task >> watermark_task
        task_map[source_name] = (render_task, exec_task, watermark_task)

    # Create dependency chain for reference data (nodes/generators should run before others)
    if "isone_nodes" in task_map and "isone_generators" in task_map:
        nodes_tasks = task_map["isone_nodes"]
        generators_tasks = task_map["isone_generators"]

        # Make all other tasks depend on reference data completion
        for source_name, tasks in task_map.items():
            if source_name not in ["isone_nodes", "isone_generators"]:
                render_task, exec_task, watermark_task = tasks
                nodes_tasks[2] >> render_task  # nodes watermark >> other render
                generators_tasks[2] >> render_task  # generators watermark >> other render

    end = EmptyOperator(task_id="end")

    # Connect all watermark tasks to end
    for source_name, tasks in task_map.items():
        tasks[2] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_isone_comprehensive")
