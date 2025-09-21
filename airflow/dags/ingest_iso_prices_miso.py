"""Airflow DAG to ingest MISO Market Reports into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable


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


def build_miso_task(
    task_id: str,
    market: str,
    url_var: str,
    interval_seconds: int,
    *,
    pool: str | None = None,
) -> BashOperator:
    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"
    report_url = f"{{{{ var.value.get('{url_var}') }}}}"

    env_exports = "\n".join(
        [
            f"export MISO_URL=\"{report_url}\"",
            f"export MISO_MARKET=\"{market}\"",
            "export MISO_TOPIC=\"aurum.iso.miso.lmp.v1\"",
            f"export MISO_INTERVAL_SECONDS=\"{interval_seconds}\"",
            "export MISO_TIME_FORMAT=\"{{ var.value.get('aurum_miso_time_format', 'yyyy-MM-dd HH:mm:ss') }}\"",
            "export MISO_TIME_COLUMN=\"{{ var.value.get('aurum_miso_time_column', 'Time') }}\"",
            "export MISO_NODE_COLUMN=\"{{ var.value.get('aurum_miso_node_column', 'CPNode') }}\"",
            "export MISO_NODE_ID_COLUMN=\"{{ var.value.get('aurum_miso_node_id_column', 'CPNode ID') }}\"",
            "export MISO_LMP_COLUMN=\"{{ var.value.get('aurum_miso_lmp_column', 'LMP') }}\"",
            "export MISO_CONGESTION_COLUMN=\"{{ var.value.get('aurum_miso_congestion_column', 'MCC') }}\"",
            "export MISO_LOSS_COLUMN=\"{{ var.value.get('aurum_miso_loss_column', 'MLC') }}\"",
            f"export KAFKA_BOOTSTRAP_SERVERS=\"{kafka_bootstrap}\"",
            f"export SCHEMA_REGISTRY_URL=\"{schema_registry}\"",
        ]
    )

    command = "\n".join(
        [
            "set -euo pipefail",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi",
            "cd /opt/airflow",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe miso_lmp_to_kafka; fi",
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then env | grep -E 'DLQ_TOPIC|DLQ_SUBJECT' || true; fi",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            env_exports,
            "scripts/seatunnel/run_job.sh miso_lmp_to_kafka",
        ]
    )

    operator_kwargs: dict[str, object] = {
        "task_id": task_id,
        "bash_command": command,
        "execution_timeout": timedelta(minutes=15),
    }
    if pool:
        operator_kwargs["pool"] = pool
    return BashOperator(**operator_kwargs)


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

    da_task = build_miso_task(
        "miso_da_to_kafka",
        market="DAY_AHEAD",
        url_var="aurum_miso_da_url",
        interval_seconds=3600,
        pool="api_miso",
    )
    rt_task = build_miso_task(
        "miso_rt_to_kafka",
        market="REAL_TIME",
        url_var="aurum_miso_rt_url",
        interval_seconds=300,
        pool="api_miso",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> [da_task, rt_task] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_miso")
