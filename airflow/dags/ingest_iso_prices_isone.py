"""Airflow DAG to ingest ISO-NE web services LMP into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


with DAG(
    dag_id="ingest_iso_prices_isone",
    description="Fetch ISO-NE LMP via web services and publish to Kafka (SeaTunnel)",
    default_args=DEFAULT_ARGS,
    schedule_interval="10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "isone", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    env_exports = "\n".join(
        [
            "export ISONE_URL=\"{{ var.value.get('aurum_isone_endpoint') }}\"",
            "export ISONE_START=\"{{ data_interval_start.in_timezone('UTC').isoformat() }}\"",
            "export ISONE_END=\"{{ data_interval_end.in_timezone('UTC').isoformat() }}\"",
            "export ISONE_MARKET=\"{{ var.value.get('aurum_isone_market', 'DA') }}\"",
            "export ISONE_TOPIC=\"aurum.iso.isone.lmp.v1\"",
            "export ISONE_AUTH_HEADER=\"{{ var.value.get('aurum_isone_auth_header', '') }}\"",
            "export ISONE_USERNAME=\"{{ var.value.get('aurum_isone_username', '') }}\"",
            "export ISONE_PASSWORD=\"{{ var.value.get('aurum_isone_password', '') }}\"",
            "export ISONE_NODE_FIELD=\"{{ var.value.get('aurum_isone_node_field', 'name') }}\"",
            "export ISONE_NODE_ID_FIELD=\"{{ var.value.get('aurum_isone_node_id_field', 'ptid') }}\"",
            "export ISONE_NODE_TYPE_FIELD=\"{{ var.value.get('aurum_isone_node_type_field', 'locationType') }}\"",
            "export KAFKA_BOOTSTRAP_SERVERS=\"{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}\"",
            "export SCHEMA_REGISTRY_URL=\"{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}\"",
        ]
    )

    bash_command = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            env_exports,
            "scripts/seatunnel/run_job.sh isone_lmp_to_kafka",
        ]
    )

    isone_ingest = BashOperator(task_id="isone_ws_to_kafka", bash_command=bash_command)

    end = EmptyOperator(task_id="end")

    start >> isone_ingest >> end
