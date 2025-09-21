"""Airflow DAG to ingest ISO-NE web services LMP into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

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
        "isone_ws_lmp",
        description="ISO-NE web services LMP ingestion",
        schedule="10 * * * *",
        target="kafka",
    ),
)


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

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
            optional_variables=(
                "aurum_isone_ws_username",
                "aurum_isone_ws_password",
                "aurum_isone_topic",
            ),
            warn_only_variables=("aurum_isone_timezone",),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    # Best-effort pull of ISO-NE WS creds from Vault
    pull_cmd = (
        "eval \"$(VAULT_ADDR=${AURUM_VAULT_ADDR:-http://127.0.0.1:8200} "
        "VAULT_TOKEN=${AURUM_VAULT_TOKEN:-aurum-dev-token} "
        "PYTHONPATH=${PYTHONPATH:-}:" + PYTHONPATH_ENTRY + " "
        + os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python") +
        " scripts/secrets/pull_vault_env.py --mapping secret/data/aurum/isone:username=ISONE_USERNAME --mapping secret/data/aurum/isone:password=ISONE_PASSWORD --format shell)\" || true"
    )

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
            pull_cmd,
            env_exports,
            # Prefer Python helper in-cluster; avoid depending on docker in seatunnel runner
            "python scripts/ingest/isone_ws_to_kafka.py "
            "--url \"${ISONE_URL}\" --start \"${ISONE_START}\" --end \"${ISONE_END}\" "
            "--market \"${ISONE_MARKET}\" --topic \"${ISONE_TOPIC}\" "
            "--schema-registry \"${SCHEMA_REGISTRY_URL}\" --bootstrap-servers \"${KAFKA_BOOTSTRAP_SERVERS}\" "
            "${ISONE_USERNAME:+--username \"${ISONE_USERNAME}\"} ${ISONE_PASSWORD:+--password \"${ISONE_PASSWORD}\"}",
        ]
    )

    isone_ingest = BashOperator(
        task_id="isone_ws_to_kafka",
        bash_command=bash_command,
        execution_timeout=timedelta(minutes=20),
        pool="api_isone",
    )

    watermark = PythonOperator(
        task_id="isone_watermark",
        python_callable=iso_utils.make_watermark_callable("isone_ws_lmp"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> isone_ingest >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_isone")
