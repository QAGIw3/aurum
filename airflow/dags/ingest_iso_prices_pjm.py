"""Airflow DAG to ingest PJM LMP via SeaTunnel (Data Miner API)."""
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
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")


SOURCES = (
    iso_utils.IngestSource(
        "pjm_da_lmp",
        description="PJM day-ahead LMP ingestion",
        schedule="25 * * * *",
        target="kafka",
    ),
)


with DAG(
    dag_id="ingest_iso_prices_pjm",
    description="Download PJM day-ahead LMP and publish via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="25 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "pjm", "lmp"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
            ),
            optional_variables=(
                "aurum_pjm_topic",
                "aurum_pjm_endpoint",
                "aurum_pjm_row_limit",
            ),
        ),
    )

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    # Pull PJM API token from Vault into PJM_API_KEY (best effort)
    mapping_flags = "--mapping secret/data/aurum/pjm:token=PJM_API_KEY"
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )

    env_entries = [
        "PJM_TOPIC=\"{{ var.value.get('aurum_pjm_topic', 'aurum.iso.pjm.lmp.v1') }}\"",
        # EPT (Eastern) time window for PJM Data Miner
        "PJM_INTERVAL_START=\"{{ data_interval_start.in_timezone('America/New_York').isoformat() }}\"",
        "PJM_INTERVAL_END=\"{{ data_interval_end.in_timezone('America/New_York').isoformat() }}\"",
        "PJM_ROW_LIMIT=\"{{ var.value.get('aurum_pjm_row_limit', '10000') }}\"",
        "PJM_MARKET=\"{{ var.value.get('aurum_pjm_market', 'DAY_AHEAD') }}\"",
        "PJM_LOCATION_TYPE=\"{{ var.value.get('aurum_pjm_location_type', 'NODE') }}\"",
        "PJM_ENDPOINT=\"{{ var.value.get('aurum_pjm_endpoint', 'https://api.pjm.com/api/v1/da_hrl_lmps') }}\"",
        # Optional subject override and registry enrichment
        "PJM_SUBJECT=\"{{ var.value.get('aurum_pjm_subject', 'aurum.iso.pjm.lmp.v1-value') }}\"",
        "ISO_LOCATION_REGISTRY=\"{{ var.value.get('aurum_iso_location_registry', 'config/iso_nodes.csv') }}\"",
        # Duplicate AURUM_* vars if templates expect them
        "AURUM_KAFKA_BOOTSTRAP_SERVERS=\"{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}\"",
        "AURUM_SCHEMA_REGISTRY_URL=\"{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}\"",
    ]

    render, exec_k8s, watermark = iso_utils.create_seatunnel_ingest_chain(
        "pjm_da",
        job_name="pjm_lmp_to_kafka",
        source_name="pjm_da_lmp",
        env_entries=env_entries,
        pool="api_pjm",
        pre_lines=[pull_cmd],
        watermark_policy="hour",
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> render >> exec_k8s >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_pjm")
