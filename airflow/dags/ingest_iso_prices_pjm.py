"""Airflow DAG to ingest PJM LMP via SeaTunnel (Data Miner API)."""
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

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_lines = [
        "export PJM_TOPIC=\"{{ var.value.get('aurum_pjm_topic', 'aurum.iso.pjm.lmp.v1') }}\"",
        # PJM Data Miner expects interval times in EPT (Eastern). Use Airflow TZ conversion.
        "export PJM_INTERVAL_START=\"{{ data_interval_start.in_timezone('America/New_York').isoformat() }}\"",
        "export PJM_INTERVAL_END=\"{{ data_interval_end.in_timezone('America/New_York').isoformat() }}\"",
        "export PJM_ROW_LIMIT=\"{{ var.value.get('aurum_pjm_row_limit', '10000') }}\"",
        "export PJM_MARKET=\"{{ var.value.get('aurum_pjm_market', 'DAY_AHEAD') }}\"",
        "export PJM_LOCATION_TYPE=\"{{ var.value.get('aurum_pjm_location_type', 'NODE') }}\"",
        "export PJM_ENDPOINT=\"{{ var.value.get('aurum_pjm_endpoint', 'https://api.pjm.com/api/v1/da_hrl_lmps') }}\"",
        "export AURUM_KAFKA_BOOTSTRAP_SERVERS=\"" + kafka_bootstrap + "\"",
        "export AURUM_SCHEMA_REGISTRY_URL=\"" + schema_registry + "\"",
        # Optional subject override
        "export PJM_SUBJECT=\"{{ var.value.get('aurum_pjm_subject', 'aurum.iso.pjm.lmp.v1-value') }}\"",
        # Optional registry path for enrichment
        "export ISO_LOCATION_REGISTRY=\"{{ var.value.get('aurum_iso_location_registry', 'config/iso_nodes.csv') }}\"",
    ]

    render_cmd = iso_utils.build_render_command(
        job_name="pjm_lmp_to_kafka",
        env_assignments="AURUM_EXECUTE_SEATUNNEL=0",
        bin_path=BIN_PATH,
        pythonpath_entry=PYTHONPATH_ENTRY,
        debug_dump_env=True,
        pre_lines=[pull_cmd, *env_lines],
    )

    render = BashOperator(
        task_id="pjm_da_render",
        bash_command=render_cmd,
        execution_timeout=timedelta(minutes=10),
        pool="api_pjm",
    )

    exec_k8s = BashOperator(
        task_id="pjm_da_execute_k8s",
        bash_command=iso_utils.build_k8s_command(
            "pjm_lmp_to_kafka",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            timeout=600,
        ),
        execution_timeout=timedelta(minutes=20),
        pool="api_pjm",
    )

    watermark = PythonOperator(
        task_id="pjm_da_watermark",
        python_callable=iso_utils.make_watermark_callable("pjm_da_lmp"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> render >> exec_k8s >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_pjm")

