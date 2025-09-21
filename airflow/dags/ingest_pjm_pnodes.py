"""Airflow DAG to ingest PJM PNODES metadata with rate limiting."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable, metrics


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
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


def _register_sources() -> None:
    # Defer import so the real package is used at runtime inside Airflow workers
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "pjm_pnodes",
            description="PJM PNODES metadata",
            schedule="0 7 * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source pjm_pnodes: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    # Defer import so the real package is used at runtime inside Airflow workers
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "logical_date", logical_date)
        metrics.record_watermark_success(source_name, logical_date)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


with DAG(
    dag_id="ingest_pjm_pnodes",
    description="Ingest PJM PNODES metadata",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "pjm", "metadata"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_pjm_pnodes_endpoint",
            ),
            optional_variables=(
                "aurum_pjm_api_key",
                "aurum_pjm_pnodes_topic",
            ),
        ),
    )

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    env_line = " ".join(
        [
            "PJM_PNODES_ENDPOINT='{{ var.value.get('aurum_pjm_pnodes_endpoint', 'https://api.pjm.com/api/v1/pnodes') }}'",
            "PJM_ROW_LIMIT='{{ var.value.get('aurum_pjm_row_limit', '10000') }}'",
            "PJM_PNODES_TOPIC='{{ var.value.get('aurum_pjm_pnodes_topic', 'aurum.iso.pjm.pnode.v1') }}'",
            "PJM_PNODES_EFFECTIVE_START='{{ ds }}'",
            "AURUM_PJM_API_KEY='{{ var.value.get('aurum_pjm_api_key', '') }}'",
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
        ]
    )

    # Pull PJM API token from Vault if available
    _mapping_flags = "--mapping secret/data/aurum/pjm:token=PJM_API_KEY"
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {_mapping_flags} --format shell)\" || true\n"
    )

    pnodes_task = BashOperator(
        task_id="pjm_pnodes_to_kafka",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"{pull_cmd}"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "export PJM_API_KEY=\"${PJM_API_KEY:-${AURUM_PJM_API_KEY:-}}\"\n"
            # Render-only in Airflow pods without Docker; executed via k8s job
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh pjm_pnodes_to_kafka --render-only"
        ),
        pool="api_pjm",
    )

    exec_k8s = BashOperator(
        task_id="pjm_execute_k8s",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "python scripts/k8s/run_seatunnel_job.py --job-name pjm_pnodes_to_kafka --wait --timeout 600"
        ),
    )

    pnodes_watermark = PythonOperator(
        task_id="pnodes_watermark",
        python_callable=lambda **ctx: _update_watermark("pjm_pnodes", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> pnodes_task >> exec_k8s >> pnodes_watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_pjm_pnodes")
