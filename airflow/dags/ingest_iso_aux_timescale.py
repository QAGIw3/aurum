"""Airflow DAG to load ISO ASM and PNODE Kafka topics into TimescaleDB via SeaTunnel."""
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

VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


def _build_job_task(task_id: str, job_name: str, env_line: str) -> BashOperator:
    mappings = [
        "secret/data/aurum/timescale:user=TIMESCALE_USER",
        "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
    ]
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{env_line} scripts/seatunnel/run_job.sh {job_name}"
        ),
    )


with DAG(
    dag_id="ingest_iso_aux_timescale",
    description="Stream ISO ASM and PNODE Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            ),
            optional_variables=(
                "aurum_iso_asm_topic_pattern",
                "aurum_iso_pnode_topic_pattern",
                "aurum_iso_asm_table",
                "aurum_iso_pnode_table",
            ),
        ),
    )

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"
    jdbc_url = "{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}"
    asm_topic_pattern = "{{ var.value.get('aurum_iso_asm_topic_pattern', 'aurum\\.iso\\..*\\.asm\\.v1') }}"
    pnode_topic_pattern = "{{ var.value.get('aurum_iso_pnode_topic_pattern', 'aurum\\.iso\\..*\\.pnode\\.v1') }}"
    asm_table = "{{ var.value.get('aurum_iso_asm_table', 'iso_asm_timeseries') }}"
    pnode_table = "{{ var.value.get('aurum_iso_pnode_table', 'iso_pnode_registry') }}"

    common_env = (
        f"AURUM_KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
        f"AURUM_SCHEMA_REGISTRY_URL='{schema_registry}' "
        f"AURUM_TIMESCALE_JDBC_URL='{jdbc_url}' "
    )

    t_asm = _build_job_task(
        task_id="iso_asm_kafka_to_timescale",
        job_name="iso_asm_kafka_to_timescale",
        env_line=(
            f"{common_env} ISO_ASM_TOPIC_PATTERN='{asm_topic_pattern}' "
            f"ISO_ASM_TABLE='{asm_table}'"
        ),
    )

    t_pnode = _build_job_task(
        task_id="iso_pnode_kafka_to_timescale",
        job_name="iso_pnode_kafka_to_timescale",
        env_line=(
            f"{common_env} ISO_PNODE_TOPIC_PATTERN='{pnode_topic_pattern}' "
            f"ISO_PNODE_TABLE='{pnode_table}'"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> [t_asm, t_pnode] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_aux_timescale")

