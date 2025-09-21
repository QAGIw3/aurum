"""Ingest AESO SMP (system price) into Kafka via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
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
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
AESO_SOURCES = (
    iso_utils.IngestSource(
        "aeso_smp",
        description="AESO system marginal price (SMP)",
        schedule="*/5 * * * *",
        target="kafka",
    ),
)


def _register_sources() -> None:
    iso_utils.register_sources(AESO_SOURCES)


def build_seatunnel_task() -> BashOperator:
    mapping_flags = " --mapping secret/data/aurum/aeso:token=AESO_API_KEY"
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH={PYTHONPATH_ENTRY}:${{PYTHONPATH:-}} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py{mapping_flags} --format shell)\" || true\n"
    )

    env_parts = [
        "AESO_ENDPOINT='{{ var.value.get('aurum_aeso_endpoint', 'https://api.aeso.ca/report/v1/price/systemMarginalPrice') }}'",
        "AESO_START='{{ data_interval_start.in_timezone('UTC').isoformat() }}'",
        "AESO_END='{{ data_interval_end.in_timezone('UTC').isoformat() }}'",
        "AESO_TOPIC='{{ var.value.get('aurum_aeso_topic', 'aurum.iso.aeso.lmp.v1') }}'",
        "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get('aurum_kafka_bootstrap', 'kafka:9092') }}'",
        "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
    ]
    env_line = " ".join(env_parts)

    seatunnel_render = BashOperator(
        task_id="seatunnel_aeso_lmp_to_kafka",
        bash_command=iso_utils.build_render_command(
            job_name="aeso_lmp_to_kafka",
            env_assignments=f"AURUM_EXECUTE_SEATUNNEL=0 {env_line}",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            debug_dump_env=True,
            pre_lines=[pull_cmd.rstrip()],
            extra_lines=["export ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc"],
        ),
        execution_timeout=timedelta(minutes=10),
        pool="api_aeso",
    )
    seatunnel_exec = BashOperator(
        task_id="aeso_execute_k8s",
        bash_command=iso_utils.build_k8s_command(
            "aeso_lmp_to_kafka",
            bin_path=BIN_PATH,
            pythonpath_entry=PYTHONPATH_ENTRY,
            timeout=600,
            extra_lines=["export ISO_LMP_SCHEMA_PATH=/opt/airflow/scripts/kafka/schemas/iso.lmp.v1.avsc"],
        ),
        execution_timeout=timedelta(minutes=20),
    )
    return seatunnel_render, seatunnel_exec


with DAG(
    dag_id="ingest_iso_prices_aeso",
    description="Ingest AESO SMP (system price) via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "aeso", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_aeso_smp_endpoint",
            ),
            optional_variables=("aurum_aeso_api_key", "aurum_aeso_smp_topic"),
        ),
    )

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    aeso_render, aeso_exec = build_seatunnel_task()

    aeso_watermark = PythonOperator(
        task_id="aeso_watermark",
        python_callable=iso_utils.make_watermark_callable("aeso_smp"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources >> aeso_render >> aeso_exec >> aeso_watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_aeso")
