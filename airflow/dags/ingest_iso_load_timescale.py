"""Airflow DAG to load ISO load Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils


DEFAULT_ARGS: dict[str, object] = {
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

SOURCES = (
    iso_utils.IngestSource(
        "iso_load_timescale",
        description="ISO load streaming load into Timescale",
        schedule="0 * * * *",
        target="timescale.public.load_timeseries",
    ),
)


def build_timescale_task(task_id: str) -> BashOperator:
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

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"
    jdbc_url = "{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}"
    topic_pattern = "{{ var.value.get('aurum_iso_load_topic_pattern', 'aurum\\.iso\\..*\\.load\\.v1') }}"
    table_name = "{{ var.value.get('aurum_iso_load_table', 'load_timeseries') }}"
    save_mode = "{{ var.value.get('aurum_iso_load_save_mode', 'upsert') }}"
    area_fallback = "{{ var.value.get('aurum_iso_load_area_fallback', 'SYSTEM') }}"

    env_line = (
        f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
        f"SCHEMA_REGISTRY_URL='{schema_registry}' "
        f"TIMESCALE_JDBC_URL='{jdbc_url}' "
        f"ISO_LOAD_TOPIC_PATTERN='{topic_pattern}' "
        f"ISO_LOAD_TABLE='{table_name}' "
        f"ISO_LOAD_SAVE_MODE='{save_mode}' "
        f"ISO_LOAD_AREA_FALLBACK='{area_fallback}'"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe iso_load_kafka_to_timescale; fi\n"
            f"{env_line} scripts/seatunnel/run_job.sh iso_load_kafka_to_timescale"
        ),
    )


with DAG(
    dag_id="ingest_iso_load_timescale",
    description="Stream ISO load Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "load"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            ),
            optional_variables=(
                "aurum_iso_load_topic_pattern",
                "aurum_iso_load_table",
                "aurum_iso_load_save_mode",
                "aurum_iso_load_area_fallback",
            ),
        ),
    )

    register_source = PythonOperator(
        task_id="register_iso_load_source",
        python_callable=lambda: iso_utils.register_sources(SOURCES),
    )

    load_timescale = build_timescale_task(task_id="iso_load_kafka_to_timescale")

    watermark = PythonOperator(
        task_id="update_iso_load_watermark",
        python_callable=iso_utils.make_watermark_callable("iso_load_timescale"),
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_source >> load_timescale >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_load_timescale")


__all__ = ["dag"]
