"""Airflow DAG to load ISO LMP Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
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


VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


def register_stream_source(**context: Any) -> None:
    """Ensure the ISO LMP stream ingest source is registered."""
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "iso_lmp_timescale",
            description="ISO LMP streaming load into Timescale",
            schedule="0 * * * *",
            target="timescale.public.iso_lmp_timeseries",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source iso_lmp_timescale: {exc}")


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
    topic_pattern = "{{ var.value.get('aurum_iso_lmp_topic_pattern', 'aurum\\.iso\\..*\\.lmp\\.v1') }}"
    table_name = "{{ var.value.get('aurum_iso_lmp_table', 'iso_lmp_timeseries') }}"

    env_line = (
        f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
        f"SCHEMA_REGISTRY_URL='{schema_registry}' "
        f"TIMESCALE_JDBC_URL='{jdbc_url}' "
        f"ISO_LMP_TOPIC_PATTERN='{topic_pattern}' "
        f"ISO_LMP_TABLE='{table_name}'"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe iso_lmp_kafka_to_timescale; fi\n"
            f"{env_line} scripts/seatunnel/run_job.sh iso_lmp_kafka_to_timescale"
        ),
    )


with DAG(
    dag_id="ingest_iso_prices_timescale",
    description="Stream ISO LMP Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "lmp"],
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
            optional_variables=("aurum_iso_lmp_topic_pattern", "aurum_iso_lmp_table"),
        ),
    )

    register_source = PythonOperator(task_id="register_iso_lmp_source", python_callable=register_stream_source)

    load_timescale = build_timescale_task(task_id="iso_lmp_kafka_to_timescale")

    def _update_watermark(**context: Any) -> None:
        logical_date: datetime = context["logical_date"]
        watermark = logical_date.astimezone(timezone.utc)
        try:
            import sys
            src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
            if src_path and src_path not in sys.path:
                sys.path.insert(0, src_path)
            from aurum.db import update_ingest_watermark  # type: ignore

            update_ingest_watermark("iso_lmp_timescale", "logical_date", watermark)
            metrics.record_watermark_success("iso_lmp_timescale", watermark)
        except Exception as exc:  # pragma: no cover
            print(f"Failed to update iso_lmp_timescale watermark: {exc}")

    watermark = PythonOperator(task_id="update_iso_lmp_watermark", python_callable=_update_watermark)

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_source >> load_timescale >> watermark >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_prices_timescale")
