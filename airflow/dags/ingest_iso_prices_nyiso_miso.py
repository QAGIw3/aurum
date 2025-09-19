"""Airflow DAG to ingest NYISO and MISO day-ahead feeds via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.db import register_ingest_source, update_ingest_watermark

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
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")


def _register_sources() -> None:
    for name, description in (
        ("nyiso_csv", "NYISO LBMP CSV ingestion"),
        ("miso_csv", "MISO market report CSV ingestion"),
    ):
        try:
            register_ingest_source(
                name,
                description=description,
                schedule="0 * * * *",
                target="kafka",
            )
        except Exception as exc:  # pragma: no cover
            print(f"Failed to register ingest source {name}: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        update_ingest_watermark(source_name, "logical_date", watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


def build_seatunnel_task(job_name: str, required_vars: list[str], env_assignments: list[str]) -> BashOperator:
    mappings = ["secret/data/aurum/kafka:bootstrap=KAFKA_BOOTSTRAP_SERVERS"]
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\""
    )

    env_line = " ".join(env_assignments)

    return BashOperator(
        task_id=f"seatunnel_{job_name}",
        bash_command=(
            "set -euo pipefail\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{env_line} scripts/seatunnel/run_job.sh {job_name}"
        ),
    )


with DAG(
    dag_id="ingest_iso_prices_nyiso_miso",
    description="Ingest NYISO and MISO day-ahead/real-time LMP feeds into Kafka",
    default_args=DEFAULT_ARGS,
    schedule_interval="15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "iso", "nyiso", "miso"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    nyiso_task = build_seatunnel_task(
        "nyiso_lmp_to_kafka",
        required_vars=[],
        env_assignments=[
            "NYISO_URL='{{ var.value.get('aurum_nyiso_url') }}'",
            "NYISO_TOPIC='{{ var.value.get('aurum_nyiso_topic', 'aurum.iso.nyiso.lmp.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
    )

    miso_da = build_seatunnel_task(
        "miso_lmp_to_kafka",
        required_vars=[],
        env_assignments=[
            "MISO_URL='{{ var.value.get('aurum_miso_da_url') }}'",
            "MISO_MARKET='DAY_AHEAD'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
    )

    miso_rt = build_seatunnel_task(
        "miso_lmp_to_kafka",
        required_vars=[],
        env_assignments=[
            "MISO_URL='{{ var.value.get('aurum_miso_rt_url') }}'",
            "MISO_MARKET='REAL_TIME'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
    )

    nyiso_watermark = PythonOperator(
        task_id="nyiso_watermark",
        python_callable=lambda **ctx: _update_watermark("nyiso_csv", ctx["logical_date"]),
    )

    miso_da_watermark = PythonOperator(
        task_id="miso_da_watermark",
        python_callable=lambda **ctx: _update_watermark("miso_csv", ctx["logical_date"]),
    )

    miso_rt_watermark = PythonOperator(
        task_id="miso_rt_watermark",
        python_callable=lambda **ctx: _update_watermark("miso_csv", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources
    register_sources >> nyiso_task >> nyiso_watermark
    register_sources >> miso_da >> miso_da_watermark
    register_sources >> miso_rt >> miso_rt_watermark
    [nyiso_watermark, miso_da_watermark, miso_rt_watermark] >> end
