"""Airflow DAG to ingest ISO-NE and SPP feeds via SeaTunnel."""
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

ISO_SOURCES = {
    "isone_ws": "ISO-NE web services ingestion",
    "spp_file": "SPP market file ingestion",
}


def _register_sources() -> None:
    for name, description in ISO_SOURCES.items():
        try:
            register_ingest_source(
                name,
                description=description,
                schedule="20 * * * *",
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


def build_seatunnel_task(job_name: str, env_assignments: list[str], mappings: list[str]) -> BashOperator:
    mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\"\n"
    )

    env_line = " ".join(env_assignments)

    return BashOperator(
        task_id=f"seatunnel_{job_name}",
        bash_command=(
            "set -euo pipefail\n"
            f"{pull_cmd}"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{env_line} scripts/seatunnel/run_job.sh {job_name}"
        ),
    )


with DAG(
    dag_id="ingest_iso_prices_isone_spp",
    description="Ingest ISO-NE and SPP feeds via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="20 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["aurum", "isone", "spp", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    isone_task = build_seatunnel_task(
        "isone_lmp_to_kafka",
        [
            "ISONE_URL='{{ var.value.get('aurum_isone_endpoint') }}'",
            "ISONE_START='{{ data_interval_start.in_timezone('UTC').isoformat() }}'",
            "ISONE_END='{{ data_interval_end.in_timezone('UTC').isoformat() }}'",
            "ISONE_MARKET='{{ var.value.get('aurum_isone_market', 'DA') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=[
            "secret/data/aurum/isone:username=ISONE_USERNAME",
            "secret/data/aurum/isone:password=ISONE_PASSWORD",
        ],
    )

    # SPP: stage JSON via Python helper, then publish via SeaTunnel LocalFile source
    spp_staging = os.environ.get("AURUM_STAGING_DIR", "files/staging") + "/spp/{{ ds }}.json"

    stage_spp_cmd = "\n".join(
        [
            "set -euo pipefail",
            "cd /opt/airflow",
            "mkdir -p $(dirname '" + spp_staging + "')",
            f"export PATH=\"{BIN_PATH}\"",
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"",
            (
                "python scripts/ingest/spp_file_api_to_kafka.py "
                "--base-url {{ var.value.get('aurum_spp_base_url') }} "
                "--report {{ var.value.get('aurum_spp_rt_report') }} "
                "--date {{ ds }} "
                f"--output-json {spp_staging} --no-kafka"
            ),
        ]
    )
    stage_spp = BashOperator(task_id="stage_spp_lmp", bash_command=stage_spp_cmd)

    spp_task = build_seatunnel_task(
        "spp_lmp_to_kafka",
        [
            f"SPP_INPUT_JSON={spp_staging}",
            "SPP_TOPIC='{{ var.value.get('aurum_spp_topic', 'aurum.iso.spp.lmp.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'",
        ],
        mappings=[],
    )

    isone_watermark = PythonOperator(
        task_id="isone_watermark",
        python_callable=lambda **ctx: _update_watermark("isone_ws", ctx["logical_date"]),
    )

    spp_watermark = PythonOperator(
        task_id="spp_watermark",
        python_callable=lambda **ctx: _update_watermark("spp_file", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources
    register_sources >> isone_task >> isone_watermark
    register_sources >> stage_spp >> spp_task >> spp_watermark
    [isone_watermark, spp_watermark] >> end
