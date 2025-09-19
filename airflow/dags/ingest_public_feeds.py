"""Airflow DAG to ingest public reference feeds (NOAA, EIA, FRED, PJM)."""
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


PUBLIC_SOURCES = {
    "noaa_ghcnd": "NOAA GHCND daily ingestion",
    "eia_series": "EIA v2 series ingestion",
    "fred_series": "FRED series ingestion",
    "pjm_lmp": "PJM day-ahead LMP ingestion",
}


def _register_sources() -> None:
    for name, description in PUBLIC_SOURCES.items():
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


def build_seatunnel_task(job_name: str, env_assignments: list[str], mappings: list[str] | None = None) -> BashOperator:
    mapping_flags = ""
    if mappings:
        mapping_flags = " ".join(f"--mapping {mapping}" for mapping in mappings)
    pull_cmd = ""
    if mapping_flags:
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
    dag_id="ingest_public_feeds",
    description="Ingest NOAA, EIA, FRED, and PJM public data feeds",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "public", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=_register_sources,
    )

    noaa_task = build_seatunnel_task(
        "noaa_ghcnd_to_kafka",
        [
            "NOAA_GHCND_START_DATE='{{ ds }}'",
            "NOAA_GHCND_END_DATE='{{ ds }}'",
            "NOAA_GHCND_TOPIC='{{ var.value.get('aurum_noaa_topic', 'aurum.ref.noaa.weather.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN"],
    )

    eia_task = build_seatunnel_task(
        "eia_series_to_kafka",
        [
            "EIA_SERIES_PATH='{{ var.value.get('aurum_eia_series_path', 'electricity/wholesale/prices/data') }}'",
            "EIA_SERIES_ID='{{ var.value.get('aurum_eia_series_id', 'EBA.ALL.D.H') }}'",
            "EIA_FREQUENCY='{{ var.value.get('aurum_eia_frequency', 'HOURLY') }}'",
            "EIA_TOPIC='{{ var.value.get('aurum_eia_topic', 'aurum.ref.eia.series.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/eia:api_key=EIA_API_KEY"],
    )

    fred_task = build_seatunnel_task(
        "fred_series_to_kafka",
        [
            "FRED_SERIES_ID='{{ var.value.get('aurum_fred_series_id', 'DGS10') }}'",
            "FRED_FREQUENCY='{{ var.value.get('aurum_fred_frequency', 'DAILY') }}'",
            "FRED_SEASONAL_ADJ='{{ var.value.get('aurum_fred_seasonal_adj', 'NSA') }}'",
            "FRED_TOPIC='{{ var.value.get('aurum_fred_topic', 'aurum.ref.fred.series.v1') }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/fred:api_key=FRED_API_KEY"],
    )

    pjm_task = build_seatunnel_task(
        "pjm_lmp_to_kafka",
        [
            "PJM_TOPIC='{{ var.value.get('aurum_pjm_topic', 'aurum.iso.pjm.lmp.v1') }}'",
            "PJM_INTERVAL_START='{{ data_interval_start.in_timezone('America/New_York').isoformat() }}'",
            "PJM_INTERVAL_END='{{ data_interval_end.in_timezone('America/New_York').isoformat() }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}'"
        ],
        mappings=["secret/data/aurum/pjm:token=PJM_API_KEY"],
    )

    noaa_watermark = PythonOperator(
        task_id="noaa_watermark",
        python_callable=lambda **ctx: _update_watermark("noaa_ghcnd", ctx["logical_date"]),
    )

    eia_watermark = PythonOperator(
        task_id="eia_watermark",
        python_callable=lambda **ctx: _update_watermark("eia_series", ctx["logical_date"]),
    )

    fred_watermark = PythonOperator(
        task_id="fred_watermark",
        python_callable=lambda **ctx: _update_watermark("fred_series", ctx["logical_date"]),
    )

    pjm_watermark = PythonOperator(
        task_id="pjm_watermark",
        python_callable=lambda **ctx: _update_watermark("pjm_lmp", ctx["logical_date"]),
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources
    register_sources >> noaa_task >> noaa_watermark
    register_sources >> eia_task >> eia_watermark
    register_sources >> fred_task >> fred_watermark
    register_sources >> pjm_task >> pjm_watermark
    [noaa_watermark, eia_watermark, fred_watermark, pjm_watermark] >> end
