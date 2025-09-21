"""Airflow DAG to ingest AESO load and generation mix via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Tuple

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


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


def _register_sources() -> None:
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "aeso_load",
            description="AESO load ingestion",
            schedule="*/10 * * * *",
            target="kafka",
        )
        register_ingest_source(
            "aeso_genmix",
            description="AESO generation mix ingestion",
            schedule="*/10 * * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register AESO ingest sources: {exc}")


def _update_watermark(source_name: str, logical_date: datetime) -> None:
    watermark = logical_date.astimezone(timezone.utc)
    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "logical_date", watermark)
    except Exception as exc:  # pragma: no cover
        print(f"Failed to update watermark for {source_name}: {exc}")


def _build_job(
    task_prefix: str,
    job_name: str,
    source_name: str,
    *,
    env_entries: Iterable[str],
) -> Tuple[BashOperator, BashOperator, PythonOperator]:
    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"

    env_line = " ".join(
        list(env_entries)
        + [
            f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}'",
            f"SCHEMA_REGISTRY_URL='{schema_registry}'",
        ]
    )

    render = BashOperator(
        task_id=f"{task_prefix}_render",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
    )

    exec_job = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=(
            "set -euo pipefail\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"python scripts/k8s/run_seatunnel_job.py --job-name {job_name} --wait --timeout 600"
        ),
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=lambda **ctx: _update_watermark(source_name, ctx["logical_date"]),
    )

    return render, exec_job, watermark


with DAG(
    dag_id="ingest_iso_metrics_aeso",
    description="Ingest AESO load and generation mix observations via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/10 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "aeso", "load", "genmix"],
) as dag:
    start = EmptyOperator(task_id="start")

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    api_key = "{{ var.value.get(\"aurum_aeso_api_key\", \"\") }}"

    load_env = [
        "AESO_LOAD_ENDPOINT=\"{{ var.value.get(\"aurum_aeso_load_endpoint\") }}\"",
        "AESO_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "AESO_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "AESO_LOAD_TOPIC=\"{{ var.value.get(\"aurum_aeso_load_topic\", \"aurum.iso.aeso.load.v1\") }}\"",
        f"AESO_LOAD_API_KEY=\"{api_key}\"",
    ]

    load_render, load_exec, load_watermark = _build_job(
        "aeso_load",
        "aeso_load_to_kafka",
        "aeso_load",
        env_entries=load_env,
    )

    genmix_env = [
        "AESO_GENMIX_ENDPOINT=\"{{ var.value.get(\"aurum_aeso_genmix_endpoint\") }}\"",
        "AESO_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "AESO_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "AESO_GENMIX_TOPIC=\"{{ var.value.get(\"aurum_aeso_genmix_topic\", \"aurum.iso.aeso.genmix.v1\") }}\"",
        f"AESO_GENMIX_API_KEY=\"{api_key}\"",
    ]

    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "aeso_genmix",
        "aeso_genmix_to_kafka",
        "aeso_genmix",
        env_entries=genmix_env,
    )

    end = EmptyOperator(task_id="end")

    start >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end
