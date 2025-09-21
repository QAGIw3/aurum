"""Airflow DAG to ingest CAISO load and generation mix via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Tuple

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
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(minutes=30),
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
            "caiso_load",
            description="CAISO load ingestion",
            schedule="30 * * * *",
            target="kafka",
        )
        register_ingest_source(
            "caiso_genmix",
            description="CAISO generation mix ingestion",
            schedule="30 * * * *",
            target="kafka",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register CAISO ingest sources: {exc}")


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
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"if [ \"${{AURUM_DEBUG:-0}}\" != \"0\" ]; then scripts/seatunnel/run_job.sh --describe {job_name}; fi\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"AURUM_EXECUTE_SEATUNNEL=0 {env_line} scripts/seatunnel/run_job.sh {job_name} --render-only"
        ),
        execution_timeout=timedelta(minutes=10),
        pool="api_caiso",
    )

    exec_job = BashOperator(
        task_id=f"{task_prefix}_execute",
        bash_command=(
            "set -euo pipefail\n"
            "if [ \"${AURUM_DEBUG:-0}\" != \"0\" ]; then set -x; fi\n"
            "cd /opt/airflow\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"python scripts/k8s/run_seatunnel_job.py --job-name {job_name} --wait --timeout 600"
        ),
        execution_timeout=timedelta(minutes=20),
        pool="api_caiso",
    )

    watermark = PythonOperator(
        task_id=f"{task_prefix}_watermark",
        python_callable=lambda **ctx: _update_watermark(source_name, ctx["logical_date"]),
    )

    return render, exec_job, watermark


with DAG(
    dag_id="ingest_iso_metrics_caiso",
    description="Ingest CAISO load and generation mix observations via SeaTunnel",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "caiso", "load", "genmix"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_caiso_load_endpoint",
                "aurum_caiso_genmix_endpoint",
            ),
            optional_variables=(
                "aurum_caiso_token",
                "aurum_caiso_load_topic",
                "aurum_caiso_genmix_topic",
            ),
        ),
    )

    register_sources = PythonOperator(task_id="register_sources", python_callable=_register_sources)

    token = "{{ var.value.get(\"aurum_caiso_token\", \"\") }}"

    load_env = [
        "CAISO_LOAD_ENDPOINT=\"{{ var.value.get(\"aurum_caiso_load_endpoint\") }}\"",
        "CAISO_LOAD_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_LOAD_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_LOAD_TOPIC=\"{{ var.value.get(\"aurum_caiso_load_topic\", \"aurum.iso.caiso.load.v1\") }}\"",
        f"CAISO_LOAD_AUTH_HEADER=\"Bearer {token}\"",
    ]

    load_render, load_exec, load_watermark = _build_job(
        "caiso_load",
        "caiso_load_to_kafka",
        "caiso_load",
        env_entries=load_env,
    )

    genmix_env = [
        "CAISO_GENMIX_ENDPOINT=\"{{ var.value.get(\"aurum_caiso_genmix_endpoint\") }}\"",
        "CAISO_GENMIX_INTERVAL_START=\"{{ data_interval_start.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_GENMIX_INTERVAL_END=\"{{ data_interval_end.in_timezone(\"UTC\").isoformat() }}\"",
        "CAISO_GENMIX_TOPIC=\"{{ var.value.get(\"aurum_caiso_genmix_topic\", \"aurum.iso.caiso.genmix.v1\") }}\"",
        f"CAISO_GENMIX_AUTH_HEADER=\"Bearer {token}\"",
    ]

    genmix_render, genmix_exec, genmix_watermark = _build_job(
        "caiso_genmix",
        "caiso_genmix_to_kafka",
        "caiso_genmix",
        env_entries=genmix_env,
    )

    end = EmptyOperator(task_id="end")

    start >> preflight >> register_sources
    register_sources >> load_render >> load_exec >> load_watermark
    register_sources >> genmix_render >> genmix_exec >> genmix_watermark
    [load_watermark, genmix_watermark] >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.ingest_iso_metrics_caiso")
