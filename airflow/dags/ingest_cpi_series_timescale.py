"""Airflow DAG to load CPI series Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from aurum.airflow_utils import build_preflight_callable


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


def register_stream_source(**context: Any) -> None:
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            "cpi_series_timescale",
            description="CPI series streaming load into Timescale",
            schedule="0 * * * *",
            target="timescale.public.cpi_series_timeseries",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source cpi_series_timescale: {exc}")



def emit_lakefs_lineage(dataset: str, **context: Any) -> None:
    repo = os.environ.get("AURUM_LAKEFS_REPO")
    if not repo:
        print("LakeFS repo not configured; skipping lineage commit")
        return

    branch = os.environ.get("AURUM_LAKEFS_BRANCH", "main")
    run_id = context.get("run_id", "unknown")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    dag_run = context.get("dag_run")
    backfill_flag = dag_run.conf.get("backfill", False) if dag_run else False
    logical_date = context.get("logical_date")

    metadata = {
        "dataset": dataset,
        "dag_id": dag_id,
        "run_id": run_id,
        "backfill": str(bool(backfill_flag)).lower(),
    }
    if logical_date:
        metadata["logical_date"] = logical_date.astimezone(timezone.utc).isoformat()

    try:
        import sys

        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)

        from aurum.lakefs_client import commit_branch, ensure_branch  # type: ignore

        ensure_branch(repo, branch)
        commit_branch(repo, branch, f"airflow:{dag_id}:{run_id}", metadata)
    except Exception as exc:  # pragma: no cover
        print(f"LakeFS lineage commit failed: {exc}")


from aurum.airflow_utils.timescale import build_timescale_task as _build_ts


with DAG(
    dag_id="ingest_cpi_series_timescale",
    description="Stream CPI series Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "cpi"],
) as dag:
    start = EmptyOperator(task_id="start")
    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=build_preflight_callable(
            required_variables=(
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            )
        ),
    )
    register_source = PythonOperator(task_id="register_cpi_series_source", python_callable=register_stream_source)
    load_timescale = _build_ts(
        task_id="cpi_series_kafka_to_timescale",
        job_name="cpi_series_kafka_to_timescale",
        env_entries=[
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get(\"aurum_kafka_bootstrap\", \"localhost:9092\") }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get(\"aurum_schema_registry\", \"http://localhost:8081\") }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get(\"aurum_timescale_jdbc\", \"jdbc:postgresql://timescale:5432/timeseries\") }}'",
            "CPI_TOPIC_PATTERN='{{ var.value.get(\"aurum_cpi_topic_pattern\", \"aurum\\.ref\\.cpi\\..*\\.v1\") }}'",
            "CPI_SERIES_TABLE='{{ var.value.get(\"aurum_cpi_series_table\", \"cpi_series_timeseries\") }}'",
            "DLQ_TOPIC='{{ var.value.get(\"aurum_cpi_dlq_topic\", \"aurum.ref.cpi.series.dlq.v1\") }}'",
            "BACKFILL_ENABLED='{{ dag_run.conf.get(\"backfill\", \"0\") }}'",
            "BACKFILL_START='{{ dag_run.conf.get(\"backfill_start\", \"\") }}'",
            "BACKFILL_END='{{ dag_run.conf.get(\"backfill_end\", \"\") }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
    )

    def _update_watermark(**context: Any) -> None:
        logical_date: datetime = context["logical_date"]
        watermark = logical_date.astimezone(timezone.utc)
        try:
            import sys
            src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
            if src_path and src_path not in sys.path:
                sys.path.insert(0, src_path)
            from aurum.db import update_ingest_watermark  # type: ignore

            update_ingest_watermark("cpi_series_timescale", "logical_date", watermark)
        except Exception as exc:  # pragma: no cover
            print(f"Failed to update cpi_series_timescale watermark: {exc}")

    watermark = PythonOperator(task_id="update_cpi_series_watermark", python_callable=_update_watermark)

    end = EmptyOperator(task_id="end")

    lakefs_commit = PythonOperator(
        task_id="lakefs_lineage_commit",
        python_callable=emit_lakefs_lineage,
        op_kwargs={"dataset": "timescale.public.cpi_series_timeseries"},
    )

    start >> preflight >> register_source >> load_timescale >> watermark >> lakefs_commit >> end
