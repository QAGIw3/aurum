"""Airflow DAG to load FRED series Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

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
            "fred_series_timescale",
            description="FRED series streaming load into Timescale",
            schedule="0 * * * *",
            target="timescale.public.fred_series_timeseries",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source fred_series_timescale: {exc}")


def preflight_required_vars(keys: list[str]) -> None:
    try:
        from airflow.models import Variable  # type: ignore
    except Exception as exc:  # pragma: no cover
        print(f"Airflow Variable API unavailable: {exc}")
        return
    missing: list[str] = []
    for key in keys:
        try:
            Variable.get(key)
        except Exception:
            missing.append(key)
    if missing:
        critical = {"aurum_kafka_bootstrap", "aurum_schema_registry", "aurum_timescale_jdbc"}
        if any(k in critical for k in missing):
            raise RuntimeError(f"Missing required Airflow Variables: {missing}")
        print(f"Warning: missing optional Airflow Variables: {missing}")


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


def build_timescale_task(task_id: str) -> BashOperator:
    mappings = [
        "secret/data/aurum/timescale:user=TIMESCALE_USER",
        "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
    ]
    mapping_flags = " ".join(f"--mapping {m}" for m in mappings)
    pull_cmd = (
        f"eval \"$(VAULT_ADDR={VAULT_ADDR} VAULT_TOKEN={VAULT_TOKEN} "
        f"PYTHONPATH=${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY} "
        f"{VENV_PYTHON} scripts/secrets/pull_vault_env.py {mapping_flags} --format shell)\" || true"
    )

    kafka_bootstrap = "{{ var.value.get('aurum_kafka_bootstrap', 'localhost:9092') }}"
    schema_registry = "{{ var.value.get('aurum_schema_registry', 'http://localhost:8081') }}"
    jdbc_url = "{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale:5432/timeseries') }}"
    topic_pattern = "{{ var.value.get('aurum_fred_topic_pattern', 'aurum\\.ref\\.fred\\..*\\.v1') }}"
    table_name = "{{ var.value.get('aurum_fred_series_table', 'fred_series_timeseries') }}"
    dlq_topic = "{{ var.value.get('aurum_fred_dlq_topic', 'aurum.ref.fred.series.dlq.v1') }}"
    backfill_flag = "{{ dag_run.conf.get('backfill', '0') }}"
    backfill_start = "{{ dag_run.conf.get('backfill_start', '') }}"
    backfill_end = "{{ dag_run.conf.get('backfill_end', '') }}"

    env_line = (
        f"KAFKA_BOOTSTRAP_SERVERS='{kafka_bootstrap}' "
        f"SCHEMA_REGISTRY_URL='{schema_registry}' "
        f"TIMESCALE_JDBC_URL='{jdbc_url}' "
        f"FRED_TOPIC_PATTERN='{topic_pattern}' "
        f"FRED_SERIES_TABLE='{table_name}' "
        f"DLQ_TOPIC='{dlq_topic}' "
        f"BACKFILL_ENABLED='{backfill_flag}' "
        f"BACKFILL_START='{backfill_start}' "
        f"BACKFILL_END='{backfill_end}'"
    )

    return BashOperator(
        task_id=task_id,
        bash_command=(
            "set -euo pipefail\n"
            f"{pull_cmd}\n"
            f"export PATH=\"{BIN_PATH}\"\n"
            f"export PYTHONPATH=\"${{PYTHONPATH:-}}:{PYTHONPATH_ENTRY}\"\n"
            f"{env_line} scripts/seatunnel/run_job.sh fred_series_kafka_to_timescale"
        ),
    )


with DAG(
    dag_id="ingest_fred_series_timescale",
    description="Stream FRED series Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "fred"],
) as dag:
    start = EmptyOperator(task_id="start")
    preflight = PythonOperator(
        task_id="preflight_airflow_vars",
        python_callable=lambda **_: preflight_required_vars(
            [
                "aurum_kafka_bootstrap",
                "aurum_schema_registry",
                "aurum_timescale_jdbc",
            ]
        ),
    )
    register_source = PythonOperator(task_id="register_fred_series_source", python_callable=register_stream_source)
    load_timescale = build_timescale_task(task_id="fred_series_kafka_to_timescale")

    def _update_watermark(**context: Any) -> None:
        logical_date: datetime = context["logical_date"]
        watermark = logical_date.astimezone(timezone.utc)
        try:
            import sys
            src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
            if src_path and src_path not in sys.path:
                sys.path.insert(0, src_path)
            from aurum.db import update_ingest_watermark  # type: ignore

            update_ingest_watermark("fred_series_timescale", "logical_date", watermark)
        except Exception as exc:  # pragma: no cover
            print(f"Failed to update fred_series_timescale watermark: {exc}")

    watermark = PythonOperator(task_id="update_fred_series_watermark", python_callable=_update_watermark)

    end = EmptyOperator(task_id="end")

    lakefs_commit = PythonOperator(
        task_id="lakefs_lineage_commit",
        python_callable=emit_lakefs_lineage,
        op_kwargs={"dataset": "timescale.public.fred_series_timeseries"},
    )

    start >> preflight >> register_source >> load_timescale >> watermark >> lakefs_commit >> end
