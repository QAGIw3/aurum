"""Airflow DAG to orchestrate incremental backfills using aurum.scripts.ingest.backfill."""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 0,
}


def _parse_bool(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "on"}


def run_backfill(**context: Any) -> None:
    execution_ds = context.get("ds")
    execution_date = date.fromisoformat(execution_ds) if execution_ds else date.today()

    source = os.environ.get("AURUM_BACKFILL_SOURCE")
    if not source:
        raise RuntimeError("AURUM_BACKFILL_SOURCE is required for the backfill DAG")

    watermark_key = os.environ.get("AURUM_BACKFILL_WATERMARK_KEY", "default")
    days = max(1, int(os.environ.get("AURUM_BACKFILL_DAYS", "7") or 7))
    history_limit = max(days, int(os.environ.get("AURUM_BACKFILL_HISTORY_LIMIT", "30") or 30))

    start_date = execution_date - timedelta(days=days - 1)
    oldest_allowed = execution_date - timedelta(days=history_limit - 1)
    if start_date < oldest_allowed:
        start_date = oldest_allowed

    command_template = os.environ.get("AURUM_BACKFILL_COMMAND_TEMPLATE")
    use_shell = _parse_bool(os.environ.get("AURUM_BACKFILL_COMMAND_SHELL"))
    dry_run = _parse_bool(os.environ.get("AURUM_BACKFILL_DRY_RUN"))

    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.scripts.ingest import backfill as backfill_cmd  # type: ignore

        argv: list[str] = [
            "--source",
            source,
            "--start",
            start_date.isoformat(),
            "--end",
            execution_date.isoformat(),
            "--key",
            watermark_key,
        ]
        if command_template:
            argv.extend(["--command", command_template])
            if use_shell:
                argv.append("--shell")
        if dry_run:
            argv.append("--dry-run")
        backfill_cmd.main(argv)
    except Exception as exc:  # pragma: no cover - propagate failure to Airflow
        raise RuntimeError(f"Backfill execution failed: {exc}") from exc


with DAG(
    dag_id="aurum_backfill_scenarios",
    description="Trigger scripted scenario backfills and watermark updates",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "backfill"],
) as dag:
    start = EmptyOperator(task_id="start")
    execute_backfill = PythonOperator(task_id="run_backfill", python_callable=run_backfill)

    chain(start, execute_backfill)
