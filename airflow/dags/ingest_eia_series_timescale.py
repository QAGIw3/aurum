"""Airflow DAG to load EIA series Kafka topics into TimescaleDB via SeaTunnel."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from aurum.airflow_utils.datasets import URIS
from aurum.airflow_utils import build_preflight_callable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com", "data-team@aurum.com", "energy-team@aurum.com"],
    "retries": 5,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=6),
    "sla": timedelta(hours=24),
    "pool": "api_eia",
    "pool_slots": 1,
    "on_failure_callback": None,
    "on_success_callback": None,
    "on_retry_callback": None,
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
            "eia_series_timescale",
            description="EIA series streaming load into Timescale",
            schedule="0 * * * *",
            target="timescale.public.eia_series_timeseries",
        )
    except Exception as exc:  # pragma: no cover
        print(f"Failed to register ingest source eia_series_timescale: {exc}")



def emit_lakefs_lineage(dataset: str, **context: Any) -> None:
    repo = os.environ.get("AURUM_LAKEFS_REPO")
    if not repo:
        print("LakeFS repo not configured; skipping lineage commit")
        return

    branch = os.environ.get("AURUM_LAKEFS_BRANCH", "main")
    run_id = context.get("run_id", "unknown")
    dag = context.get("dag")
    dag_id = dag.dag_id if dag else "unknown"
    backfill_flag = context.get("dag_run").conf.get("backfill", False) if context.get("dag_run") else False
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
        commit_branch(
            repo,
            branch,
            f"airflow:{dag_id}:{run_id}",
            metadata,
        )
    except Exception as exc:  # pragma: no cover
        print(f"LakeFS lineage commit failed: {exc}")


from aurum.airflow_utils.timescale import build_timescale_task as _build_ts


try:  # dataset scheduling
    from airflow.datasets import Dataset  # type: ignore
    DATASET_SCHEDULE = [Dataset(URIS.INGEST_EIA_SERIES_RAW)]  # emitted by upstream ingest
    PRODUCED_DATASETS = [Dataset(URIS.INGEST_EIA_SERIES_TIMESCALE)]  # emitted after load
except Exception:  # pragma: no cover
    Dataset = None  # type: ignore
    DATASET_SCHEDULE = "0 * * * *"
    PRODUCED_DATASETS = None

with DAG(
    dag_id="ingest_eia_series_timescale",
    description="Stream EIA series Kafka topics into TimescaleDB",
    default_args=DEFAULT_ARGS,
    schedule=DATASET_SCHEDULE,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "timescale", "eia"],
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

    register_source = PythonOperator(task_id="register_eia_series_source", python_callable=register_stream_source)

    load_timescale = _build_ts(
        task_id="eia_series_kafka_to_timescale",
        job_name="eia_series_kafka_to_timescale",
        env_entries=[
            "KAFKA_BOOTSTRAP_SERVERS='{{ var.value.get(\"aurum_kafka_bootstrap\", \"localhost:9092\") }}'",
            "SCHEMA_REGISTRY_URL='{{ var.value.get(\"aurum_schema_registry\", \"http://localhost:8081\") }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get(\"aurum_timescale_jdbc\", \"jdbc:postgresql://timescale:5432/timeseries\") }}'",
            "EIA_TOPIC_PATTERN='{{ var.value.get(\"aurum_eia_topic_pattern\", \"aurum\\.ref\\.eia\\..*\\.v1\") }}'",
            "EIA_SERIES_TABLE='{{ var.value.get(\"aurum_eia_series_table\", \"eia_series_timeseries\") }}'",
            "DLQ_TOPIC='{{ var.value.get(\"aurum_eia_dlq_topic\", \"aurum.ref.eia.series.dlq.v1\") }}'",
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

            update_ingest_watermark("eia_series_timescale", "logical_date", watermark)
        except Exception as exc:  # pragma: no cover
            print(f"Failed to update eia_series_timescale watermark: {exc}")

    watermark = PythonOperator(task_id="update_eia_series_watermark", python_callable=_update_watermark)

    end = EmptyOperator(task_id="end")

    lakefs_commit = PythonOperator(
        task_id="lakefs_lineage_commit",
        python_callable=emit_lakefs_lineage,
        op_kwargs={"dataset": "timescale.public.eia_series_timeseries"},
    )

    # Attach dataset lineage where supported
    try:
        if Dataset is not None:
            preflight.inlets = [Dataset(URIS.INGEST_EIA_SERIES_RAW)]  # type: ignore[attr-defined]
            lakefs_commit.outlets = PRODUCED_DATASETS  # type: ignore[attr-defined]
    except Exception:
        pass

    start >> preflight >> register_source >> load_timescale >> watermark >> lakefs_commit >> end
