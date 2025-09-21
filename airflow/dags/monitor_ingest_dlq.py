"""Airflow DAG that monitors the ingest DLQ volume."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable


DEFAULT_ARGS = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


def _run_dlq_check(**_: Any) -> None:
    from datetime import timedelta

    from airflow.models import Variable

    from scripts.ingest.process_dlq import DLQThresholdExceeded, analyse_dlq

    bootstrap = Variable.get("aurum_kafka_bootstrap")
    schema_registry = Variable.get("aurum_schema_registry")
    topic = Variable.get("aurum_ingest_error_topic", default_var="aurum.ingest.error.v1")
    lookback_minutes = int(Variable.get("aurum_ingest_dlq_lookback_minutes", default_var="15"))
    warn_threshold = int(Variable.get("aurum_ingest_dlq_warn_threshold", default_var="20"))
    crit_threshold = int(Variable.get("aurum_ingest_dlq_crit_threshold", default_var="75"))
    max_messages = int(Variable.get("aurum_ingest_dlq_max_messages", default_var="1500"))

    try:
        summary = analyse_dlq(
            bootstrap=bootstrap,
            schema_registry=schema_registry,
            topic=topic,
            lookback=timedelta(minutes=lookback_minutes),
            max_messages=max_messages,
            warn_threshold=warn_threshold,
            crit_threshold=crit_threshold,
        )
    except DLQThresholdExceeded as exc:
        if exc.level == "critical":
            raise
        raise AirflowSkipException(exc.summary)

    print(
        f"DLQ healthy: processed {summary.total_records} records in the last "
        f"{summary.window_minutes} minutes; max source {summary.max_source or '<none>'}"
    )


with DAG(
    dag_id="monitor_ingest_dlq",
    description="Monitor ingest DLQ volume and alert on spikes",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aurum", "monitoring", "dlq"],
) as dag:
    start = EmptyOperator(task_id="start")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
            optional_variables=(
                "aurum_ingest_error_topic",
                "aurum_ingest_dlq_lookback_minutes",
                "aurum_ingest_dlq_warn_threshold",
                "aurum_ingest_dlq_crit_threshold",
                "aurum_ingest_dlq_max_messages",
            ),
        ),
    )

    monitor = PythonOperator(task_id="monitor_dlq", python_callable=_run_dlq_check)

    end = EmptyOperator(task_id="end")

    start >> preflight >> monitor >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.monitor_ingest_dlq")

__all__ = ["dag"]
