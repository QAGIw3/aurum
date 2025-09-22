"""Airflow DAG to backfill derived dbt models on a rolling window."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ShortCircuitOperator

from aurum.airflow_utils import build_failure_callback

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
}

PYTHON_BIN = os.environ.get("AURUM_PYTHON_BIN", "python3")
BACKFILL_SCRIPT = os.environ.get(
    "AURUM_DERIVED_BACKFILL_SCRIPT",
    "/opt/airflow/scripts/dbt/backfill_derived_models.py",
)
DERIVED_MODELS = os.environ.get(
    "AURUM_DERIVED_MODELS",
    "publish_curve_observation mart_curve_latest mart_curve_asof_diff mart_scenario_metric_latest mart_scenario_output",
)
MODEL_ARGS = " ".join(DERIVED_MODELS.split())

DBT_BIN = os.environ.get("AURUM_DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.environ.get("AURUM_DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("AURUM_DBT_PROFILES_DIR", "/opt/airflow/dbt")
DBT_BASE_CMD = f"{DBT_BIN} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

with DAG(
    dag_id="aurum_backfill_derived_models",
    description="Run targeted dbt backfills for derived marts and validate freshness",
    default_args=DEFAULT_ARGS,
    schedule="30 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "backfill", "derived"],
    on_failure_callback=build_failure_callback("aurum_backfill_derived_models"),
) as dag:
    start = EmptyOperator(task_id="start")

    backfill_recent_window = BashOperator(
        task_id="backfill_recent_window",
        bash_command=(
            f"{PYTHON_BIN} {BACKFILL_SCRIPT} {MODEL_ARGS} "
            "--full-refresh "
            "--start-date {{ macros.ds_add(ds, -2) }} "
            "--end-date {{ ds }}"
        ),
    )

    def _is_month_start(**context: Any) -> bool:
        logical_date = context["logical_date"]
        return logical_date.day == 1

    month_start_gate = ShortCircuitOperator(
        task_id="month_start_gate",
        python_callable=_is_month_start,
    )

    backfill_month_to_date = BashOperator(
        task_id="backfill_month_to_date",
        bash_command=(
            f"{PYTHON_BIN} {BACKFILL_SCRIPT} {MODEL_ARGS} "
            "--full-refresh "
            "--start-date {{ ds | ds_format('%Y-%m-01', '%Y-%m-%d') }} "
            "--end-date {{ ds }}"
        ),
    )

    validate_outputs = BashOperator(
        task_id="validate_outputs",
        bash_command=(
            f"{DBT_BASE_CMD} test --select test_fct_curve_freshness "
            "test_mart_curve_latest_freshness test_mart_scenario_output_not_empty"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> backfill_recent_window >> month_start_gate >> backfill_month_to_date >> validate_outputs >> end
