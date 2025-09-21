"""Airflow DAG to refresh curve facts/marts and run freshness tests."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from aurum.airflow_utils import build_failure_callback

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
}

DBT_BIN = os.environ.get("AURUM_DBT_BIN", "dbt")
DBT_PROJECT_DIR = os.environ.get("AURUM_DBT_PROJECT_DIR", "/opt/airflow/dbt")
DBT_PROFILES_DIR = os.environ.get("AURUM_DBT_PROFILES_DIR", "/opt/airflow/dbt")

DBT_BASE_CMD = f"{DBT_BIN} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"

with DAG(
    dag_id="refresh_curve_marts",
    description="Refresh fct_curve_observation and hot marts, then run freshness checks",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=build_failure_callback("refresh_curve_marts"),
    tags=["dbt", "curve", "iceberg"],
) as dag:
    start = EmptyOperator(task_id="start")

    refresh_facts = BashOperator(
        task_id="dbt_run_facts",
        bash_command=f"{DBT_BASE_CMD} run --select fct_curve_observation",
    )

    refresh_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            f"{DBT_BASE_CMD} run --select mart_curve_latest mart_curve_asof_diff"
        ),
    )

    test_freshness = BashOperator(
        task_id="dbt_test_freshness",
        bash_command=(
            f"{DBT_BASE_CMD} test --select test_fct_curve_freshness test_mart_curve_latest_freshness"
        ),
    )

    end = EmptyOperator(task_id="end")

    start >> refresh_facts >> refresh_marts >> test_freshness >> end
