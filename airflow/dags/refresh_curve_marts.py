"""Airflow DAG to refresh curve facts/marts and run freshness tests with DBT management service integration."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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


async def execute_scheduled_dbt_tests() -> None:
    """Execute scheduled DBT tests using the management service."""
    try:
        from aurum.api.services.dbt_management_shim import get_dbt_management_service

        service = get_dbt_management_service()
        results = await service.execute_scheduled_tests()

        print(f"Executed {results['executed_count']} test schedules")
        print(f"Results: {results['results']}")

        # Check for failures
        failed_schedules = [
            name for name, result in results['results'].items()
            if result.get('status') in ['error', 'failed']
        ]

        if failed_schedules:
            raise Exception(f"Scheduled tests failed for: {', '.join(failed_schedules)}")

    except Exception as e:
        print(f"Error executing scheduled tests: {e}")
        raise


def run_scheduled_tests():
    """Synchronous wrapper for async test execution."""
    import asyncio
    asyncio.run(execute_scheduled_dbt_tests())

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

    execute_scheduled_tests = PythonOperator(
        task_id="execute_scheduled_tests",
        python_callable=run_scheduled_tests,
    )

    end = EmptyOperator(task_id="end")

    start >> refresh_facts >> refresh_marts >> test_freshness >> execute_scheduled_tests >> end
