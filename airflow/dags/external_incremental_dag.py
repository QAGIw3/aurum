"""Airflow DAG for incremental external data updates."""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")

INCREMENTAL_CONFIG = Path(__file__).resolve().parents[2] / "config" / "external_incremental_config.json"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=30),
    "start_date": datetime(2024, 9, 21, tzinfo=timezone.utc),
}

# SLA for incremental updates - should complete within 1 hour
SLA = timedelta(hours=1)


def _load_incremental_config() -> dict[str, Any]:
    """Load incremental update configuration."""
    if not INCREMENTAL_CONFIG.exists():
        return {}
    return json.loads(INCREMENTAL_CONFIG.read_text(encoding="utf-8"))


def _create_incremental_task(dag: DAG, provider: str) -> BashOperator:
    """Create a task to run incremental updates for a provider."""
    config = _load_incremental_config()
    provider_config = next((p for p in config.get("providers", []) if p["name"] == provider.lower()), {})
    datasets = provider_config.get("datasets", [])
    
    datasets_str = " ".join(f"'{d}'" for d in datasets) if datasets else ""
    
    return BashOperator(
        task_id=f"incremental_{provider}",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.incremental import run_incremental_update

        async def main():
            await run_incremental_update(
                provider='{provider}',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}',
                datasets=[{datasets_str}]
            )

        asyncio.run(main())
        "
        """,
        env={
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        },
        sla=SLA,
        execution_timeout=timedelta(minutes=45),
    )


def _create_incremental_validation_task(dag: DAG, provider: str) -> BashOperator:
    """Create a task to validate incremental data."""
    return BashOperator(
        task_id=f"validate_incremental_{provider}",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.ge_validation import run_ge_validation

        async def main():
            await run_ge_validation(
                table_name='external.timeseries_observation_{provider}',
                expectation_suite='external_timeseries_obs',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """,
        env={
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        },
        sla=SLA,
    )


def _create_incremental_dbt_task(dag: DAG) -> BashOperator:
    """Create a task to run dbt models for external data."""
    return BashOperator(
        task_id="run_external_dbt",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.dbt_runner import run_external_models

        async def main():
            await run_external_models(
                models=['stg_external__*', 'int_external__*', 'cur_external__*'],
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """,
        env={
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        },
        sla=SLA,
    )


def _create_dbt_validation_task(dag: DAG) -> BashOperator:
    """Create a task to validate dbt outputs."""
    return BashOperator(
        task_id="validate_dbt_outputs",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.ge_validation import run_ge_validation

        async def main():
            await run_ge_validation(
                table_name='int_external_obs_conformed',
                expectation_suite='external_obs_conformed',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )
            await run_ge_validation(
                table_name='cur_external_obs_mapped',
                expectation_suite='external_obs_curated',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """,
        env={
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        },
        sla=SLA,
    )


def _create_incremental_lineage_task(dag: DAG) -> BashOperator:
    """Create a task to emit lineage for incremental data."""
    return BashOperator(
        task_id="emit_incremental_lineage",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.lineage import emit_incremental_lineage

        async def main():
            await emit_incremental_lineage(
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """,
        env={
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        },
        sla=SLA,
    )


# Create the DAG
dag = DAG(
    "external_incremental",
    default_args=DEFAULT_ARGS,
    description="Incremental external data updates with validation",
    schedule_interval="0 */4 * * *",  # Every 4 hours
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=None,
    tags=["external", "incremental", "frequent"],
)

# Start task
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

# End task
end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# Load configuration and create tasks
config = _load_incremental_config()
providers = config.get("providers", ["FRED", "EIA"])  # Default providers

# Provider-specific tasks
provider_tasks = []
for provider in providers:
    incremental_task = _create_incremental_task(dag, provider)
    validation_task = _create_incremental_validation_task(dag, provider)

    start >> incremental_task >> validation_task
    provider_tasks.append(validation_task)

# Common tasks
dbt_task = _create_incremental_dbt_task(dag)
validation_task = _create_dbt_validation_task(dag)
lineage_task = _create_incremental_lineage_task(dag)

# Wire the dependency chain
provider_tasks >> dbt_task >> validation_task >> lineage_task >> end
