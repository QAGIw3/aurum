"""Airflow DAG for backfilling historical external data."""

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

BACKFILL_CONFIG = Path(__file__).resolve().parents[2] / "config" / "external_backfill_config.json"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(hours=1),
    "start_date": datetime(2024, 9, 21, tzinfo=timezone.utc),
}

# SLA for backfill operations - should complete within 8 hours
SLA = timedelta(hours=8)


def _load_backfill_config() -> dict[str, Any]:
    """Load backfill configuration."""
    if not BACKFILL_CONFIG.exists():
        return {}
    return json.loads(BACKFILL_CONFIG.read_text(encoding="utf-8"))


def _create_backfill_task(dag: DAG, provider: str, dataset: str) -> BashOperator:
    """Create a task to backfill historical data for a provider/dataset."""
    return BashOperator(
        task_id=f"backfill_{provider}_{dataset}",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.backfill import run_backfill

        async def main():
            await run_backfill(
                provider='{provider}',
                dataset='{dataset}',
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
        execution_timeout=timedelta(hours=4),
    )


def _create_backfill_validation_task(dag: DAG, provider: str) -> BashOperator:
    """Create a task to validate backfilled data."""
    return BashOperator(
        task_id=f"validate_backfill_{provider}",
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


def _create_backfill_lineage_task(dag: DAG, provider: str) -> BashOperator:
    """Create a task to emit lineage for backfilled data."""
    return BashOperator(
        task_id=f"emit_backfill_lineage_{provider}",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.lineage import emit_backfill_lineage

        async def main():
            await emit_backfill_lineage(
                provider='{provider}',
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
    "external_backfill",
    default_args=DEFAULT_ARGS,
    description="Backfill historical external data with validation",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=None,
    tags=["external", "backfill", "manual"],
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
config = _load_backfill_config()
providers = config.get("providers", [])

for provider in providers:
    provider_name = provider["name"]
    datasets = provider.get("datasets", [])

    for dataset in datasets:
        backfill_task = _create_backfill_task(dag, provider_name, dataset)
        validation_task = _create_backfill_validation_task(dag, provider_name)
        lineage_task = _create_backfill_lineage_task(dag, provider_name)

        start >> backfill_task >> validation_task >> lineage_task >> end
