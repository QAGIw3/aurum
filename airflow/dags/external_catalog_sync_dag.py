"""Airflow DAG to sync external data catalogs with Great Expectations validation."""

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

CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "external_catalog_config.json"
EXTERNAL_PROVIDERS_CONFIG = Path(__file__).resolve().parents[2] / "config" / "external_providers.json"

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "start_date": datetime(2024, 9, 21, tzinfo=timezone.utc),
}

# SLA for catalog sync - should complete within 2 hours
SLA = timedelta(hours=2)


def _load_external_providers() -> list[dict[str, Any]]:
    """Load external provider configuration."""
    if not EXTERNAL_PROVIDERS_CONFIG.exists():
        return []
    raw = json.loads(EXTERNAL_PROVIDERS_CONFIG.read_text(encoding="utf-8"))
    return raw.get("providers", [])


def _create_provider_sync_task(dag: DAG, provider: dict[str, Any]) -> BashOperator:
    """Create a task to sync catalog for a specific provider."""
    provider_name = provider["name"]

    return BashOperator(
        task_id=f"sync_{provider_name}_catalog",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.catalog_sync import sync_provider_catalog

        async def main():
            await sync_provider_catalog(
                provider='{provider_name}',
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


def _create_ge_validation_task(dag: DAG, table_name: str) -> BashOperator:
    """Create a Great Expectations validation task."""
    return BashOperator(
        task_id=f"validate_{table_name}",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.ge_validation import run_ge_validation

        async def main():
            await run_ge_validation(
                table_name='{table_name}',
                expectation_suite='external_series_catalog',
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


def _create_lineage_task(dag: DAG) -> BashOperator:
    """Create a task to emit OpenLineage events."""
    return BashOperator(
        task_id="emit_lineage",
        bash_command=f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.external.lineage import emit_catalog_lineage

        async def main():
            await emit_catalog_lineage(
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
    "external_catalog_sync",
    default_args=DEFAULT_ARGS,
    description="Sync external data catalogs with validation and lineage",
    schedule_interval="0 6 * * *",  # Daily at 6 AM UTC
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=None,
    tags=["external", "catalog", "daily"],
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

# Load providers and create sync tasks
providers = _load_external_providers()
sync_tasks = []

for provider in providers:
    sync_task = _create_provider_sync_task(dag, provider)
    validation_task = _create_ge_validation_task(dag, f"external.series_catalog_{provider['name']}")
    lineage_task = _create_lineage_task(dag)

    start >> sync_task >> validation_task >> lineage_task >> end
    sync_tasks.append(sync_task)

# If no providers configured, still run validation on existing data
if not providers:
    validation_task = _create_ge_validation_task(dag, "external.series_catalog")
    lineage_task = _create_lineage_task(dag)

    start >> validation_task >> lineage_task >> end
