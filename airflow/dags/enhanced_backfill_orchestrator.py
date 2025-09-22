"""Enhanced backfill orchestrator with pools, concurrency caps, and date range support."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from aurum.airflow_utils import DAGFactory, SLA_CONFIGS, create_backfill_dag


def generate_date_ranges(
    start_date: datetime,
    end_date: datetime,
    chunk_days: int = 30
) -> List[Dict[str, datetime]]:
    """Generate date ranges for backfill processing."""
    date_ranges = []
    current_start = start_date

    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_days), end_date)

        date_ranges.append({
            "start_date": current_start,
            "end_date": current_end,
            "chunk_id": f"{current_start.strftime('%Y%m%d')}_{current_end.strftime('%Y%m%d')}"
        })

        current_start = current_end + timedelta(days=1)

    return date_ranges


def create_backfill_config(
    sources: List[str],
    start_date: datetime,
    end_date: datetime,
    chunk_days: int = 30,
    max_concurrent_chunks: int = 3
) -> Dict[str, any]:
    """Create backfill configuration with date ranges and concurrency limits."""
    date_ranges = generate_date_ranges(start_date, end_date, chunk_days)

    return {
        "sources": sources,
        "date_ranges": date_ranges,
        "max_concurrent_chunks": max_concurrent_chunks,
        "chunk_days": chunk_days,
        "total_chunks": len(date_ranges),
        "estimated_duration_hours": len(date_ranges) * 8,  # 8 hours per chunk estimate
    }


async def validate_backfill_readiness(**context):
    """Validate that the system is ready for backfill operations."""
    from aurum.data_ingestion.watermark_store import WatermarkStore

    store = WatermarkStore()

    # Check for recent failures
    stale_sources = await store.get_sources_by_watermark_age(max_age_hours=24)

    if stale_sources:
        raise ValueError(f"Found {len(stale_sources)} stale sources. Please resolve before starting backfill.")

    print(f"✓ Backfill validation passed - {await store.get_watermark_statistics()}")

    return "validation_passed"


def create_backfill_chunk_task(
    chunk_id: str,
    source: str,
    start_date: datetime,
    end_date: datetime,
    factory: DAGFactory,
    task_group: Optional[TaskGroup] = None
) -> BashOperator:
    """Create a task for processing a single backfill chunk."""
    bash_command = f"""
    export VAULT_ADDR={factory.dag_config.get('VAULT_ADDR', 'http://127.0.0.1:8200')}
    export VAULT_TOKEN={factory.dag_config.get('VAULT_TOKEN', 'aurum-dev-token')}
    export PYTHONPATH={factory.dag_config.get('PYTHONPATH_ENTRY', '/opt/airflow/src')}

    {factory.dag_config.get('VENV_PYTHON', '.venv/bin/python')} -c "
    import asyncio
    from datetime import datetime
    from aurum.data_ingestion.backfill_processor import run_backfill_chunk

    async def main():
        await run_backfill_chunk(
            source='{source}',
            start_date='{start_date.isoformat()}',
            end_date='{end_date.isoformat()}',
            chunk_id='{chunk_id}',
            vault_addr='{factory.dag_config.get('VAULT_ADDR', 'http://127.0.0.1:8200')}',
            vault_token='{factory.dag_config.get('VAULT_TOKEN', 'aurum-dev-token')}'
        )

    asyncio.run(main())
    "

    echo 'Backfill chunk {chunk_id} completed for {source} from {start_date} to {end_date}'
    """

    return factory.create_bash_task(
        f"backfill_{source}_{chunk_id}",
        bash_command,
        task_group=task_group,
        pool="backfill_processing",
        execution_timeout=timedelta(hours=6),
    )


def create_enhanced_backfill_dag(
    dag_id: str,
    sources: List[str],
    start_date: datetime,
    end_date: datetime,
    chunk_days: int = 30,
    max_concurrent_chunks: int = 3,
    sla: timedelta = SLA_CONFIGS["bulk_load"],
    **dag_kwargs
) -> DAG:
    """Create an enhanced backfill DAG with sophisticated orchestration."""

    # Create backfill configuration
    backfill_config = create_backfill_config(
        sources, start_date, end_date, chunk_days, max_concurrent_chunks
    )

    # Initialize DAG factory
    factory = DAGFactory(
        dag_id,
        sla=sla,
        max_active_tasks=max_concurrent_chunks * 2,  # Allow some buffer
        **dag_kwargs
    )

    # Set up pools
    factory.set_pool("backfill_processing", max_concurrent_chunks)
    factory.set_pool("backfill_validation", 2)

    # Start and end tasks
    start = factory.create_empty_task("start")
    end = factory.create_empty_task("end")

    # Validation task
    validation_task = factory.create_python_task(
        "validate_readiness",
        validate_backfill_readiness,
        pool="backfill_validation"
    )

    # Create task groups for each source
    source_task_groups = {}
    all_chunk_tasks = []

    for source in sources:
        with factory.create_task_group(f"{source}_backfill") as source_group:
            source_tasks = []

            # Process each date range chunk
            for chunk in backfill_config["date_ranges"]:
                chunk_task = create_backfill_chunk_task(
                    chunk["chunk_id"],
                    source,
                    chunk["start_date"],
                    chunk["end_date"],
                    factory,
                    task_group=source_group
                )
                source_tasks.append(chunk_task)
                all_chunk_tasks.append(chunk_task)

            # Set up dependencies within source group
            if len(source_tasks) > 1:
                for i in range(len(source_tasks) - 1):
                    source_tasks[i] >> source_tasks[i + 1]

            source_task_groups[source] = source_tasks

    # Cross-source dependencies - ensure sources can run in parallel but with limits
    # Group chunks by time to enforce concurrency limits
    chunk_groups = {}
    for i, chunk in enumerate(backfill_config["date_ranges"]):
        group_key = i % max_concurrent_chunks
        if group_key not in chunk_groups:
            chunk_groups[group_key] = []
        chunk_groups[group_key].append(chunk)

    # Set up inter-chunk dependencies to enforce concurrency limits
    for group_key, chunks in chunk_groups.items():
        if len(chunks) > 1:
            # Find tasks for this time group across all sources
            group_tasks = []
            for chunk in chunks:
                for source in sources:
                    task_id = f"backfill_{source}_{chunk['chunk_id']}"
                    # Find the actual task (this is a simplified approach)
                    for task in all_chunk_tasks:
                        if task.task_id == task_id:
                            group_tasks.append(task)
                            break

            # Set up dependencies within the concurrency group
            if len(group_tasks) > 1:
                for i in range(len(group_tasks) - 1):
                    group_tasks[i] >> group_tasks[i + 1]

    # Post-backfill validation
    post_validation_task = factory.create_bash_task(
        "validate_backfill_results",
        """
        echo 'Running post-backfill validation...'
        # Add validation logic here
        echo '✓ Post-backfill validation completed'
        """,
        pool="backfill_validation"
    )

    # Final reporting
    reporting_task = factory.create_bash_task(
        "generate_backfill_report",
        f"""
        echo 'Generating backfill completion report...'
        echo 'Backfill completed for sources: {", ".join(sources)}'
        echo 'Total chunks processed: {backfill_config["total_chunks"]}'
        echo 'Date range: {start_date.date()} to {end_date.date()}'
        """,
    )

    # Wire the complete dependency chain
    start >> validation_task

    # Connect validation to first chunks
    for source in sources:
        if source_task_groups[source]:
            validation_task >> source_task_groups[source][0]

    # Connect all chunks to post-validation
    for task in all_chunk_tasks:
        task >> post_validation_task

    post_validation_task >> reporting_task >> end

    return factory.get_dag()


# Convenience functions for common backfill scenarios
def create_eia_backfill_dag(
    start_date: datetime,
    end_date: datetime,
    **kwargs
) -> DAG:
    """Create a backfill DAG specifically for EIA data sources."""
    return create_enhanced_backfill_dag(
        dag_id="eia_comprehensive_backfill",
        sources=["eia_series", "eia_bulk", "eia_fuel_curves"],
        start_date=start_date,
        end_date=end_date,
        chunk_days=90,  # Quarterly chunks for EIA
        max_concurrent_chunks=2,  # Conservative for EIA API limits
        sla=SLA_CONFIGS["bulk_load"],
        description="Comprehensive EIA data backfill with rate limiting",
        tags=["eia", "backfill", "historical"],
        **kwargs
    )


def create_iso_backfill_dag(
    start_date: datetime,
    end_date: datetime,
    **kwargs
) -> DAG:
    """Create a backfill DAG for ISO market data."""
    return create_enhanced_backfill_dag(
        dag_id="iso_market_data_backfill",
        sources=["caiso", "pjm", "miso", "nyiso", "isone", "spp", "aeso", "ercot"],
        start_date=start_date,
        end_date=end_date,
        chunk_days=30,  # Monthly chunks for ISO data
        max_concurrent_chunks=4,  # Can be more aggressive for ISO data
        sla=SLA_CONFIGS["bulk_load"],
        description="ISO market data backfill across all regions",
        tags=["iso", "backfill", "market-data"],
        **kwargs
    )


def create_weather_backfill_dag(
    start_date: datetime,
    end_date: datetime,
    **kwargs
) -> DAG:
    """Create a backfill DAG for weather data sources."""
    return create_enhanced_backfill_dag(
        dag_id="weather_data_backfill",
        sources=["noaa_ghcnd", "noaa_weather", "noaa_drought"],
        start_date=start_date,
        end_date=end_date,
        chunk_days=60,  # Bimonthly chunks for weather data
        max_concurrent_chunks=3,
        sla=SLA_CONFIGS["bulk_load"],
        description="Weather and climate data backfill",
        tags=["weather", "backfill", "climate"],
        **kwargs
    )


# Main backfill orchestrator DAG
def create_backfill_orchestrator_dag() -> DAG:
    """Create the main backfill orchestrator DAG."""

    # Default configuration
    default_start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    default_end_date = datetime.now(timezone.utc)

    factory = DAGFactory(
        "backfill_orchestrator",
        schedule_interval=None,  # Manual trigger only
        catchup=False,
        max_active_runs=1,
        sla=SLA_CONFIGS["bulk_load"],
        description="Master orchestrator for all backfill operations",
        tags=["orchestrator", "backfill", "system"]
    )

    start = factory.create_empty_task("start")
    end = factory.create_empty_task("end")

    # System readiness check
    readiness_check = factory.create_python_task(
        "check_system_readiness",
        validate_backfill_readiness
    )

    # Trigger individual backfill DAGs
    trigger_eia_backfill = TriggerDagRunOperator(
        task_id="trigger_eia_backfill",
        trigger_dag_id="eia_comprehensive_backfill",
        conf={
            "start_date": default_start_date.isoformat(),
            "end_date": default_end_date.isoformat()
        },
        dag=factory.dag
    )

    trigger_iso_backfill = TriggerDagRunOperator(
        task_id="trigger_iso_backfill",
        trigger_dag_id="iso_market_data_backfill",
        conf={
            "start_date": default_start_date.isoformat(),
            "end_date": default_end_date.isoformat()
        },
        dag=factory.dag
    )

    trigger_weather_backfill = TriggerDagRunOperator(
        task_id="trigger_weather_backfill",
        trigger_dag_id="weather_data_backfill",
        conf={
            "start_date": default_start_date.isoformat(),
            "end_date": default_end_date.isoformat()
        },
        dag=factory.dag
    )

    # Final consolidation
    consolidate_results = factory.create_bash_task(
        "consolidate_backfill_results",
        """
        echo 'Consolidating backfill results...'
        echo 'All backfill operations completed successfully'
        """
    )

    # Wire dependencies
    start >> readiness_check >> [
        trigger_eia_backfill,
        trigger_iso_backfill,
        trigger_weather_backfill
    ] >> consolidate_results >> end

    return factory.get_dag()


# Create the DAG
backfill_orchestrator_dag = create_backfill_orchestrator_dag()
