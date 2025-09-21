"""Airflow DAG for orchestrating backfill operations."""

from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import json

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from aurum.slices import SliceClient, SliceConfig, SliceManager, SliceType
from aurum.bulk_archive import BulkArchiveManager, ArchiveConfig
from aurum.logging import create_logger, LogLevel


# Default DAG arguments
default_args = {
    'owner': 'data-ingestion',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=6),
}

# Create DAG
dag = DAG(
    'aurum_backfill_orchestrator',
    default_args=default_args,
    description='Orchestrates backfill operations for historical data',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    max_active_tasks=10,
    tags=['aurum', 'backfill', 'orchestrator'],
    params={
        "source_name": "eia",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "operation_type": "backfill",  # backfill, replay, partial_reprocess
        "priority": "normal",
        "max_concurrency": 3,
        "batch_size": 1000,
        "dry_run": False
    }
)


def validate_backfill_request(**context) -> str:
    """Validate backfill request parameters."""
    logger = create_logger(
        source_name="backfill_orchestrator",
        kafka_bootstrap_servers=None,
        kafka_topic="aurum.backfill.validation",
        dataset="backfill_validation"
    )

    ti = context['task_instance']
    params = ti.dag_run.conf or dag.params

    source_name = params.get('source_name', 'eia')
    start_date = params.get('start_date', '2024-01-01')
    end_date = params.get('end_date', '2024-12-31')
    operation_type = params.get('operation_type', 'backfill')
    dry_run = params.get('dry_run', False)

    logger.log(
        LogLevel.INFO,
        f"Validating backfill request: {source_name} from {start_date} to {end_date}",
        "backfill_validation_started",
        source_name=source_name,
        start_date=start_date,
        end_date=end_date,
        operation_type=operation_type
    )

    # Validate parameters
    if not source_name:
        raise ValueError("source_name parameter is required")

    if not start_date or not end_date:
        raise ValueError("start_date and end_date parameters are required")

    # Parse dates
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d').date()
        end = datetime.strptime(end_date, '%Y-%m-%d').date()
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")

    if start > end:
        raise ValueError("start_date must be before end_date")

    # Check for existing operations
    if operation_type == "backfill":
        # Check if backfill is already running for this source
        async def check_existing():
            config = SliceConfig(database_url=Variable.get("AURUM_DATABASE_URL"))
            async with SliceClient(config) as client:
                query = SliceQuery(
                    source_name=source_name,
                    status=SliceStatus.RUNNING,
                    limit=100
                )
                running_slices = await client.get_slices(query)
                return len(running_slices) > 0

        # For now, just log and continue
        logger.log(
            LogLevel.INFO,
            f"Backfill validation passed for {source_name}",
            "backfill_validation_completed",
            source_name=source_name,
            start_date=str(start),
            end_date=str(end)
        )

    return "proceed_with_backfill"


def create_backfill_slices(**context) -> None:
    """Create slices for backfill operation."""
    logger = create_logger(
        source_name="backfill_orchestrator",
        kafka_bootstrap_servers=None,
        kafka_topic="aurum.backfill.slice_creation",
        dataset="slice_creation"
    )

    ti = context['task_instance']
    params = ti.dag_run.conf or dag.params

    source_name = params['source_name']
    start_date = params['start_date']
    end_date = params['end_date']
    operation_type = params['operation_type']
    priority = params['priority']
    batch_size = params['batch_size']

    logger.log(
        LogLevel.INFO,
        f"Creating slices for {operation_type} operation",
        "slice_creation_started",
        source_name=source_name,
        start_date=start_date,
        end_date=end_date,
        operation_type=operation_type
    )

    # Parse dates
    start = datetime.strptime(start_date, '%Y-%m-%d').date()
    end = datetime.strptime(end_date, '%Y-%m-%d').date()

    # Create slice configurations
    slice_configs = []
    current_date = start

    while current_date <= end:
        slice_config = {
            "slice_key": f"{operation_type}_{source_name}_{current_date.strftime('%Y%m%d')}",
            "slice_type": "time_window",
            "slice_data": {
                "date": current_date.strftime('%Y-%m-%d'),
                "operation_type": operation_type,
                "source_name": source_name,
                "batch_size": batch_size
            },
            "priority": 50 if priority == "high" else 100,
            "records_expected": batch_size * 24,  # Estimate based on hourly data
            "metadata": {
                "created_by": "backfill_orchestrator",
                "operation_type": operation_type,
                "source_name": source_name,
                "date": current_date.strftime('%Y-%m-%d')
            }
        }
        slice_configs.append(slice_config)
        current_date += timedelta(days=1)

    # Store slice configs in XCom for next task
    ti.xcom_push(key='slice_configs', value=slice_configs)
    ti.xcom_push(key='total_slices', value=len(slice_configs))

    logger.log(
        LogLevel.INFO,
        f"Created {len(slice_configs)} slice configurations",
        "slice_creation_completed",
        source_name=source_name,
        slice_count=len(slice_configs),
        operation_type=operation_type
    )


def execute_backfill_slices(**context) -> None:
    """Execute backfill slices using slice manager."""
    logger = create_logger(
        source_name="backfill_orchestrator",
        kafka_bootstrap_servers=None,
        kafka_topic="aurum.backfill.slice_execution",
        dataset="slice_execution"
    )

    ti = context['task_instance']
    params = ti.dag_run.conf or dag.params

    source_name = params['source_name']
    operation_type = params['operation_type']
    max_concurrency = params.get('max_concurrency', 3)

    # Get slice configs from previous task
    slice_configs = ti.xcom_pull(task_ids='create_backfill_slices', key='slice_configs')
    total_slices = ti.xcom_pull(task_ids='create_backfill_slices', key='total_slices')

    if not slice_configs:
        raise ValueError("No slice configurations found")

    logger.log(
        LogLevel.INFO,
        f"Executing {len(slice_configs)} slices for {source_name}",
        "slice_execution_started",
        source_name=source_name,
        slice_count=len(slice_configs),
        max_concurrency=max_concurrency
    )

    # Create slice manager
    config = SliceConfig(database_url=Variable.get("AURUM_DATABASE_URL"))
    client = SliceClient(config)
    manager = SliceManager(client)

    # Create slices
    slice_ids = manager.create_slices_from_operation(
        source_name=source_name,
        operation_type=operation_type,
        operation_data={
            "dag_run_id": context['dag_run'].run_id,
            "total_slices": total_slices,
            "max_concurrency": max_concurrency
        },
        slice_configs=slice_configs
    )

    # Start workers to process slices
    worker_tasks = []
    for i in range(max_concurrency):
        worker_id = f"backfill_worker_{source_name}_{i+1}"
        worker_tasks.append(
            manager.start_worker(
                worker_id=worker_id,
                source_name=source_name,
                slice_types=[SliceType.TIME_WINDOW],
                max_slices=total_slices // max_concurrency + 1
            )
        )

    # Wait for all workers to complete
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        # Run workers concurrently
        loop.run_until_complete(asyncio.gather(*worker_tasks))

        # Get final results
        completed_slices = 0
        failed_slices = 0

        for slice_config in slice_configs:
            slice_key = slice_config['slice_key']
            # Get slice status from database
            # This would need to be implemented based on the actual slice client

        ti.xcom_push(key='completed_slices', value=completed_slices)
        ti.xcom_push(key='failed_slices', value=failed_slices)

        logger.log(
            LogLevel.INFO,
            f"Slice execution completed: {completed_slices}/{len(slice_configs)} successful",
            "slice_execution_completed",
            source_name=source_name,
            completed_slices=completed_slices,
            failed_slices=failed_slices
        )

    finally:
        loop.close()


def generate_backfill_report(**context) -> None:
    """Generate backfill operation report."""
    logger = create_logger(
        source_name="backfill_orchestrator",
        kafka_bootstrap_servers=None,
        kafka_topic="aurum.backfill.reporting",
        dataset="backfill_reporting"
    )

    ti = context['task_instance']
    params = ti.dag_run.conf or dag.params

    source_name = params['source_name']
    start_date = params['start_date']
    end_date = params['end_date']
    operation_type = params['operation_type']

    # Get execution results
    completed_slices = ti.xcom_pull(task_ids='execute_backfill_slices', key='completed_slices') or 0
    failed_slices = ti.xcom_pull(task_ids='execute_backfill_slices', key='failed_slices') or 0
    total_slices = ti.xcom_pull(task_ids='create_backfill_slices', key='total_slices') or 0

    # Generate report
    report = {
        "operation_id": context['dag_run'].run_id,
        "source_name": source_name,
        "operation_type": operation_type,
        "date_range": f"{start_date} to {end_date}",
        "execution_summary": {
            "total_slices": total_slices,
            "completed_slices": completed_slices,
            "failed_slices": failed_slices,
            "success_rate": completed_slices / max(total_slices, 1)
        },
        "execution_time": str(context['dag_run'].end_date - context['dag_run'].start_date) if context['dag_run'].end_date else None,
        "generated_at": datetime.now().isoformat()
    }

    # Save report to file
    report_file = f"/tmp/backfill_report_{source_name}_{context['dag_run'].run_id}.json"
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.log(
        LogLevel.INFO,
        f"Generated backfill report: {report_file}",
        "backfill_report_generated",
        source_name=source_name,
        report_file=report_file,
        success_rate=report["execution_summary"]["success_rate"]
    )

    # Push report to XCom
    ti.xcom_push(key='backfill_report', value=report)


# DAG Tasks
validate_request = BranchPythonOperator(
    task_id='validate_backfill_request',
    python_callable=validate_backfill_request,
    dag=dag
)

proceed_with_backfill = DummyOperator(
    task_id='proceed_with_backfill',
    dag=dag
)

create_slices = PythonOperator(
    task_id='create_backfill_slices',
    python_callable=create_backfill_slices,
    dag=dag
)

execute_slices = PythonOperator(
    task_id='execute_backfill_slices',
    python_callable=execute_backfill_slices,
    dag=dag
)

generate_report = PythonOperator(
    task_id='generate_backfill_report',
    python_callable=generate_backfill_report,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command='find /tmp -name "backfill_report_*" -mtime +7 -delete',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

# DAG Flow
validate_request >> [proceed_with_backfill, DummyOperator(task_id='validation_failed', dag=dag)]

proceed_with_backfill >> create_slices >> execute_slices >> generate_report >> cleanup
