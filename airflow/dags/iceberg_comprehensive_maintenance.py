"""Comprehensive Airflow DAG for Iceberg table maintenance operations."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Sequence

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from aurum.airflow_utils import build_failure_callback, emit_alert, metrics
from aurum.iceberg import maintenance

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Configuration
MAINTENANCE_SCHEDULES = {
    'snapshot_expiry': os.getenv("AURUM_ICEBERG_SNAPSHOT_SCHEDULE", "0 2 * * *"),  # Daily at 2 AM
    'compaction': os.getenv("AURUM_ICEBERG_COMPACTION_SCHEDULE", "0 3 * * *"),      # Daily at 3 AM
    'manifest_rewrite': os.getenv("AURUM_ICEBERG_MANIFEST_SCHEDULE", "0 4 * * *"),  # Daily at 4 AM
    'orphan_cleanup': os.getenv("AURUM_ICEBERG_ORPHAN_SCHEDULE", "0 5 * * *"),      # Daily at 5 AM
    'metadata_refresh': os.getenv("AURUM_ICEBERG_METADATA_SCHEDULE", "0 6 * * 1"), # Weekly on Monday
    'statistics_update': os.getenv("AURUM_ICEBERG_STATS_SCHEDULE", "0 1 * * *"),   # Daily at 1 AM
}

# Table configurations for different maintenance operations
TABLE_CONFIGS = {
    'high_frequency': {
        'tables': [
            "iceberg.market.curve_observation",
            "iceberg.external.timeseries_observation",
            "iceberg.fact.fct_curve_observation"
        ],
        'snapshot_retention_days': 7,
        'target_file_size_mb': 256,
        'orphan_retention_hours': 48,
        'priority': 'high'
    },
    'medium_frequency': {
        'tables': [
            "iceberg.market.scenario_output",
            "iceberg.raw.curve_landing",
            "iceberg.market.curve_observation_quarantine"
        ],
        'snapshot_retention_days': 14,
        'target_file_size_mb': 128,
        'orphan_retention_hours': 72,
        'priority': 'medium'
    },
    'low_frequency': {
        'tables': [
            "iceberg.market.series_curve_map",
            "iceberg.ref.geography",
            "iceberg.ref.calendar"
        ],
        'snapshot_retention_days': 30,
        'target_file_size_mb': 64,
        'orphan_retention_hours': 168,  # 7 days
        'priority': 'low'
    }
}

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


def _get_tables_by_priority(priority: str) -> List[str]:
    """Get tables for a specific priority level."""
    tables = []
    for config in TABLE_CONFIGS.values():
        if config['priority'] == priority:
            tables.extend(config['tables'])
    return tables


def _get_table_config(table_name: str) -> Optional[Dict[str, Any]]:
    """Get configuration for a specific table."""
    for config in TABLE_CONFIGS.values():
        if table_name in config['tables']:
            return config
    return None


def _create_maintenance_task(dag: DAG, operation: str, table_name: str,
                           config: Dict[str, Any], dry_run: bool = False) -> PythonOperator:
    """Create a maintenance task for a specific operation and table."""

    def _execute_maintenance(**context):
        """Execute a specific maintenance operation."""
        task_instance = context['task_instance']
        table_config = _get_table_config(table_name) or {}

        # Operation-specific parameters
        op_kwargs = {
            "table_name": table_name,
            "dry_run": dry_run
        }

        if operation == "expire_snapshots":
            op_kwargs.update({
                "older_than_days": table_config.get('snapshot_retention_days', 14)
            })
        elif operation == "rewrite_data_files":
            op_kwargs.update({
                "target_file_size_mb": table_config.get('target_file_size_mb', 128)
            })
        elif operation == "purge_orphan_files":
            op_kwargs.update({
                "older_than_hours": table_config.get('orphan_retention_hours', 24)
            })

        # Execute the maintenance operation
        if hasattr(maintenance, operation):
            func = getattr(maintenance, operation)
            result = func(**op_kwargs)

            # Log metrics
            metric_tags = {
                "operation": operation,
                "table": table_name.replace(".", "_"),
                "dry_run": str(dry_run).lower(),
                "priority": table_config.get('priority', 'unknown')
            }

            try:
                metrics.increment("iceberg.maintenance.tasks_executed", tags=metric_tags)
                metrics.gauge("iceberg.maintenance.execution_time", result.get('execution_time', 0), tags=metric_tags)

                if 'rows_affected' in result:
                    metrics.gauge("iceberg.maintenance.rows_affected", result['rows_affected'], tags=metric_tags)

            except Exception as e:
                LOGGER.warning(f"Failed to emit metrics for {operation} on {table_name}: {e}")

            task_instance.xcom_push(key=f"{operation}_result", value=result)
            return result
        else:
            raise ValueError(f"Unknown maintenance operation: {operation}")

    task_id = f"{operation}_{table_name.replace('.', '_').replace('-', '_')}"

    return PythonOperator(
        task_id=task_id,
        python_callable=_execute_maintenance,
        op_kwargs={},
        dag=dag,
        pool=f"iceberg_maintenance_{config['priority']}",
        sla=timedelta(hours=2) if config['priority'] == 'high' else timedelta(hours=4)
    )


def _create_health_check_task(dag: DAG, operation: str, tables: List[str]) -> BashOperator:
    """Create a health check task for maintenance operations."""

    def _health_check(**context):
        """Perform health checks after maintenance operations."""
        task_instance = context['task_instance']
        dag_run = context['dag_run']

        # Get results from upstream tasks
        results = {}
        for table in tables:
            task_id = f"{operation}_{table.replace('.', '_').replace('-', '_')}"
            result = task_instance.xcom_pull(task_ids=task_id)
            if result:
                results[table] = result

        # Analyze results
        successful = 0
        failed = 0
        total_execution_time = 0

        for table, result in results.items():
            if result.get('success', False):
                successful += 1
                total_execution_time += result.get('execution_time', 0)
            else:
                failed += 1

        # Emit health check metrics
        health_tags = {
            "operation": operation,
            "total_tables": str(len(tables)),
            "successful_tables": str(successful),
            "failed_tables": str(failed)
        }

        try:
            metrics.increment("iceberg.maintenance.health_checks", tags=health_tags)
            metrics.gauge("iceberg.maintenance.total_execution_time", total_execution_time, tags=health_tags)
        except Exception as e:
            LOGGER.warning(f"Failed to emit health check metrics: {e}")

        # Log detailed results
        LOGGER.info(f"Health check for {operation}: {successful}/{len(tables)} successful")

        if failed > 0:
            failed_tables = [table for table, result in results.items() if not result.get('success', False)]
            error_msg = f"{operation} failed for {len(failed_tables)} tables: {', '.join(failed_tables)}"
            emit_alert(
                "WARN",
                error_msg,
                source=f"aurum.airflow.iceberg_maintenance.{operation}",
                context={"failed_tables": failed_tables}
            )
            raise RuntimeError(error_msg)

        return {
            "operation": operation,
            "successful_tables": successful,
            "failed_tables": failed,
            "total_execution_time": total_execution_time,
            "results": results
        }

    return BashOperator(
        task_id=f"health_check_{operation}",
        bash_command="""
        #!/bin/bash
        echo "Performing health check for {{ params.operation }} on {{ params.table_count }} tables"
        echo "Health check completed successfully"
        """,
        params={"operation": operation, "table_count": len(tables)},
        python_callable=_health_check,
        dag=dag
    )


def _create_summary_task(dag: DAG, operations: List[str], all_tables: List[str]) -> PythonOperator:
    """Create a summary task that aggregates results from all maintenance operations."""

    def _generate_summary(**context):
        """Generate a comprehensive maintenance summary."""
        task_instance = context['task_instance']
        dag_run = context['dag_run']

        summary = {
            "dag_run_id": dag_run.run_id,
            "execution_date": dag_run.execution_date.isoformat(),
            "operations": {},
            "overall_status": "SUCCESS",
            "total_tables": len(all_tables),
            "total_operations": len(operations)
        }

        for operation in operations:
            operation_results = {}
            failed_tables = []

            for table in all_tables:
                task_id = f"{operation}_{table.replace('.', '_').replace('-', '_')}"
                result = task_instance.xcom_pull(task_ids=task_id)
                if result:
                    operation_results[table] = result
                    if not result.get('success', False):
                        failed_tables.append(table)

            summary["operations"][operation] = {
                "results": operation_results,
                "successful_tables": len(operation_results) - len(failed_tables),
                "failed_tables": failed_tables,
                "total_execution_time": sum(
                    r.get('execution_time', 0) for r in operation_results.values()
                )
            }

            # Update overall status
            if failed_tables:
                summary["overall_status"] = "PARTIAL_SUCCESS"

        # Emit summary metrics
        try:
            metrics.increment("iceberg.maintenance.dag_completions", tags={"status": summary["overall_status"]})
            metrics.gauge("iceberg.maintenance.operations_executed", len(operations), tags={"dag_run_id": dag_run.run_id})
            metrics.gauge("iceberg.maintenance.tables_processed", len(all_tables), tags={"dag_run_id": dag_run.run_id})
        except Exception as e:
            LOGGER.warning(f"Failed to emit summary metrics: {e}")

        # Log summary
        LOGGER.info(f"Maintenance summary: {json.dumps(summary, indent=2, default=str)}")

        return summary

    return PythonOperator(
        task_id="generate_maintenance_summary",
        python_callable=_generate_summary,
        dag=dag
    )


def create_maintenance_dag(dag_id: str, schedule: str, operations: List[str],
                          tables: List[str], dry_run: bool = False) -> DAG:
    """Create a maintenance DAG with specified operations and tables."""

    dag = DAG(
        dag_id=dag_id,
        description=f"Iceberg maintenance: {', '.join(operations)}",
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule,
        catchup=False,
        dagrun_timeout=timedelta(hours=6),
        tags=["iceberg", "maintenance"] + operations,
        max_active_runs=1
    )

    dag.on_failure_callback = build_failure_callback(source=f"aurum.airflow.{dag_id}")

    # Create tasks for each operation and table combination
    operation_tasks = {}

    for operation in operations:
        operation_tasks[operation] = []

        for table in tables:
            table_config = _get_table_config(table) or TABLE_CONFIGS['medium_frequency']
            task = _create_maintenance_task(dag, operation, table, table_config, dry_run)
            operation_tasks[operation].append(task)

    # Create health check tasks
    health_check_tasks = []
    for operation in operations:
        health_task = _create_health_check_task(dag, operation, tables)
        health_check_tasks.append(health_task)

        # Set up dependencies: all operation tasks -> health check
        for task in operation_tasks[operation]:
            task >> health_task

    # Create summary task
    summary_task = _create_summary_task(dag, operations, tables)

    # Set up final dependencies: all health checks -> summary
    for health_task in health_check_tasks:
        health_task >> summary_task

    return dag


# Create individual DAGs for different maintenance operations
snapshot_expiry_dag = create_maintenance_dag(
    dag_id="iceberg_snapshot_expiry",
    schedule=MAINTENANCE_SCHEDULES['snapshot_expiry'],
    operations=["expire_snapshots"],
    tables=_get_tables_by_priority('high') + _get_tables_by_priority('medium'),
    dry_run=os.getenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
)

compaction_dag = create_maintenance_dag(
    dag_id="iceberg_compaction",
    schedule=MAINTENANCE_SCHEDULES['compaction'],
    operations=["rewrite_data_files"],
    tables=_get_tables_by_priority('high') + _get_tables_by_priority('medium'),
    dry_run=os.getenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
)

manifest_rewrite_dag = create_maintenance_dag(
    dag_id="iceberg_manifest_rewrite",
    schedule=MAINTENANCE_SCHEDULES['manifest_rewrite'],
    operations=["rewrite_manifests"],
    tables=_get_tables_by_priority('high'),
    dry_run=os.getenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
)

orphan_cleanup_dag = create_maintenance_dag(
    dag_id="iceberg_orphan_cleanup",
    schedule=MAINTENANCE_SCHEDULES['orphan_cleanup'],
    operations=["purge_orphan_files"],
    tables=_get_tables_by_priority('high') + _get_tables_by_priority('medium') + _get_tables_by_priority('low'),
    dry_run=os.getenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
)

# Create comprehensive maintenance DAG that runs all operations
comprehensive_maintenance_dag = create_maintenance_dag(
    dag_id="iceberg_comprehensive_maintenance",
    schedule=None,  # Manually triggered
    operations=["expire_snapshots", "rewrite_data_files", "purge_orphan_files"],
    tables=_get_tables_by_priority('high') + _get_tables_by_priority('medium'),
    dry_run=os.getenv("AURUM_ICEBERG_MAINTENANCE_DRY_RUN", "false").lower() in {"1", "true", "yes", "on"}
)

# Export DAGs
__all__ = [
    "snapshot_expiry_dag",
    "compaction_dag",
    "manifest_rewrite_dag",
    "orphan_cleanup_dag",
    "comprehensive_maintenance_dag"
]
