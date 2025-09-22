"""Enhanced DAG factory with TaskFlow API, shared defaults, and comprehensive configuration."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Union

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Default configuration
DEFAULT_DAG_CONFIG = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "start_date": datetime(2024, 9, 21, tzinfo=timezone.utc),
    "catchup": False,
    "max_active_runs": 1,
    "max_active_tasks": 10,
    "execution_timeout": timedelta(hours=2),
    "dagrun_timeout": timedelta(hours=4),
    "default_view": "graph",
    "orientation": "TB",
    "tags": ["aurum", "data-ingestion"],
}

# SLA configurations by data source
SLA_CONFIGS = {
    "high_frequency": timedelta(minutes=30),
    "medium_frequency": timedelta(hours=2),
    "low_frequency": timedelta(hours=6),
    "bulk_load": timedelta(hours=24),
}

# Pool configurations for resource management
POOL_CONFIGS = {
    "api_calls": {"slots": 5, "description": "API rate limiting pool"},
    "heavy_processing": {"slots": 2, "description": "Heavy data processing pool"},
    "kafka_producers": {"slots": 3, "description": "Kafka producer pool"},
    "db_writes": {"slots": 4, "description": "Database write operations pool"},
}

# Environment variables
VAULT_ADDR = os.environ.get("AURUM_VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("AURUM_VAULT_TOKEN", "aurum-dev-token")
VENV_PYTHON = os.environ.get("AURUM_VENV_PYTHON", ".venv/bin/python")
BIN_PATH = os.environ.get("AURUM_BIN_PATH", ".venv/bin:$PATH")
PYTHONPATH_ENTRY = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")


class DAGFactory:
    """Enhanced DAG factory with TaskFlow API support and shared configurations."""

    def __init__(self, dag_id: str, **dag_kwargs):
        """Initialize DAG factory with base configuration."""
        self.dag_id = dag_id
        self.dag_config = DEFAULT_DAG_CONFIG.copy()
        self.dag_config.update(dag_kwargs)

        # Set default schedule if not provided
        if "schedule_interval" not in self.dag_config:
            self.dag_config["schedule_interval"] = "0 */4 * * *"  # Every 4 hours

        # Set default SLA if not provided
        if "sla" not in self.dag_config:
            self.dag_config["sla"] = SLA_CONFIGS.get("medium_frequency")

        # Create the DAG
        self.dag = DAG(dag_id, **self.dag_config)

    def create_python_task(
        self,
        task_id: str,
        python_callable: Callable,
        op_kwargs: Optional[Dict[str, Any]] = None,
        task_group: Optional[TaskGroup] = None,
        pool: Optional[str] = None,
        **task_kwargs
    ) -> PythonOperator:
        """Create a PythonOperator with enhanced configuration."""
        env_vars = {
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        }

        config = {
            "task_id": task_id,
            "python_callable": python_callable,
            "op_kwargs": op_kwargs or {},
            "env": env_vars,
            "dag": task_group.dag if task_group else self.dag,
            "pool": pool,
            **task_kwargs
        }

        return PythonOperator(**config)

    def create_bash_task(
        self,
        task_id: str,
        bash_command: str,
        task_group: Optional[TaskGroup] = None,
        pool: Optional[str] = None,
        **task_kwargs
    ) -> BashOperator:
        """Create a BashOperator with enhanced configuration."""
        env_vars = {
            "VAULT_ADDR": VAULT_ADDR,
            "VAULT_TOKEN": VAULT_TOKEN,
            "PYTHONPATH": PYTHONPATH_ENTRY,
        }

        config = {
            "task_id": task_id,
            "bash_command": bash_command,
            "env": env_vars,
            "dag": task_group.dag if task_group else self.dag,
            "pool": pool,
            **task_kwargs
        }

        return BashOperator(**config)

    def create_sensor_task(
        self,
        task_id: str,
        sensor_class: type[BaseSensorOperator],
        sensor_kwargs: Optional[Dict[str, Any]] = None,
        deferrable: bool = True,
        task_group: Optional[TaskGroup] = None,
        **task_kwargs
    ) -> BaseSensorOperator:
        """Create a sensor with deferrable option."""
        if sensor_kwargs is None:
            sensor_kwargs = {}

        config = {
            "task_id": task_id,
            "dag": task_group.dag if task_group else self.dag,
            "deferrable": deferrable,
            **sensor_kwargs,
            **task_kwargs
        }

        return sensor_class(**config)

    def create_seatunnel_task(
        self,
        task_id: str,
        job_config_path: str,
        job_params: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
        pool: str = "heavy_processing",
        **task_kwargs
    ) -> BashOperator:
        """Create a SeaTunnel job task with proper configuration."""
        # Build parameter string for environment variables
        env_exports = "\n".join([f'export {k}="{v}"' for k, v in job_params.items()])

        bash_command = f"""
        {env_exports}
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.seatunnel.orchestrator import run_seatunnel_job

        async def main():
            await run_seatunnel_job(
                config_path='{job_config_path}',
                params={job_params},
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """

        return self.create_bash_task(
            task_id=task_id,
            bash_command=bash_command,
            task_group=task_group,
            pool=pool,
            **task_kwargs
        )

    def create_kafka_producer_task(
        self,
        task_id: str,
        topic: str,
        producer_config: Dict[str, Any],
        task_group: Optional[TaskGroup] = None,
        pool: str = "kafka_producers",
        **task_kwargs
    ) -> BashOperator:
        """Create a Kafka producer task with optimized configuration."""
        bash_command = f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.kafka.producer import run_kafka_producer

        async def main():
            await run_kafka_producer(
                topic='{topic}',
                config={producer_config},
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """

        return self.create_bash_task(
            task_id=task_id,
            bash_command=bash_command,
            task_group=task_group,
            pool=pool,
            **task_kwargs
        )

    def create_validation_task(
        self,
        task_id: str,
        table_name: str,
        expectation_suite: str,
        task_group: Optional[TaskGroup] = None,
        **task_kwargs
    ) -> BashOperator:
        """Create a data validation task."""
        bash_command = f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.data_quality.validation import run_data_validation

        async def main():
            await run_data_validation(
                table_name='{table_name}',
                expectation_suite='{expectation_suite}',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """

        return self.create_bash_task(
            task_id=task_id,
            bash_command=bash_command,
            task_group=task_group,
            **task_kwargs
        )

    def create_lineage_task(
        self,
        task_id: str,
        data_source: str,
        task_group: Optional[TaskGroup] = None,
        **task_kwargs
    ) -> BashOperator:
        """Create a data lineage tracking task."""
        bash_command = f"""
        export VAULT_ADDR={VAULT_ADDR}
        export VAULT_TOKEN={VAULT_TOKEN}
        export PYTHONPATH={PYTHONPATH_ENTRY}

        {VENV_PYTHON} -c "
        import asyncio
        from aurum.data_lineage.tracker import emit_data_lineage

        async def main():
            await emit_data_lineage(
                data_source='{data_source}',
                vault_addr='{VAULT_ADDR}',
                vault_token='{VAULT_TOKEN}'
            )

        asyncio.run(main())
        "
        """

        return self.create_bash_task(
            task_id=task_id,
            bash_command=bash_command,
            task_group=task_group,
            **task_kwargs
        )

    def create_empty_task(
        self,
        task_id: str,
        task_group: Optional[TaskGroup] = None,
        **task_kwargs
    ) -> EmptyOperator:
        """Create an empty task for DAG structure."""
        config = {
            "task_id": task_id,
            "dag": task_group.dag if task_group else self.dag,
            **task_kwargs
        }

        return EmptyOperator(**config)

    def create_task_group(
        self,
        group_id: str,
        tooltip: str = "",
        **group_kwargs
    ) -> TaskGroup:
        """Create a task group for logical organization."""
        return TaskGroup(
            group_id=group_id,
            tooltip=tooltip,
            dag=self.dag,
            **group_kwargs
        )

    def get_dag(self) -> DAG:
        """Return the configured DAG."""
        return self.dag

    def add_failure_callback(self, callback: Callable):
        """Add a failure callback to the DAG."""
        self.dag.on_failure_callback = callback

    def add_success_callback(self, callback: Callable):
        """Add a success callback to the DAG."""
        self.dag.on_success_callback = callback

    def set_sla(self, sla: timedelta):
        """Set SLA for the DAG."""
        self.dag.sla = sla

    def set_pool(self, pool_name: str, slots: int = 1):
        """Create or configure a pool for resource management."""
        # This would typically be done via Airflow admin or CLI
        # For now, we'll just store the pool configuration
        if pool_name not in POOL_CONFIGS:
            POOL_CONFIGS[pool_name] = {"slots": slots}


# Convenience functions for common patterns
def create_incremental_dag(
    dag_id: str,
    providers: List[str],
    sla: timedelta = SLA_CONFIGS["medium_frequency"],
    **dag_kwargs
) -> DAG:
    """Create an incremental data ingestion DAG."""
    factory = DAGFactory(dag_id, sla=sla, **dag_kwargs)

    start = factory.create_empty_task("start")
    end = factory.create_empty_task("end")

    # Provider-specific tasks
    provider_tasks = []
    for provider in providers:
        with factory.create_task_group(f"{provider}_processing") as provider_group:
            incremental_task = factory.create_bash_task(
                f"incremental_{provider}",
                f"echo 'Processing incremental data for {provider}'",
                task_group=provider_group
            )

            validation_task = factory.create_validation_task(
                f"validate_{provider}",
                f"external.timeseries_observation_{provider}",
                "external_timeseries_obs",
                task_group=provider_group
            )

            incremental_task >> validation_task
            provider_tasks.append(validation_task)

    # Common tasks
    dbt_task = factory.create_bash_task("run_external_dbt", "echo 'Running dbt models'")
    validation_task = factory.create_validation_task(
        "validate_dbt_outputs",
        "int_external_obs_conformed",
        "external_obs_conformed"
    )
    lineage_task = factory.create_lineage_task("emit_incremental_lineage", "external")

    # Wire the dependency chain
    start >> provider_tasks >> dbt_task >> validation_task >> lineage_task >> end

    return factory.get_dag()


def create_seatunnel_dag(
    dag_id: str,
    seatunnel_jobs: List[Dict[str, Any]],
    sla: timedelta = SLA_CONFIGS["medium_frequency"],
    **dag_kwargs
) -> DAG:
    """Create a SeaTunnel-based data processing DAG."""
    factory = DAGFactory(dag_id, sla=sla, **dag_kwargs)

    start = factory.create_empty_task("start")
    end = factory.create_empty_task("end")

    # SeaTunnel job tasks
    job_tasks = []
    for job in seatunnel_jobs:
        job_task = factory.create_seatunnel_task(
            job["task_id"],
            job["config_path"],
            job["params"]
        )
        job_tasks.append(job_task)

    # Add validation and lineage
    validation_task = factory.create_validation_task(
        "validate_seatunnel_outputs",
        "seatunnel_output",
        "seatunnel_quality"
    )
    lineage_task = factory.create_lineage_task("emit_seatunnel_lineage", "seatunnel")

    # Wire dependencies
    start >> job_tasks >> validation_task >> lineage_task >> end

    return factory.get_dag()


def create_backfill_dag(
    dag_id: str,
    backfill_config: Dict[str, Any],
    sla: timedelta = SLA_CONFIGS["bulk_load"],
    **dag_kwargs
) -> DAG:
    """Create a backfill orchestration DAG."""
    factory = DAGFactory(dag_id, sla=sla, **dag_kwargs)

    # Set up pools for backfill
    factory.set_pool("backfill_processing", 3)

    start = factory.create_empty_task("start")
    end = factory.create_empty_task("end")

    # Backfill orchestration
    backfill_tasks = []
    for source in backfill_config.get("sources", []):
        backfill_task = factory.create_bash_task(
            f"backfill_{source}",
            f"echo 'Backfilling data for {source}'",
            pool="backfill_processing"
        )
        backfill_tasks.append(backfill_task)

    # Validation for backfilled data
    validation_task = factory.create_validation_task(
        "validate_backfill",
        "backfill_results",
        "backfill_quality"
    )

    # Wire dependencies
    start >> backfill_tasks >> validation_task >> end

    return factory.get_dag()
