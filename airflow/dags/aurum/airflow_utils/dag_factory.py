"""Feature-flagged DAG factory with consolidation support.

Supports gradual migration from individual DAGs to consolidated patterns:
- Legacy mode: Individual DAGs with custom logic
- Consolidated mode: Reusable patterns with configuration-driven behavior
- Feature flags control migration between modes

Migration phases:
1. Individual DAGs (current state)
2. Consolidated factory with templates
3. Single unified pipeline orchestrator
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Union
from enum import Enum

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

# Feature flags for pipeline consolidation
PIPELINE_FEATURE_FLAGS = {
    "use_consolidated_dags": os.getenv("AURUM_USE_CONSOLIDATED_DAGS", "false").lower() == "true",
    "pipeline_migration_phase": os.getenv("AURUM_PIPELINE_MIGRATION_PHASE", "1"),
    "enable_dag_templates": os.getenv("AURUM_ENABLE_DAG_TEMPLATES", "false").lower() == "true",
}

# Data source types for consolidation
class DataSourceType(str, Enum):
    """Types of data sources supported by the factory."""
    EIA = "eia"
    FRED = "fred"
    CPI = "cpi"
    ISO = "iso"
    NOAA = "noaa"
    PUBLIC_FEEDS = "public_feeds"
    VENDOR_CURVES = "vendor_curves"

# Pipeline types for consolidation
class PipelineType(str, Enum):
    """Types of pipelines supported by the factory."""
    BATCH_INGEST = "batch_ingest"
    STREAM_INGEST = "stream_ingest"
    TRANSFORM = "transform"
    VALIDATE = "validate"
    MONITOR = "monitor"

def get_pipeline_migration_phase() -> str:
    """Get current pipeline migration phase."""
    return PIPELINE_FEATURE_FLAGS["pipeline_migration_phase"]

def is_pipeline_consolidation_enabled() -> bool:
    """Check if consolidated DAGs are enabled."""
    return PIPELINE_FEATURE_FLAGS["use_consolidated_dags"]

def log_pipeline_migration_status():
    """Log current pipeline migration status."""
    phase = get_pipeline_migration_phase()
    consolidated = is_pipeline_consolidation_enabled()
    logger = logging.getLogger(__name__)

    logger.info(f"Pipeline Migration Status: Phase {phase}, Consolidated: {consolidated}")
    logger.info(f"Template DAGs Enabled: {PIPELINE_FEATURE_FLAGS['enable_dag_templates']}")


class DAGFactory:
    """Feature-flagged DAG factory with consolidation support."""

    def __init__(self, dag_id: str, **dag_kwargs):
        """Initialize DAG factory with base configuration."""
        self.dag_id = dag_id
        self.dag_config = DEFAULT_DAG_CONFIG.copy()
        self.dag_config.update(dag_kwargs)

        # Log migration status
        log_pipeline_migration_status()

        # Set default schedule if not provided
        if "schedule_interval" not in self.dag_config:
            self.dag_config["schedule_interval"] = "0 */4 * * *"  # Every 4 hours

        # Set default SLA if not provided
        if "sla" not in self.dag_config:
            self.dag_config["sla"] = SLA_CONFIGS.get("medium_frequency")

        # Create the DAG
        self.dag = DAG(dag_id, **self.dag_config)

    def create_consolidated_ingestion_dag(
        self,
        source_type: DataSourceType,
        target_system: str = "timescale",
        **kwargs
    ) -> DAG:
        """Create a consolidated ingestion DAG using feature flags.

        Args:
            source_type: Type of data source (EIA, FRED, etc.)
            target_system: Target system (timescale, trino, iceberg)
            **kwargs: Additional configuration options

        Returns:
            Configured DAG for the ingestion pipeline
        """
        if not is_pipeline_consolidation_enabled():
            # Fall back to legacy individual DAG
            return self._create_legacy_ingestion_dag(source_type, target_system, **kwargs)

        # Create consolidated DAG
        dag_name = f"consolidated_ingest_{source_type.value}_{target_system}"
        desc = f"Consolidated ingestion from {source_type.value} to {target_system}"

        # Configure based on source type
        config_overrides = {}
        if source_type == DataSourceType.EIA:
            config_overrides.update({
                "retries": 5,
                "execution_timeout": timedelta(hours=6),
                "pool": "api_eia",
                "pool_slots": 1,
                "sla": SLA_CONFIGS["high_frequency"],
            })
        elif source_type == DataSourceType.ISO:
            config_overrides.update({
                "retries": 3,
                "execution_timeout": timedelta(minutes=45),
                "pool": "api_iso",
                "pool_slots": 2,
                "sla": SLA_CONFIGS["medium_frequency"],
            })
        elif source_type == DataSourceType.NOAA:
            config_overrides.update({
                "retries": 2,
                "execution_timeout": timedelta(minutes=30),
                "pool": "api_noaa",
                "pool_slots": 1,
                "sla": SLA_CONFIGS["low_frequency"],
            })

        # Apply overrides
        self.dag_config.update(config_overrides)
        self.dag_config.update({
            "dag_id": dag_name,
            "description": desc,
            "tags": ["aurum", "consolidated", source_type.value],
        })

        # Recreate DAG with new config
        self.dag = DAG(dag_name, **self.dag_config)

        with self.dag:
            # Standard ingestion pipeline
            start = EmptyOperator(task_id="start")
            extract = self._create_extract_task(source_type)
            validate = self._create_validate_task(source_type)
            load = self._create_load_task(source_type, target_system)
            end = EmptyOperator(task_id="end")

            # Define task dependencies
            start >> extract >> validate >> load >> end

        return self.dag

    def _create_legacy_ingestion_dag(
        self,
        source_type: DataSourceType,
        target_system: str,
        **kwargs
    ) -> DAG:
        """Create legacy individual ingestion DAG for backward compatibility."""
        dag_name = f"legacy_ingest_{source_type.value}_{target_system}"
        desc = f"Legacy individual ingestion from {source_type.value} to {target_system}"

        self.dag_config.update({
            "dag_id": dag_name,
            "description": desc,
            "tags": ["aurum", "legacy", "individual", source_type.value],
        })

        self.dag = DAG(dag_name, **self.dag_config)

        with self.dag:
            # Legacy specific implementation based on source
            if source_type == DataSourceType.EIA:
                self._create_eia_legacy_tasks()
            elif source_type == DataSourceType.ISO:
                self._create_iso_legacy_tasks()
            elif source_type == DataSourceType.NOAA:
                self._create_noaa_legacy_tasks()
            else:
                # Generic fallback
                self._create_generic_legacy_tasks(source_type)

        return self.dag

    def _create_extract_task(self, source_type: DataSourceType) -> EmptyOperator:
        """Create extraction task based on source type."""
        return EmptyOperator(task_id=f"extract_{source_type.value}")

    def _create_validate_task(self, source_type: DataSourceType) -> EmptyOperator:
        """Create validation task based on source type."""
        return EmptyOperator(task_id=f"validate_{source_type.value}")

    def _create_load_task(self, source_type: DataSourceType, target_system: str) -> EmptyOperator:
        """Create load task based on source type and target."""
        return EmptyOperator(task_id=f"load_{source_type.value}_{target_system}")

    def _create_eia_legacy_tasks(self):
        """Create EIA-specific legacy tasks."""
        start = EmptyOperator(task_id="start")
        extract = BashOperator(
            task_id="extract_eia_data",
            bash_command="echo 'Extracting EIA data'",
        )
        validate = PythonOperator(
            task_id="validate_eia_data",
            python_callable=lambda: print("Validating EIA data"),
        )
        end = EmptyOperator(task_id="end")

        start >> extract >> validate >> end

    def _create_iso_legacy_tasks(self):
        """Create ISO-specific legacy tasks."""
        start = EmptyOperator(task_id="start")
        extract = BashOperator(
            task_id="extract_iso_data",
            bash_command="echo 'Extracting ISO data'",
        )
        end = EmptyOperator(task_id="end")

        start >> extract >> end

    def _create_noaa_legacy_tasks(self):
        """Create NOAA-specific legacy tasks."""
        start = EmptyOperator(task_id="start")
        extract = BashOperator(
            task_id="extract_noaa_data",
            bash_command="echo 'Extracting NOAA data'",
        )
        end = EmptyOperator(task_id="end")

        start >> extract >> end

    def _create_generic_legacy_tasks(self, source_type: DataSourceType):
        """Create generic legacy tasks for unknown sources."""
        start = EmptyOperator(task_id="start")
        process = BashOperator(
            task_id=f"process_{source_type.value}",
            bash_command=f"echo 'Processing {source_type.value} data'",
        )
        end = EmptyOperator(task_id="end")

        start >> process >> end

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
