"""Main Airflow DAG factory for dynamic DAG generation from configuration files."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from .builders import (
    DatasetConfig,
    SeaTunnelTaskBuilder,
    VaultSecretBuilder,
    WatermarkBuilder,
    TaskGroupBuilder
)


class AirflowDagFactory:
    """Factory for generating Airflow DAGs from configuration files."""

    def __init__(
        self,
        dag_id: str = "dynamic_ingestion",
        default_args: Optional[Dict[str, Any]] = None,
        schedule_interval: str = "0 6 * * *",
        start_date: Optional[Any] = None,
        catchup: bool = False,
        max_active_runs: int = 1,
        tags: Optional[List[str]] = None,
        description: str = "Dynamic data ingestion DAG generated from configuration"
    ):
        """Initialize the DAG factory.

        Args:
            dag_id: DAG identifier
            default_args: Default arguments for tasks
            schedule_interval: Cron schedule interval
            start_date: DAG start date
            catchup: Whether to enable catchup
            max_active_runs: Maximum number of active DAG runs
            tags: List of DAG tags
            description: DAG description
        """
        self.dag_id = dag_id
        self.default_args = default_args or self._get_default_args()
        self.schedule_interval = schedule_interval
        self.start_date = start_date or self._get_default_start_date()
        self.catchup = catchup
        self.max_active_runs = max_active_runs
        self.tags = tags or ["aurum", "ingestion", "dynamic"]
        self.description = description

        # Initialize builders
        self.task_builder = SeaTunnelTaskBuilder()
        self.secret_builder = VaultSecretBuilder()
        self.watermark_builder = WatermarkBuilder()
        self.group_builder = TaskGroupBuilder()

    def _get_default_args(self) -> Dict[str, Any]:
        """Get default arguments for tasks."""
        return {
            "owner": "aurum-data",
            "depends_on_past": False,
            "email_on_failure": True,
            "email": ["aurum-ops@example.com"],
            "retries": 3,
            "retry_delay": timedelta(minutes=10),
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(minutes=60),
            "execution_timeout": timedelta(minutes=45),
        }

    def _get_default_start_date(self) -> datetime:
        """Get default start date for DAG."""
        return datetime(2024, 1, 1)

    def create_dag(self, config_files: List[str]) -> DAG:
        """Create a DAG from configuration files.

        Args:
            config_files: List of configuration file paths

        Returns:
            Configured Airflow DAG
        """
        with DAG(
            dag_id=self.dag_id,
            description=self.description,
            default_args=self.default_args,
            schedule_interval=self.schedule_interval,
            start_date=self.start_date,
            catchup=self.catchup,
            max_active_runs=self.max_active_runs,
            tags=self.tags,
        ) as dag:
            # Load all configurations
            all_configs = []
            for config_file in config_files:
                configs = self._load_configs(config_file)
                all_configs.extend(configs)

            # Group configurations by source type
            grouped_configs = self._group_configs_by_source(all_configs)

            # Create task groups for each source
            task_groups = []
            for source_type, configs in grouped_configs.items():
                group = self._create_source_task_group(source_type, configs)
                task_groups.append(group)

            # Create start/end tasks
            start_task = EmptyOperator(task_id="start")
            end_task = EmptyOperator(task_id="end")

            # Set up dependencies
            start_task >> task_groups >> end_task

        return dag

    def _load_configs(self, config_file: str) -> List[DatasetConfig]:
        """Load dataset configurations from file.

        Args:
            config_file: Path to configuration file

        Returns:
            List of dataset configurations
        """
        config_path = Path(config_file)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_file}")

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                raw_config = json.load(f)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"Failed to parse {config_file}: {e}")

        datasets = raw_config.get("datasets", [])
        if not isinstance(datasets, list):
            raise RuntimeError(f"Expected 'datasets' array in {config_file}")

        configs = []
        for dataset in datasets:
            try:
                config = DatasetConfig.from_dict(dataset)
                configs.append(config)
            except Exception as e:
                print(f"Warning: Failed to parse dataset config: {e}")

        return configs

    def _group_configs_by_source(self, configs: List[DatasetConfig]) -> Dict[str, List[DatasetConfig]]:
        """Group configurations by source type.

        Args:
            configs: List of dataset configurations

        Returns:
            Dictionary mapping source types to configuration lists
        """
        grouped = {}
        for config in configs:
            source_type = self._get_source_type(config)
            if source_type not in grouped:
                grouped[source_type] = []
            grouped[source_type].append(config)
        return grouped

    def _get_source_type(self, config: DatasetConfig) -> str:
        """Determine source type from configuration.

        Args:
            config: Dataset configuration

        Returns:
            Source type string
        """
        if config.source_name.startswith("eia_"):
            return "eia"
        elif config.source_name.startswith("fred_"):
            return "fred"
        elif config.source_name.startswith("cpi_"):
            return "cpi"
        elif config.source_name.startswith("noaa_"):
            return "noaa"
        else:
            return "other"

    def _create_source_task_group(
        self,
        source_type: str,
        configs: List[DatasetConfig]
    ) -> TaskGroup:
        """Create task group for a source type.

        Args:
            source_type: Type of data source
            configs: List of dataset configurations

        Returns:
            Task group containing all tasks for this source
        """
        group_id = f"ingest_{source_type}"

        with TaskGroup(group_id=group_id, prefix_group_id=False) as group:
            # Create tasks for each configuration
            for config in configs:
                self._create_dataset_tasks(config)

        return group

    def _create_dataset_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for a single dataset configuration.

        Args:
            config: Dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Determine task type and create appropriate tasks
        source_type = self._get_source_type(config)

        if source_type == "eia":
            tasks.extend(self._create_eia_tasks(config))
        elif source_type == "fred":
            tasks.extend(self._create_fred_tasks(config))
        elif source_type == "cpi":
            tasks.extend(self._create_cpi_tasks(config))
        elif source_type == "noaa":
            tasks.extend(self._create_noaa_tasks(config))
        else:
            tasks.extend(self._create_generic_tasks(config))

        return tasks

    def _create_eia_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for EIA dataset.

        Args:
            config: EIA dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Create SeaTunnel ingestion task
        env_vars = self.task_builder.build_eia_env(config)
        vault_mappings = self.secret_builder.build_eia_mappings()

        task = self.task_builder.create_seatunnel_task(
            job_name="eia_series_to_kafka",
            env_assignments=env_vars,
            mappings=vault_mappings,
            task_id_override=f"seatunnel_{config.source_name}",
            pool="api_eia"
        )
        tasks.append(task)

        # Create watermark task
        watermark_task = self.watermark_builder.create_watermark_task(
            source_name=config.source_name,
            policy=config.watermark_policy,
            task_id=f"{config.source_name}_watermark"
        )
        tasks.append(watermark_task)

        # Set up dependencies
        task >> watermark_task

        return tasks

    def _create_fred_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for FRED dataset.

        Args:
            config: FRED dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Create SeaTunnel ingestion task
        env_vars = self.task_builder.build_fred_env(config)
        vault_mappings = self.secret_builder.build_fred_mappings()

        task = self.task_builder.create_seatunnel_task(
            job_name="fred_series_to_kafka",
            env_assignments=env_vars,
            mappings=vault_mappings,
            task_id_override=f"seatunnel_{config.source_name}",
            pool="api_fred"
        )
        tasks.append(task)

        # Create watermark task
        watermark_task = self.watermark_builder.create_watermark_task(
            source_name=config.source_name,
            policy=config.watermark_policy,
            task_id=f"{config.source_name}_watermark"
        )
        tasks.append(watermark_task)

        # Set up dependencies
        task >> watermark_task

        return tasks

    def _create_cpi_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for CPI dataset.

        Args:
            config: CPI dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Create SeaTunnel ingestion task
        env_vars = self.task_builder.build_cpi_env(config)
        vault_mappings = self.secret_builder.build_fred_mappings()  # CPI uses FRED API key

        task = self.task_builder.create_seatunnel_task(
            job_name="cpi_series_to_kafka",
            env_assignments=env_vars,
            mappings=vault_mappings,
            task_id_override=f"seatunnel_{config.source_name}",
            pool="api_fred"  # CPI uses FRED pool
        )
        tasks.append(task)

        # Create watermark task
        watermark_task = self.watermark_builder.create_watermark_task(
            source_name=config.source_name,
            policy=config.watermark_policy,
            task_id=f"{config.source_name}_watermark"
        )
        tasks.append(watermark_task)

        # Set up dependencies
        task >> watermark_task

        return tasks

    def _create_noaa_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for NOAA dataset.

        Args:
            config: NOAA dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Create SeaTunnel ingestion task
        env_vars = self.task_builder.build_noaa_env(config)
        vault_mappings = self.secret_builder.build_noaa_mappings()

        task = self.task_builder.create_seatunnel_task(
            job_name="noaa_ghcnd_to_kafka",
            env_assignments=env_vars,
            mappings=vault_mappings,
            task_id_override=f"seatunnel_{config.source_name}",
            pool="api_noaa"
        )
        tasks.append(task)

        # Create watermark task
        watermark_task = self.watermark_builder.create_watermark_task(
            source_name=config.source_name,
            policy=config.watermark_policy,
            task_id=f"{config.source_name}_watermark"
        )
        tasks.append(watermark_task)

        # Set up dependencies
        task >> watermark_task

        return tasks

    def _create_generic_tasks(self, config: DatasetConfig) -> List[Union[BashOperator, PythonOperator]]:
        """Create tasks for generic dataset.

        Args:
            config: Generic dataset configuration

        Returns:
            List of created tasks
        """
        tasks = []

        # Create generic SeaTunnel task
        env_vars = self.task_builder.build_generic_env(config)
        vault_mappings = self.secret_builder.build_generic_mappings(config.source_name)

        task = self.task_builder.create_seatunnel_task(
            job_name=f"{config.source_name}_to_kafka",
            env_assignments=env_vars,
            mappings=vault_mappings,
            task_id_override=f"seatunnel_{config.source_name}",
            pool="api_generic"
        )
        tasks.append(task)

        # Create watermark task
        watermark_task = self.watermark_builder.create_watermark_task(
            source_name=config.source_name,
            policy=config.watermark_policy,
            task_id=f"{config.source_name}_watermark"
        )
        tasks.append(watermark_task)

        # Set up dependencies
        task >> watermark_task

        return tasks


# Convenience function for creating DAGs
def create_dynamic_ingestion_dag(
    config_files: List[str],
    dag_id: str = "dynamic_ingestion"
) -> DAG:
    """Create a dynamic ingestion DAG from configuration files.

    Args:
        config_files: List of configuration file paths
        dag_id: DAG identifier

    Returns:
        Configured Airflow DAG
    """
    factory = AirflowDagFactory(dag_id=dag_id)
    return factory.create_dag(config_files)
