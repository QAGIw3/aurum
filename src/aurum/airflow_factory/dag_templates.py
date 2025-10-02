"""DAG templates for standardized Airflow workflow patterns.

Provides reusable DAG templates to consolidate 50+ individual DAGs into
a smaller set of configurable templates.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.utils.task_group import TaskGroup
except ImportError:
    DAG = None  # type: ignore
    PythonOperator = None  # type: ignore
    EmptyOperator = None  # type: ignore
    TaskGroup = None  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class DagConfig:
    """Configuration for DAG generation."""
    dag_id: str
    description: str
    schedule: str = "@daily"
    start_date: datetime = field(default_factory=lambda: datetime(2025, 1, 1))
    catchup: bool = False
    max_active_runs: int = 1
    default_args: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class IngestionConfig:
    """Configuration for data ingestion DAG."""
    source: str  # "eia", "fred", "noaa", etc.
    dataset: str  # Dataset name
    extract_callable: Callable
    transform_callable: Optional[Callable] = None
    load_callable: Optional[Callable] = None
    validate_callable: Optional[Callable] = None
    quality_check_callable: Optional[Callable] = None


class DataIngestionDagFactory:
    """Factory for creating standardized data ingestion DAGs.
    
    Consolidates the many individual ingestion DAGs into a single
    template-based pattern.
    """
    
    @staticmethod
    def create_ingestion_dag(
        dag_config: DagConfig,
        ingestion_config: IngestionConfig
    ) -> DAG:
        """Create a standardized data ingestion DAG.
        
        Creates a DAG with the following standard pattern:
        1. Validate source
        2. Extract data
        3. Transform data (optional)
        4. Load data
        5. Quality check (optional)
        
        Args:
            dag_config: DAG configuration
            ingestion_config: Ingestion-specific configuration
            
        Returns:
            Configured Airflow DAG
            
        Example:
            ```python
            from aurum.airflow_factory.dag_templates import (
                DataIngestionDagFactory,
                DagConfig,
                IngestionConfig
            )
            
            dag_config = DagConfig(
                dag_id="ingest_eia_electricity",
                description="Ingest EIA electricity data",
                schedule="@hourly",
                tags=["ingestion", "eia", "electricity"]
            )
            
            ingestion_config = IngestionConfig(
                source="eia",
                dataset="electricity",
                extract_callable=extract_eia_electricity,
                transform_callable=transform_eia_data,
                load_callable=load_to_iceberg
            )
            
            dag = DataIngestionDagFactory.create_ingestion_dag(
                dag_config,
                ingestion_config
            )
            ```
        """
        if DAG is None:
            raise ImportError("Airflow is required for DAG creation")
        
        # Set default args
        default_args = {
            "owner": "aurum",
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "retries": 3,
            "retry_delay": timedelta(minutes=5),
            **dag_config.default_args
        }
        
        # Create DAG
        dag = DAG(
            dag_id=dag_config.dag_id,
            description=dag_config.description,
            schedule_interval=dag_config.schedule,
            start_date=dag_config.start_date,
            catchup=dag_config.catchup,
            max_active_runs=dag_config.max_active_runs,
            default_args=default_args,
            tags=dag_config.tags
        )
        
        with dag:
            # 1. Validate source (optional)
            if ingestion_config.validate_callable:
                validate = PythonOperator(
                    task_id="validate_source",
                    python_callable=ingestion_config.validate_callable,
                    op_kwargs={
                        "source": ingestion_config.source,
                        "dataset": ingestion_config.dataset
                    }
                )
            else:
                validate = EmptyOperator(task_id="validate_source")
            
            # 2. Extract data
            extract = PythonOperator(
                task_id="extract_data",
                python_callable=ingestion_config.extract_callable,
                op_kwargs={
                    "source": ingestion_config.source,
                    "dataset": ingestion_config.dataset
                }
            )
            
            # 3. Transform data (optional)
            if ingestion_config.transform_callable:
                transform = PythonOperator(
                    task_id="transform_data",
                    python_callable=ingestion_config.transform_callable,
                    op_kwargs={
                        "dataset": ingestion_config.dataset
                    }
                )
            else:
                transform = EmptyOperator(task_id="transform_data")
            
            # 4. Load data
            if ingestion_config.load_callable:
                load = PythonOperator(
                    task_id="load_data",
                    python_callable=ingestion_config.load_callable,
                    op_kwargs={
                        "dataset": ingestion_config.dataset
                    }
                )
            else:
                load = EmptyOperator(task_id="load_data")
            
            # 5. Quality check (optional)
            if ingestion_config.quality_check_callable:
                quality = PythonOperator(
                    task_id="quality_check",
                    python_callable=ingestion_config.quality_check_callable,
                    op_kwargs={
                        "dataset": ingestion_config.dataset
                    }
                )
            else:
                quality = EmptyOperator(task_id="quality_check")
            
            # Define task dependencies (standard flow)
            validate >> extract >> transform >> load >> quality
        
        logger.info(f"Created ingestion DAG: {dag_config.dag_id}")
        return dag


class TransformationDagFactory:
    """Factory for creating standardized transformation DAGs."""
    
    @staticmethod
    def create_transformation_dag(
        dag_config: DagConfig,
        transformation_steps: List[Dict[str, Any]]
    ) -> DAG:
        """Create a transformation DAG with configurable steps.
        
        Args:
            dag_config: DAG configuration
            transformation_steps: List of transformation step configs
            
        Returns:
            Configured Airflow DAG
        """
        if DAG is None:
            raise ImportError("Airflow is required for DAG creation")
        
        default_args = {
            "owner": "aurum",
            "depends_on_past": True,
            "email_on_failure": True,
            "retries": 2,
            "retry_delay": timedelta(minutes=10),
            **dag_config.default_args
        }
        
        dag = DAG(
            dag_id=dag_config.dag_id,
            description=dag_config.description,
            schedule_interval=dag_config.schedule,
            start_date=dag_config.start_date,
            catchup=dag_config.catchup,
            default_args=default_args,
            tags=dag_config.tags
        )
        
        with dag:
            tasks = []
            
            for step_config in transformation_steps:
                task = PythonOperator(
                    task_id=step_config["task_id"],
                    python_callable=step_config["callable"],
                    op_kwargs=step_config.get("op_kwargs", {})
                )
                tasks.append(task)
            
            # Chain tasks in order
            for i in range(len(tasks) - 1):
                tasks[i] >> tasks[i + 1]
        
        logger.info(f"Created transformation DAG: {dag_config.dag_id} with {len(tasks)} steps")
        return dag


class MonitoringDagFactory:
    """Factory for creating standardized monitoring/health check DAGs."""
    
    @staticmethod
    def create_monitoring_dag(
        dag_config: DagConfig,
        checks: List[Dict[str, Callable]]
    ) -> DAG:
        """Create a monitoring DAG with configurable health checks.
        
        Args:
            dag_config: DAG configuration
            checks: List of check configurations with callables
            
        Returns:
            Configured Airflow DAG
        """
        if DAG is None:
            raise ImportError("Airflow is required for DAG creation")
        
        default_args = {
            "owner": "aurum-ops",
            "depends_on_past": False,
            "email_on_failure": True,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            **dag_config.default_args
        }
        
        dag = DAG(
            dag_id=dag_config.dag_id,
            description=dag_config.description,
            schedule_interval=dag_config.schedule,
            start_date=dag_config.start_date,
            catchup=False,  # Never catchup for monitoring
            default_args=default_args,
            tags=["monitoring"] + dag_config.tags
        )
        
        with dag:
            check_tasks = []
            
            for check_config in checks:
                task = PythonOperator(
                    task_id=check_config["task_id"],
                    python_callable=check_config["callable"],
                    op_kwargs=check_config.get("op_kwargs", {})
                )
                check_tasks.append(task)
            
            # All checks run in parallel (no dependencies)
        
        logger.info(f"Created monitoring DAG: {dag_config.dag_id} with {len(checks)} checks")
        return dag


def generate_ingestion_dags_from_config(
    config_file: str
) -> Dict[str, DAG]:
    """Generate all ingestion DAGs from a configuration file.
    
    This allows defining all ingestion workflows in a single config file
    rather than creating individual DAG files.
    
    Args:
        config_file: Path to YAML/JSON configuration file
        
    Returns:
        Dictionary of dag_id -> DAG
        
    Example config.yaml:
        ```yaml
        ingestion:
          - source: eia
            dataset: electricity
            schedule: "@hourly"
            extract: aurum.external.eia.extract_electricity
            transform: aurum.external.eia.transform_electricity
            load: aurum.external.eia.load_electricity
          
          - source: fred
            dataset: interest_rates
            schedule: "@daily"
            extract: aurum.external.fred.extract_rates
            load: aurum.external.fred.load_rates
        ```
    """
    # This would parse config file and generate DAGs
    # Simplified implementation
    dags = {}
    
    logger.info(f"Would generate DAGs from config: {config_file}")
    
    return dags


__all__ = [
    "DagConfig",
    "IngestionConfig",
    "DataIngestionDagFactory",
    "TransformationDagFactory",
    "MonitoringDagFactory",
    "generate_ingestion_dags_from_config",
]

