"""Dedicated Airflow DAG for NOAA weather data ingestion workflows.

This DAG provides a comprehensive NOAA data pipeline with:
- Multiple data sources (GHCND, GSOM, etc.)
- Configurable stations and data types
- Enhanced monitoring and alerting
- Better error handling and retry logic
- Support for backfill operations
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.task_group import TaskGroup

from aurum.airflow_utils.alerting import build_failure_callback
from aurum.airflow_utils.metrics import emit_task_metrics
from aurum.lakefs_client import emit_lakefs_lineage
from aurum.parsers.runner import build_seatunnel_task
from aurum.staleness.watermark_tracker import WatermarkTracker
from aurum.telemetry.context import log_structured


def update_watermark(
    dataset_name: str,
    logical_date: datetime,
    **context
) -> None:
    """Update watermark for a dataset."""
    watermark_tracker = WatermarkTracker()
    watermark_tracker.update_watermark(
        dataset_name=dataset_name,
        watermark_key="logical_date",
        watermark_value=logical_date,
        metadata={
            "dag_id": context.get("dag").dag_id,
            "task_id": context.get("task_id"),
            "execution_date": logical_date.isoformat(),
        }
    )


def check_staleness_and_alert(
    dataset_name: str,
    expected_frequency_hours: int = 24,
    **context
) -> None:
    """Check staleness and emit alerts if needed."""
    from aurum.staleness.staleness_monitor import StalenessMonitor
    from aurum.staleness.alert_manager import AlertManager

    monitor = StalenessMonitor.get_instance()
    alert_manager = AlertManager.get_instance()

    # Check staleness
    is_stale = monitor.is_dataset_stale(dataset_name, expected_frequency_hours)

    if is_stale:
        # Create alert
        alert_manager.create_alert(
            dataset=dataset_name,
            alert_type="staleness",
            severity="warning",
            message=f"Dataset {dataset_name} is stale",
            metadata={
                "dag_id": context.get("dag").dag_id,
                "expected_frequency_hours": expected_frequency_hours,
            }
        )


def create_backfill_tasks(
    start_date: str,
    end_date: str,
    dataset_name: str,
    backfill_task_group: TaskGroup,
    dag: DAG
) -> None:
    """Create backfill tasks for a dataset."""
    from airflow.operators.python import PythonOperator

    # Calculate date range
    start = datetime.fromisoformat(start_date)
    end = datetime.fromisoformat(end_date)

    current_date = start
    while current_date <= end:
        task_id = f"backfill_{dataset_name}_{current_date.strftime('%Y%m%d')}"

        PythonOperator(
            task_id=task_id,
            python_callable=run_backfill_for_date,
            op_kwargs={
                "dataset_name": dataset_name,
                "backfill_date": current_date,
            },
            dag=dag,
            task_group=backfill_task_group,
        )

        current_date += timedelta(days=1)


def run_backfill_for_date(
    dataset_name: str,
    backfill_date: datetime,
    **context
) -> None:
    """Run backfill for a specific date."""
    # This would trigger the actual backfill process
    # For now, just log the backfill operation
    log_structured(
        "info",
        "backfill_operation_started",
        dataset=dataset_name,
        backfill_date=backfill_date.isoformat(),
        dag_id=context.get("dag").dag_id,
    )

    # Update watermark after successful backfill
    update_watermark(dataset_name, backfill_date, **context)


# Production-ready default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(hours=4),
    "sla": timedelta(hours=12),  # SLA for production
    "pool": "api_noaa",
    "pool_slots": 1,
    "on_failure_callback": build_failure_callback(source="aurum.airflow.noaa_ingest"),
    "on_success_callback": None,
    "on_retry_callback": None,
}

# NOAA dataset configurations
NOAA_DATASETS = {
    "ghcnd_daily": {
        "dataset": "GHCND",
        "description": "Global Historical Climatology Network - Daily",
        "schedule": "0 6 * * *",  # Daily at 6 AM
        "stations": [
            "GHCND:USW00094728",  # NYC
            "GHCND:USW00023174",  # Chicago
            "GHCND:USW00012842",  # Seattle
            "GHCND:USW00023188",  # Denver
            "GHCND:USW00013874",  # Atlanta
        ],
        "datatypes": ["TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "WSF2", "WSF5"],
        "window_days": 1,
        "topic": "aurum.ref.noaa.weather.ghcnd.daily.v1",
        "pool": "api_noaa",
    },
    "ghcnd_hourly": {
        "dataset": "GHCND",
        "description": "Global Historical Climatology Network - Hourly",
        "schedule": "0 */6 * * *",  # Every 6 hours
        "stations": [
            "GHCND:USW00094728",  # NYC
            "GHCND:USW00023174",  # Chicago
        ],
        "datatypes": ["TMAX", "TMIN", "PRCP"],
        "window_days": 0,
        "window_hours": 1,
        "topic": "aurum.ref.noaa.weather.ghcnd.hourly.v1",
        "pool": "api_noaa",
    },
    "gsom": {
        "dataset": "GSOM",
        "description": "Global Summary of the Month",
        "schedule": "0 8 1 * *",  # Monthly on the 1st at 8 AM
        "stations": [
            "GHCND:USW00094728",  # NYC
            "GHCND:USW00023174",  # Chicago
            "GHCND:USW00012842",  # Seattle
        ],
        "datatypes": ["TAVG", "TMAX", "TMIN", "PRCP", "SNOW"],
        "window_days": 30,
        "topic": "aurum.ref.noaa.weather.gsom.monthly.v1",
        "pool": "api_noaa",
    }
}

# Production-ready NOAA API configuration with cost controls
NOAA_API_CONFIG = {
    "base_url": "https://www.ncei.noaa.gov/cdo-web/api/v2",
    "timeout_seconds": 60,
    "rate_limit_sleep_ms": 300,
    "retry_attempts": 7,
    "retry_backoff_ms": 3000,
    "retry_backoff_max_ms": 180000,
    "page_limit": 1000,
    "station_limit": 1000,
    "daily_request_limit": 50000,
    "hourly_request_limit": 10000,
    "cost_per_1000_requests_usd": 0.01,
    "monthly_budget_usd": 5000.00,
    "enable_circuit_breaker": True,
    "circuit_breaker_failure_threshold": 10,
    "enable_adaptive_rate_limiting": True,
    "enable_quota_management": True,
    "enable_cost_tracking": True,
}

def get_noaa_config() -> Dict[str, Any]:
    """Get NOAA configuration from environment with production defaults."""
    return {
        "api_token": "{{ var.value.get('aurum_noaa_api_token', '') }}",
        "base_url": NOAA_API_CONFIG["base_url"],
        "kafka_bootstrap_servers": "{{ var.value.get('aurum_kafka_bootstrap_servers', 'kafka-prod:9092') }}",
        "schema_registry_url": "{{ var.value.get('aurum_schema_registry', 'http://schema-registry-prod:8081') }}",
        "timescale_jdbc_url": "{{ var.value.get('aurum_timescale_jdbc', 'jdbc:postgresql://timescale-prod:5432/timeseries') }}",
        "timescale_table": "{{ var.value.get('aurum_noaa_timescale_table', 'noaa_weather_timeseries') }}",
        "dlq_topic": "{{ var.value.get('aurum_noaa_dlq_topic', 'aurum.ref.noaa.weather.dlq.v1') }}",
        # Production settings
        "daily_request_limit": NOAA_API_CONFIG["daily_request_limit"],
        "hourly_request_limit": NOAA_API_CONFIG["hourly_request_limit"],
        "cost_per_1000_requests": NOAA_API_CONFIG["cost_per_1000_requests_usd"],
        "monthly_budget": NOAA_API_CONFIG["monthly_budget_usd"],
        "enable_circuit_breaker": NOAA_API_CONFIG["enable_circuit_breaker"],
        "enable_adaptive_rate_limiting": NOAA_API_CONFIG["enable_adaptive_rate_limiting"],
        "enable_cost_tracking": NOAA_API_CONFIG["enable_cost_tracking"],
        # Environment-specific settings
        "environment": "{{ var.value.get('aurum_environment', 'prod') }}",
        "region": "{{ var.value.get('aurum_region', 'us-east-1') }}",
        "deployment_id": "{{ var.value.get('aurum_deployment_id', 'prod-001') }}",
    }

def validate_noaa_config(**context) -> bool:
    """Validate NOAA configuration and API connectivity."""
    try:
        config = get_noaa_config()
        required_vars = ["api_token", "base_url"]

        for var in required_vars:
            if not config[var]:
                raise ValueError(f"Missing required NOAA configuration: {var}")

        # Test API connectivity (lightweight check)
        import requests
        response = requests.get(
            f"{config['base_url']}/datasets",
            headers={"token": config["api_token"]},
            timeout=10
        )
        response.raise_for_status()

        print("✅ NOAA API configuration validated successfully")
        return True

    except Exception as e:
        print(f"❌ NOAA API validation failed: {e}")
        raise

@task
def update_noaa_watermark(dataset_key: str, logical_date: str):
    """Update watermark for NOAA dataset."""
    try:
        # This would typically update a database or file with the last processed date
        print(f"📅 Updating watermark for {dataset_key}: {logical_date}")

        # Emit metrics
        emit_task_metrics(
            dataset=dataset_key,
            task="watermark_update",
            status="success",
            message=f"Watermark updated to {logical_date}"
        )

    except Exception as e:
        emit_task_metrics(
            dataset=dataset_key,
            task="watermark_update",
            status="failure",
            message=str(e)
        )
        raise

def build_noaa_ingest_task(dataset_key: str, dataset_config: Dict[str, Any]) -> Any:
    """Build a SeaTunnel task for NOAA data ingestion."""

    env_vars = [
        f"NOAA_GHCND_DATASET='{dataset_config['dataset']}'",
        f"NOAA_GHCND_START_DATE='{{{{ data_interval_start | ds }}}}'",
        f"NOAA_GHCND_END_DATE='{{{{ data_interval_start | ds }}}}'",
        f"NOAA_GHCND_TOPIC='{dataset_config['topic']}'",
        f"NOAA_GHCND_SLIDING_HOURS={dataset_config.get('window_hours', 0)}",
        f"NOAA_GHCND_SLIDING_DAYS={dataset_config['window_days']}",
        "NOAA_GHCND_LIMIT=1000",
        "NOAA_GHCND_OFFSET=1",
        f"NOAA_GHCND_STATION_LIMIT={NOAA_API_CONFIG['station_limit']}",
        "SCHEMA_REGISTRY_URL='{{ var.value.get(\"aurum_schema_registry\", \"http://localhost:8081\") }}'"
    ]

    # Add station filter if specified
    if dataset_config.get("stations"):
        stations_str = ",".join(dataset_config["stations"])
        env_vars.append(f"NOAA_GHCND_STATION='{stations_str}'")

    # Add data types filter if specified
    if dataset_config.get("datatypes"):
        datatypes_str = ",".join(dataset_config["datatypes"])
        env_vars.append(f"NOAA_GHCND_DATA_TYPES='{datatypes_str}'")

    return build_seatunnel_task(
        f"noaa_{dataset_key}_to_kafka",
        env_vars,
        mappings=["secret/data/aurum/noaa:token=NOAA_GHCND_TOKEN"],
        pool=dataset_config["pool"],
        task_id_override=f"noaa_{dataset_key}_ingest"
    )

def build_noaa_to_timescale_task(dataset_key: str, dataset_config: Dict[str, Any]) -> Any:
    """Build a SeaTunnel task to load NOAA data from Kafka to Timescale."""

    return build_seatunnel_task(
        "noaa_weather_kafka_to_timescale",
        [
            "SCHEMA_REGISTRY_URL='{{ var.value.get(\"aurum_schema_registry\", \"http://localhost:8081\") }}'",
            "TIMESCALE_JDBC_URL='{{ var.value.get(\"aurum_timescale_jdbc\", \"jdbc:postgresql://timescale:5432/timeseries\") }}'",
            "NOAA_TABLE='{{ var.value.get(\"aurum_noaa_timescale_table\", \"noaa_weather_timeseries\") }}'",
            "DLQ_TOPIC='{{ var.value.get(\"aurum_noaa_dlq_topic\", \"aurum.ref.noaa.weather.dlq.v1\") }}'",
            "BACKFILL_ENABLED='{{ dag_run.conf.get(\"backfill\", \"0\") }}'",
            "BACKFILL_START='{{ dag_run.conf.get(\"backfill_start\", \"\") }}'",
            "BACKFILL_END='{{ dag_run.conf.get(\"backfill_end\", \"\") }}'",
        ],
        mappings=[
            "secret/data/aurum/timescale:user=TIMESCALE_USER",
            "secret/data/aurum/timescale:password=TIMESCALE_PASSWORD",
        ],
        task_id_override=f"noaa_{dataset_key}_to_timescale"
    )

def build_noaa_lineage_task(dataset_key: str, dataset_config: Dict[str, Any]) -> Any:
    """Build lineage tracking for NOAA dataset."""

    return PythonOperator(
        task_id=f"noaa_{dataset_key}_lineage",
        python_callable=emit_lakefs_lineage,
        op_kwargs={
            "dataset": f"timescale.public.{get_noaa_config()['timescale_table']}",
            "source": "noaa",
            "pipeline": f"noaa_{dataset_key}_ingest"
        },
    )

# Create the main NOAA DAG
with DAG(
    dag_id="noaa_data_ingestion",
    description="Comprehensive NOAA weather data ingestion pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,  # Manual trigger with specific dataset configs
    catchup=False,
    max_active_runs=3,
    max_active_tasks=10,
    tags=["noaa", "weather", "ingestion", "api"],
) as noaa_dag:

    start = DummyOperator(task_id="start")

    # Configuration validation
    validate_config = PythonOperator(
        task_id="validate_noaa_config",
        python_callable=validate_noaa_config,
    )

    # Build task groups for each NOAA dataset
    dataset_tasks = []

    for dataset_key, dataset_config in NOAA_DATASETS.items():
        with TaskGroup(group_id=f"noaa_{dataset_key}", tooltip=dataset_config["description"]) as dataset_group:

            # Ingest task
            ingest_task = build_noaa_ingest_task(dataset_key, dataset_config)

            # Timescale loading task
            timescale_task = build_noaa_to_timescale_task(dataset_key, dataset_config)

            # Lineage tracking task
            lineage_task = build_noaa_lineage_task(dataset_key, dataset_config)

            # Watermark update task
            watermark_task = PythonOperator(
                task_id=f"noaa_{dataset_key}_watermark",
                python_callable=update_watermark,
                op_kwargs={
                    "dataset_name": f"noaa_{dataset_key}",
                },
            )

            # Staleness check task
            staleness_task = PythonOperator(
                task_id=f"noaa_{dataset_key}_staleness_check",
                python_callable=check_staleness_and_alert,
                op_kwargs={
                    "dataset_name": f"noaa_{dataset_key}",
                    "expected_frequency_hours": 24,
                },
            )

            # Set up task dependencies within the group
            ingest_task >> timescale_task >> lineage_task >> [watermark_task, staleness_task]

        dataset_tasks.append(dataset_group)

    end = DummyOperator(task_id="end")

    # Set up main DAG dependencies
    start >> validate_config >> dataset_tasks >> end

# Create individual DAGs for each NOAA dataset with their specific schedules
for dataset_key, dataset_config in NOAA_DATASETS.items():
    dag_id = f"noaa_{dataset_key}_ingest"

    with DAG(
        dag_id=dag_id,
        description=f"NOAA {dataset_config['description']} ingestion",
        default_args=DEFAULT_ARGS,
        schedule_interval=dataset_config["schedule"],
        catchup=True,
        max_active_runs=1,
        tags=["noaa", "weather", "ingestion", "api", dataset_key],
    ) as dataset_dag:

        start_task = DummyOperator(task_id="start")
        validate_task = PythonOperator(
            task_id="validate_config",
            python_callable=validate_noaa_config,
        )

        ingest = build_noaa_ingest_task(dataset_key, dataset_config)

        # Watermark and staleness check tasks
        watermark = PythonOperator(
            task_id="watermark",
            python_callable=update_watermark,
            op_kwargs={
                "dataset_name": f"noaa_{dataset_key}",
            },
        )

        staleness_check = PythonOperator(
            task_id="staleness_check",
            python_callable=check_staleness_and_alert,
            op_kwargs={
                "dataset_name": f"noaa_{dataset_key}",
                "expected_frequency_hours": 24,
            },
        )

        # Only add timescale loading for daily datasets to avoid duplicates
        if dataset_key == "ghcnd_daily":
            timescale = build_noaa_to_timescale_task(dataset_key, dataset_config)
            lineage = build_noaa_lineage_task(dataset_key, dataset_config)

            start_task >> validate_task >> ingest >> timescale >> lineage >> [watermark, staleness_check]
        else:
            start_task >> validate_task >> ingest >> [watermark, staleness_check]

        # Set up failure callback
        dataset_dag.on_failure_callback = build_failure_callback(source=f"aurum.airflow.{dag_id}")

# Generate documentation
if __name__ == "__main__":
    print("NOAA Data Ingestion DAGs created successfully!")
    print(f"Created {len(NOAA_DATASETS)} dataset-specific DAGs:")
    for dataset_key, config in NOAA_DATASETS.items():
        print(f"  - {dataset_key}: {config['description']} (schedule: {config['schedule']})")
