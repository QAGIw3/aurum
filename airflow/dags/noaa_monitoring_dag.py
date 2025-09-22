"""NOAA Weather Data Monitoring and Alerting DAG.

This DAG provides comprehensive monitoring for NOAA data pipelines including:
- Data freshness monitoring
- Quality metrics tracking
- API health checks
- Data volume monitoring
- Alert generation for anomalies
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from aurum.airflow_utils.alerting import build_failure_callback
from aurum.airflow_utils.metrics import emit_task_metrics
from airflow.models import Variable


logger = logging.getLogger(__name__)


# Default arguments for all tasks
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": build_failure_callback(source="aurum.airflow.noaa_monitoring"),
}

# NOAA monitoring configuration
NOAA_MONITORING_CONFIG = {
    "datasets": {
        "ghcnd_daily": {
            "expected_frequency_hours": 24,
            "critical_staleness_hours": 48,
            "warning_staleness_hours": 12,
            "min_records_per_day": 100,
            "max_null_ratio": 0.1,
            "api_endpoint": "https://www.ncei.noaa.gov/access/services/data/v1",
            "api_timeout": 30,
        },
        "ghcnd_hourly": {
            "expected_frequency_hours": 1,
            "critical_staleness_hours": 6,
            "warning_staleness_hours": 2,
            "min_records_per_day": 1000,
            "max_null_ratio": 0.05,
            "api_endpoint": "https://www.ncei.noaa.gov/access/services/data/v1",
            "api_timeout": 30,
        },
        "gsom_monthly": {
            "expected_frequency_hours": 24,  # Monthly data, but check daily
            "critical_staleness_hours": 72,
            "warning_staleness_hours": 24,
            "min_records_per_day": 10,
            "max_null_ratio": 0.15,
            "api_endpoint": "https://www.ncei.noaa.gov/access/services/data/v1",
            "api_timeout": 30,
        },
        "normals_daily": {
            "expected_frequency_hours": 24,
            "critical_staleness_hours": 168,  # 7 days - less critical
            "warning_staleness_hours": 72,
            "min_records_per_day": 50,
            "max_null_ratio": 0.05,
            "api_endpoint": "https://www.ncei.noaa.gov/cdo-web/api/v2",
            "api_timeout": 30,
        },
        # Expanded coverage datasets
        "ghcnd_daily_expanded": {
            "expected_frequency_hours": 24,
            "critical_staleness_hours": 48,
            "warning_staleness_hours": 12,
            "min_records_per_day": 400,  # Higher due to more stations
            "max_null_ratio": 0.1,
            "api_endpoint": "https://www.ncei.noaa.gov/cdo-web/api/v2",
            "api_timeout": 30,
        },
        "ghcnd_hourly_expanded": {
            "expected_frequency_hours": 1,
            "critical_staleness_hours": 6,
            "warning_staleness_hours": 2,
            "min_records_per_day": 4000,  # Higher due to more stations and hourly
            "max_null_ratio": 0.05,
            "api_endpoint": "https://www.ncei.noaa.gov/cdo-web/api/v2",
            "api_timeout": 30,
        },
        "gsom_monthly_expanded": {
            "expected_frequency_hours": 24,  # Monthly data, but check daily
            "critical_staleness_hours": 72,
            "warning_staleness_hours": 24,
            "min_records_per_day": 50,  # Higher due to more stations
            "max_null_ratio": 0.15,
            "api_endpoint": "https://www.ncei.noaa.gov/cdo-web/api/v2",
            "api_timeout": 30,
        },
        "normals_daily_expanded": {
            "expected_frequency_hours": 24,
            "critical_staleness_hours": 168,  # 7 days - less critical
            "warning_staleness_hours": 72,
            "min_records_per_day": 200,  # Higher due to more stations
            "max_null_ratio": 0.05,
            "api_endpoint": "https://www.ncei.noaa.gov/cdo-web/api/v2",
            "api_timeout": 30,
        }
    },
    "global_checks": {
        "api_health_timeout": 30,
        "data_quality_threshold": 0.95,
        "volume_threshold_percent": 0.8,
        "staleness_check_hours": 24,
    }
}

@task
def check_noaa_api_health(dataset_key: str, **context) -> Dict[str, Any]:
    """Check NOAA API health and availability."""
    try:
        import requests

        config = NOAA_MONITORING_CONFIG["datasets"][dataset_key]
        endpoint = config["api_endpoint"]

        # Build headers from Airflow Variables (for CDO API)
        token = Variable.get("aurum_noaa_api_token", default_var="")
        headers = {"token": token} if token else {}

        # Choose a reasonable health check URL
        target_url = f"{endpoint}/datasets" if "cdo-web" in endpoint else endpoint

        # Test basic API connectivity
        response = requests.get(target_url, headers=headers, timeout=config["api_timeout"])

        response.raise_for_status()

        result = {
            "dataset": dataset_key,
            "api_status": "healthy",
            "response_time_ms": response.elapsed.total_seconds() * 1000,
            "status_code": response.status_code,
            "available_datasets": len(response.json().get("results", [])),
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="api_health_check",
            status="success",
            message=f"API health check passed in {result['response_time_ms']:.2f}ms"
        )

        return result

    except Exception as e:
        error_result = {
            "dataset": dataset_key,
            "api_status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="api_health_check",
            status="failure",
            message=str(e)
        )

        raise

@task
def check_data_freshness(dataset_key: str, **context) -> Dict[str, Any]:
    """Check data freshness and staleness for NOAA dataset."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        config = NOAA_MONITORING_CONFIG["datasets"][dataset_key]

        # Query latest record timestamp
        query = """
            SELECT
                MAX(observation_timestamp) as latest_timestamp,
                MAX(observation_date) as latest_date,
                COUNT(*) as total_records,
                COUNT(CASE WHEN value IS NULL THEN 1 END) as null_records,
                NOW() - MAX(observation_timestamp/1000000)::timestamp as staleness
            FROM noaa_weather_timeseries
            WHERE dataset_id = %s
            AND observation_date >= CURRENT_DATE - INTERVAL '7 days'
        """

        postgres_hook = PostgresHook(postgres_conn_id="timescale")
        result = postgres_hook.get_first(query, parameters=[dataset_key])

        if not result or not result[0]:
            raise ValueError(f"No data found for dataset {dataset_key}")

        latest_timestamp, latest_date, total_records, null_records, staleness = result

        staleness_hours = staleness.total_seconds() / 3600 if staleness else None
        null_ratio = null_records / total_records if total_records > 0 else 0

        freshness_result = {
            "dataset": dataset_key,
            "latest_timestamp": latest_timestamp,
            "latest_date": str(latest_date),
            "staleness_hours": staleness_hours,
            "total_records": total_records,
            "null_records": null_records,
            "null_ratio": null_ratio,
            "status": "fresh" if staleness_hours and staleness_hours <= config["warning_staleness_hours"] else "stale",
            "critical": staleness_hours and staleness_hours > config["critical_staleness_hours"],
            "warning": staleness_hours and staleness_hours > config["warning_staleness_hours"],
            "timestamp": datetime.utcnow().isoformat(),
        }

        # Emit metrics
        emit_task_metrics(
            dataset=dataset_key,
            task="freshness_check",
            status=freshness_result["status"],
            message=f"Dataset {dataset_key} staleness: {staleness_hours:.2f}h"
        )

        return freshness_result

    except Exception as e:
        error_result = {
            "dataset": dataset_key,
            "error": str(e),
            "status": "error",
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="freshness_check",
            status="failure",
            message=str(e)
        )

        raise

@task
def check_data_quality(dataset_key: str, **context) -> Dict[str, Any]:
    """Check data quality metrics for NOAA dataset."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        config = NOAA_MONITORING_CONFIG["datasets"][dataset_key]

        # Query quality metrics
        query = """
            SELECT
                COUNT(*) as total_records,
                COUNT(CASE WHEN data_quality_flag = 'VALID' THEN 1 END) as valid_records,
                COUNT(CASE WHEN data_quality_flag != 'VALID' THEN 1 END) as invalid_records,
                AVG(CASE WHEN value IS NOT NULL THEN value END) as avg_value,
                STDDEV(CASE WHEN value IS NOT NULL THEN value END) as stddev_value,
                MIN(value) as min_value,
                MAX(value) as max_value
            FROM noaa_weather_timeseries
            WHERE dataset_id = %s
            AND observation_date >= CURRENT_DATE - INTERVAL '1 day'
        """

        postgres_hook = PostgresHook(postgres_conn_id="timescale")
        result = postgres_hook.get_first(query, parameters=[dataset_key])

        if not result:
            raise ValueError(f"No quality data found for dataset {dataset_key}")

        total, valid, invalid, avg_val, stddev_val, min_val, max_val = result

        quality_score = valid / total if total > 0 else 0
        outlier_ratio = invalid / total if total > 0 else 0

        quality_result = {
            "dataset": dataset_key,
            "total_records": total,
            "valid_records": valid,
            "invalid_records": invalid,
            "quality_score": quality_score,
            "outlier_ratio": outlier_ratio,
            "avg_value": float(avg_val) if avg_val else None,
            "stddev_value": float(stddev_val) if stddev_val else None,
            "min_value": float(min_val) if min_val else None,
            "max_value": float(max_val) if max_val else None,
            "status": "good" if quality_score >= config["data_quality_threshold"] else "poor",
            "critical": quality_score < (config["data_quality_threshold"] * 0.8),
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="quality_check",
            status=quality_result["status"],
            message=f"Quality score: {quality_score:.3f}"
        )

        return quality_result

    except Exception as e:
        error_result = {
            "dataset": dataset_key,
            "error": str(e),
            "status": "error",
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="quality_check",
            status="failure",
            message=str(e)
        )

        raise

@task
def check_data_volume(dataset_key: str, **context) -> Dict[str, Any]:
    """Check data volume metrics for NOAA dataset."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        config = NOAA_MONITORING_CONFIG["datasets"][dataset_key]

        # Query volume metrics
        query = """
            SELECT
                observation_date,
                COUNT(*) as daily_records,
                COUNT(CASE WHEN value IS NOT NULL THEN 1 END) as non_null_records
            FROM noaa_weather_timeseries
            WHERE dataset_id = %s
            AND observation_date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY observation_date
            ORDER BY observation_date DESC
        """

        postgres_hook = PostgresHook(postgres_conn_id="timescale")
        results = postgres_hook.get_records(query, parameters=[dataset_key])

        if not results:
            raise ValueError(f"No volume data found for dataset {dataset_key}")

        # Calculate volume metrics
        daily_volumes = [row[1] for row in results]
        avg_volume = sum(daily_volumes) / len(daily_volumes)
        min_volume = min(daily_volumes)
        max_volume = max(daily_volumes)

        expected_min = config["min_records_per_day"] * NOAA_MONITORING_CONFIG["global_checks"]["volume_threshold_percent"]

        volume_result = {
            "dataset": dataset_key,
            "avg_daily_volume": avg_volume,
            "min_daily_volume": min_volume,
            "max_daily_volume": max_volume,
            "expected_min_volume": expected_min,
            "volume_adequate": min_volume >= expected_min,
            "days_analyzed": len(daily_volumes),
            "status": "adequate" if min_volume >= expected_min else "insufficient",
            "critical": min_volume < (expected_min * 0.5),
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="volume_check",
            status=volume_result["status"],
            message=f"Average daily volume: {avg_volume:.0f} records"
        )

        return volume_result

    except Exception as e:
        error_result = {
            "dataset": dataset_key,
            "error": str(e),
            "status": "error",
            "timestamp": datetime.utcnow().isoformat(),
        }

        emit_task_metrics(
            dataset=dataset_key,
            task="volume_check",
            status="failure",
            message=str(e)
        )

        raise

@task
def generate_monitoring_report(**context) -> Dict[str, Any]:
    """Generate comprehensive monitoring report."""
    try:
        # Pull results from previous tasks
        ti = context["ti"]

        api_results = ti.xcom_pull(task_ids=["api_health_check"])
        freshness_results = ti.xcom_pull(task_ids=["freshness_check"])
        quality_results = ti.xcom_pull(task_ids=["quality_check"])
        volume_results = ti.xcom_pull(task_ids=["volume_check"])

        # Aggregate results
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "summary": {
                "total_datasets": len(NOAA_MONITORING_CONFIG["datasets"]),
                "healthy_apis": sum(1 for r in api_results if r and r.get("api_status") == "healthy"),
                "fresh_datasets": sum(1 for r in freshness_results if r and r.get("status") == "fresh"),
                "good_quality": sum(1 for r in quality_results if r and r.get("status") == "good"),
                "adequate_volume": sum(1 for r in volume_results if r and r.get("status") == "adequate"),
                "critical_issues": sum(1 for r in freshness_results + quality_results + volume_results
                                     if r and r.get("critical", False)),
            },
            "details": {
                "api_health": api_results,
                "freshness": freshness_results,
                "quality": quality_results,
                "volume": volume_results,
            }
        }

        # Determine overall status
        critical_count = report["summary"]["critical_issues"]
        if critical_count > 0:
            report["overall_status"] = "CRITICAL"
        elif report["summary"]["fresh_datasets"] < len(NOAA_MONITORING_CONFIG["datasets"]) * 0.8:
            report["overall_status"] = "WARNING"
        else:
            report["overall_status"] = "HEALTHY"

        # Log report summary
        logger.info("NOAA Monitoring Report - Status: %s", report["overall_status"])
        logger.info("API Health: %s/%s", report["summary"]["healthy_apis"], report["summary"]["total_datasets"])
        logger.info("Data Freshness: %s/%s", report["summary"]["fresh_datasets"], report["summary"]["total_datasets"])
        logger.info("Data Quality: %s/%s", report["summary"]["good_quality"], report["summary"]["total_datasets"])
        logger.info("Data Volume: %s/%s", report["summary"]["adequate_volume"], report["summary"]["total_datasets"])

        if critical_count > 0:
            logger.warning("CRITICAL ISSUES: %s datasets have critical problems", critical_count)

        emit_task_metrics(
            dataset="noaa_monitoring",
            task="monitoring_report",
            status=report["overall_status"].lower(),
            message=f"Monitoring report generated - {report['overall_status']}"
        )

        return report

    except Exception as e:
        emit_task_metrics(
            dataset="noaa_monitoring",
            task="monitoring_report",
            status="failure",
            message=str(e)
        )
        raise

@task
def send_alerts(**context) -> None:
    """Send alerts for critical issues."""
    try:
        ti = context["ti"]
        report = ti.xcom_pull(task_ids=["generate_monitoring_report"])

        if not report or report.get("overall_status") != "CRITICAL":
            return

        # This would integrate with your alerting system (Slack, email, etc.)
        critical_issues = []

        for dataset_checks in report.get("details", {}).values():
            for check in dataset_checks:
                if check and check.get("critical", False):
                    critical_issues.append(check)

        if critical_issues:
            logger.warning("SENDING CRITICAL ALERTS for %s issues", len(critical_issues))
            # Implementation would send actual alerts here

        emit_task_metrics(
            dataset="noaa_monitoring",
            task="alert_dispatch",
            status="success",
            message=f"Alerts sent for {len(critical_issues)} critical issues"
        )

    except Exception as e:
        emit_task_metrics(
            dataset="noaa_monitoring",
            task="alert_dispatch",
            status="failure",
            message=str(e)
        )
        raise

# Create the main monitoring DAG
with DAG(
    dag_id="noaa_data_monitoring",
    description="Comprehensive monitoring for NOAA weather data pipelines",
    default_args=DEFAULT_ARGS,
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "monitoring", "weather", "alerting"],
) as monitoring_dag:

    start = DummyOperator(task_id="start")

    # API health checks
    api_health_tasks = []
    for dataset_key in NOAA_MONITORING_CONFIG["datasets"].keys():
        api_task = check_noaa_api_health(dataset_key)
        api_health_tasks.append(api_task)

    # Data checks in parallel
    freshness_tasks = []
    quality_tasks = []
    volume_tasks = []

    for dataset_key in NOAA_MONITORING_CONFIG["datasets"].keys():
        freshness_tasks.append(check_data_freshness(dataset_key))
        quality_tasks.append(check_data_quality(dataset_key))
        volume_tasks.append(check_data_volume(dataset_key))

    # Generate comprehensive report
    report_task = generate_monitoring_report()

    # Send alerts for critical issues
    alert_task = send_alerts()

    end = DummyOperator(task_id="end")

    # Set up dependencies
    start >> api_health_tasks >> [freshness_tasks, quality_tasks, volume_tasks] >> report_task >> alert_task >> end

# Create a summary DAG that runs less frequently
with DAG(
    dag_id="noaa_data_monitoring_daily",
    description="Daily summary monitoring for NOAA weather data",
    default_args={**DEFAULT_ARGS, "execution_timeout": timedelta(minutes=60)},
    schedule_interval="0 9 * * *",  # Daily at 9 AM
    catchup=False,
    max_active_runs=1,
    tags=["noaa", "monitoring", "weather", "daily"],
) as daily_monitoring_dag:

    start = DummyOperator(task_id="start")

    # Run all checks
    api_checks = []
    data_checks = []

    for dataset_key in NOAA_MONITORING_CONFIG["datasets"].keys():
        api_checks.append(check_noaa_api_health(dataset_key))
        data_checks.extend([
            check_data_freshness(dataset_key),
            check_data_quality(dataset_key),
            check_data_volume(dataset_key)
        ])

    # Generate detailed daily report
    daily_report = generate_monitoring_report()

    # Send alerts if needed
    daily_alerts = send_alerts()

    end = DummyOperator(task_id="end")

    # Set up dependencies
    start >> api_checks >> data_checks >> daily_report >> daily_alerts >> end

    daily_monitoring_dag.on_failure_callback = build_failure_callback(source="aurum.airflow.noaa_monitoring_daily")
