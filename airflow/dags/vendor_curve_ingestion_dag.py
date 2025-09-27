"""Enhanced Airflow DAG for vendor curve ingestion using new parser operators.

Phase 2: Vendor Parser-Airflow Integration
This DAG replaces the previous monolithic parsing approach with modular operators
for better monitoring, error handling, and maintainability.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Import the new parser operators
import sys
src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if src_path and src_path not in sys.path:
    sys.path.insert(0, src_path)

from aurum.airflow_utils.parser_operators import (
    VendorCurveParsingOperator,
    VendorCurveValidationOperator,
    VendorCurveMonitoringOperator
)

DEFAULT_ARGS: Dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# Configuration for different vendors
VENDOR_CONFIGS = {
    "PW": {
        "file_pattern": "EOD_PW_*.xlsx",
        "validation_rules": {
            "min_rows": {"threshold": 100},
            "max_null_percentage": {"threshold": 5}
        },
        "performance_thresholds": {
            "max_processing_time_minutes": 15,
            "min_success_rate_percentage": 98,
            "max_quarantine_rate_percentage": 2
        }
    },
    "EUGP": {
        "file_pattern": "EOD_EUGP_*.xlsx",
        "validation_rules": {
            "min_rows": {"threshold": 50},
            "max_null_percentage": {"threshold": 8}
        },
        "performance_thresholds": {
            "max_processing_time_minutes": 10,
            "min_success_rate_percentage": 95,
            "max_quarantine_rate_percentage": 5
        }
    },
    "RP": {
        "file_pattern": "EOD_RP_*.xlsx",
        "validation_rules": {
            "min_rows": {"threshold": 75},
            "max_null_percentage": {"threshold": 6}
        },
        "performance_thresholds": {
            "max_processing_time_minutes": 12,
            "min_success_rate_percentage": 96,
            "max_quarantine_rate_percentage": 4
        }
    },
    "SIMPLE": {
        "file_pattern": "EOD_SIMPLE_*.xlsx",
        "validation_rules": {
            "min_rows": {"threshold": 25},
            "max_null_percentage": {"threshold": 10}
        },
        "performance_thresholds": {
            "max_processing_time_minutes": 8,
            "min_success_rate_percentage": 94,
            "max_quarantine_rate_percentage": 6
        }
    }
}


def create_lakefs_branch(**context: Any) -> None:
    """Create lakeFS branch for the ingestion run."""
    execution_date = context["ds"]
    branch_name = f"vendor_curves_{execution_date.replace('-', '')}"
    repo = os.environ.get("AURUM_LAKEFS_REPO")
    
    if not repo:
        print("lakeFS repo not configured; skipping branch creation")
        return
    
    source_branch = os.environ.get("AURUM_LAKEFS_SOURCE_BRANCH", "main")
    
    try:
        from aurum.lakefs_client import ensure_branch  # type: ignore
        ensure_branch(repo, branch_name, source_branch)
        os.environ["AURUM_LAKEFS_BRANCH"] = branch_name
        print(f"Created lakeFS branch {branch_name} from {source_branch}")
    except Exception as exc:  # pragma: no cover
        print(f"lakeFS branch creation failed: {exc}")


def consolidate_results(**context: Any) -> Dict[str, Any]:
    """Consolidate results from all vendor parsing tasks."""
    consolidated = {
        "execution_date": context["ds"],
        "vendors": {},
        "total_files_processed": 0,
        "total_rows_parsed": 0,
        "total_rows_quarantined": 0,
        "overall_status": "success"
    }
    
    for vendor in VENDOR_CONFIGS.keys():
        # Get results from parsing task
        parsing_result = context['ti'].xcom_pull(task_ids=f'parse_{vendor.lower()}_curves')
        validation_result = context['ti'].xcom_pull(task_ids=f'validate_{vendor.lower()}_curves')
        monitoring_result = context['ti'].xcom_pull(task_ids=f'monitor_{vendor.lower()}_curves')
        
        if parsing_result:
            consolidated["vendors"][vendor] = {
                "parsing": parsing_result,
                "validation": validation_result,
                "monitoring": monitoring_result
            }
            
            consolidated["total_files_processed"] += parsing_result.get("files_processed", 0)
            consolidated["total_rows_parsed"] += parsing_result.get("rows_parsed", 0)
            consolidated["total_rows_quarantined"] += parsing_result.get("rows_quarantined", 0)
            
            # Check if any vendor failed critically
            if (parsing_result.get("status") == "quarantined" or 
                (monitoring_result and monitoring_result.get("status") == "critical")):
                consolidated["overall_status"] = "partial_failure"
    
    print(f"Consolidation complete: {consolidated['total_rows_parsed']} rows parsed, "
          f"{consolidated['total_rows_quarantined']} quarantined")
    
    return consolidated


def publish_metrics(**context: Any) -> None:
    """Publish consolidated metrics to monitoring systems."""
    consolidated = context['ti'].xcom_pull(task_ids='consolidate_results')
    
    if not consolidated:
        print("No consolidated results found")
        return
    
    try:
        from prometheus_client import Counter, Histogram, Gauge
        
        # Metrics
        files_processed = Counter('aurum_vendor_files_processed_total', 'Total vendor files processed', ['vendor'])
        rows_parsed = Counter('aurum_vendor_rows_parsed_total', 'Total vendor rows parsed', ['vendor'])
        rows_quarantined = Counter('aurum_vendor_rows_quarantined_total', 'Total vendor rows quarantined', ['vendor'])
        processing_duration = Histogram('aurum_vendor_processing_duration_seconds', 'Vendor processing duration', ['vendor'])
        
        for vendor, results in consolidated["vendors"].items():
            parsing = results.get("parsing", {})
            monitoring = results.get("monitoring", {})
            
            files_processed.labels(vendor=vendor).inc(parsing.get("files_processed", 0))
            rows_parsed.labels(vendor=vendor).inc(parsing.get("rows_parsed", 0))
            rows_quarantined.labels(vendor=vendor).inc(parsing.get("rows_quarantined", 0))
            
            if monitoring and "processing_time_minutes" in monitoring.get("metrics", {}):
                processing_duration.labels(vendor=vendor).observe(
                    monitoring["metrics"]["processing_time_minutes"] * 60
                )
        
        print("Metrics published successfully")
        
    except ImportError:
        print("Prometheus client not available; skipping metrics publication")
    except Exception as exc:
        print(f"Failed to publish metrics: {exc}")


# Create the DAG
dag = DAG(
    "vendor_curve_ingestion_enhanced",
    default_args=DEFAULT_ARGS,
    description="Enhanced vendor curve ingestion with modular operators",
    schedule_interval="0 12 * * 1-5",  # Daily at 12:00 UTC, weekdays only
    catchup=False,
    tags=["vendor", "curves", "ingestion", "enhanced"],
    max_active_runs=1,
)

# Start task
start = EmptyOperator(
    task_id="start",
    dag=dag
)

# Create lakeFS branch
create_branch = PythonOperator(
    task_id="create_lakefs_branch",
    python_callable=create_lakefs_branch,
    dag=dag
)

# Create parsing, validation, and monitoring tasks for each vendor
vendor_tasks = {}
for vendor, config in VENDOR_CONFIGS.items():
    vendor_lower = vendor.lower()
    
    # Parsing task
    parse_task = VendorCurveParsingOperator(
        task_id=f"parse_{vendor_lower}_curves",
        vendor=vendor,
        file_pattern=config["file_pattern"],
        output_format="parquet",
        quarantine_on_error=True,
        dag=dag
    )
    
    # Validation task
    validate_task = VendorCurveValidationOperator(
        task_id=f"validate_{vendor_lower}_curves",
        vendor=vendor,
        input_path=f"/opt/airflow/data/processed/{vendor_lower}_{{{{ ds }}}}.parquet",
        validation_rules=config["validation_rules"],
        dag=dag
    )
    
    # Monitoring task
    monitor_task = VendorCurveMonitoringOperator(
        task_id=f"monitor_{vendor_lower}_curves",
        vendor=vendor,
        performance_thresholds=config["performance_thresholds"],
        alert_channels=['log', 'metrics'],
        dag=dag
    )
    
    # Set up dependencies for this vendor
    parse_task >> validate_task >> monitor_task
    
    vendor_tasks[vendor] = {
        'parse': parse_task,
        'validate': validate_task,
        'monitor': monitor_task
    }

# Consolidation and metrics tasks
consolidate = PythonOperator(
    task_id="consolidate_results",
    python_callable=consolidate_results,
    dag=dag
)

publish_metrics_task = PythonOperator(
    task_id="publish_metrics",
    python_callable=publish_metrics,
    dag=dag
)

# End task
end = EmptyOperator(
    task_id="end",
    dag=dag
)

# Set up overall DAG dependencies
start >> create_branch

# Connect create_branch to all parse tasks
for tasks in vendor_tasks.values():
    create_branch >> tasks['parse']

# Connect all monitor tasks to consolidation
monitor_tasks = [tasks['monitor'] for tasks in vendor_tasks.values()]
monitor_tasks >> consolidate >> publish_metrics_task >> end