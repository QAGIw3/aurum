"""Airflow DAG for warming Trino query cache with common analytical queries."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from aurum.airflow_utils import build_failure_callback, emit_alert, metrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DAG configuration
DAG_ID = "cache_warming"
SCHEDULE_INTERVAL = "0 */4 * * *"  # Every 4 hours
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Cache warming configuration
TRINO_HOST = "trino-coordinator.trino.svc.cluster.local"
TRINO_PORT = 8080
CACHE_WARMER_SCRIPT = "/opt/aurum/scripts/trino/cache_warmer.py"


def run_cache_warmer(mode: str = "warm", **context) -> dict:
    """Execute cache warming with specified mode."""
    import subprocess
    import sys
    from pathlib import Path

    try:
        # Build command
        cmd = [
            sys.executable,
            CACHE_WARMER_SCRIPT,
            "--host", TRINO_HOST,
            "--port", str(TRINO_PORT),
            "--mode", mode,
            "--verbose"
        ]

        logger.info(f"Executing cache warming command: {' '.join(cmd)}")

        # Execute cache warming
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )

        if result.returncode != 0:
            error_msg = f"Cache warming failed with exit code {result.returncode}: {result.stderr}"
            logger.error(error_msg)
            emit_alert(
                "ERROR",
                f"Cache warming DAG failed: {error_msg}",
                source=f"aurum.airflow.{DAG_ID}",
                context={"exit_code": result.returncode}
            )
            raise RuntimeError(error_msg)

        # Parse output for metrics
        output = result.stdout
        logger.info(f"Cache warming completed: {output}")

        # Extract metrics from output
        metrics_data = parse_cache_warmer_output(output)

        # Emit metrics
        metric_tags = {"mode": mode, "status": "success"}
        metrics.increment("cache_warming.runs", tags=metric_tags)
        metrics.gauge("cache_warming.execution_time", metrics_data.get("total_time", 0), tags=metric_tags)
        metrics.gauge("cache_warming.queries_executed", metrics_data.get("total_queries", 0), tags=metric_tags)
        metrics.gauge("cache_warming.rows_processed", metrics_data.get("total_rows", 0), tags=metric_tags)

        return metrics_data

    except subprocess.TimeoutExpired:
        error_msg = f"Cache warming timed out after {3600} seconds"
        logger.error(error_msg)
        emit_alert(
            "ERROR",
            error_msg,
            source=f"aurum.airflow.{DAG_ID}",
            context={"timeout_seconds": 3600}
        )
        raise RuntimeError(error_msg)
    except Exception as e:
        logger.error(f"Cache warming failed: {e}")
        emit_alert(
            "ERROR",
            f"Cache warming failed: {str(e)}",
            source=f"aurum.airflow.{DAG_ID}",
            context={"error": str(e)}
        )
        raise


def parse_cache_warming_output(output: str) -> dict:
    """Parse cache warmer output to extract metrics."""
    metrics_data = {
        "total_queries": 0,
        "successful_queries": 0,
        "failed_queries": 0,
        "total_time": 0.0,
        "total_rows": 0
    }

    try:
        # Parse key metrics from output
        lines = output.split('\n')

        for line in lines:
            if "Total queries executed:" in line:
                metrics_data["total_queries"] = int(line.split(":")[-1].strip())
            elif "Successful queries:" in line:
                metrics_data["successful_queries"] = int(line.split(":")[-1].strip())
            elif "Failed queries:" in line:
                metrics_data["failed_queries"] = int(line.split(":")[-1].strip())
            elif "Total execution time:" in line:
                time_str = line.split(":")[-1].strip()
                metrics_data["total_time"] = float(time_str.replace('s', ''))
            elif "Total rows processed:" in line:
                metrics_data["total_rows"] = int(line.split(":")[-1].strip())

    except Exception as e:
        logger.warning(f"Failed to parse cache warmer output: {e}")

    return metrics_data


# Create DAG
dag = DAG(
    DAG_ID,
    description="Warm Trino query cache with common analytical queries",
    default_args=DEFAULT_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=False,
    dagrun_timeout=timedelta(hours=2),
    tags=["trino", "cache", "analytics", "performance"],
    max_active_runs=1,  # Only one instance at a time
)

# Cache warming tasks
warm_cache_task = PythonOperator(
    task_id="warm_analytical_cache",
    python_callable=run_cache_warmer,
    op_kwargs={"mode": "warm"},
    dag=dag,
)

refresh_views_task = PythonOperator(
    task_id="refresh_materialized_views",
    python_callable=run_cache_warmer,
    op_kwargs={"mode": "refresh"},
    dag=dag,
)

# Health check task
health_check_task = BashOperator(
    task_id="cache_warming_health_check",
    bash_command="""
    #!/bin/bash
    echo "Checking cache warming health..."

    # Check if materialized views exist and are fresh
    trino --server {{ params.trino_host }}:{{ params.trino_port }} \
         --catalog iceberg \
         --schema market \
         --execute "
         SELECT
           'curve_price_daily_summary' as view_name,
           COUNT(*) as row_count
         FROM iceberg.market.curve_price_daily_summary
         WHERE asof_date >= CURRENT_DATE - INTERVAL '1' DAY
         UNION ALL
         SELECT
           'external_data_summary' as view_name,
           COUNT(*) as row_count
         FROM iceberg.market.external_data_summary
         WHERE asof_date >= CURRENT_DATE - INTERVAL '1' DAY
         "

    echo "✅ Cache warming health check completed"
    """,
    params={
        "trino_host": TRINO_HOST.replace("-coordinator.trino.svc.cluster.local", ""),
        "trino_port": TRINO_PORT
    },
    dag=dag,
)

# Task dependencies
warm_cache_task >> refresh_views_task >> health_check_task

# Set up failure callback
dag.on_failure_callback = build_failure_callback(source=f"aurum.airflow.{DAG_ID}")

__all__ = ["dag"]
