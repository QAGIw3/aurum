#!/usr/bin/env python3
"""Generate dynamic Airflow DAG from configuration files."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.airflow_factory import create_dynamic_ingestion_dag


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate dynamic Airflow DAG from configuration files")
    parser.add_argument("--config-files", nargs="+", required=True,
                       help="Configuration file paths (e.g., config/eia_ingest_datasets.json)")
    parser.add_argument("--dag-id", default="dynamic_ingestion",
                       help="DAG identifier (default: dynamic_ingestion)")
    parser.add_argument("--output", type=Path, default=None,
                       help="Output DAG file path (default: stdout)")
    parser.add_argument("--schedule", default="0 6 * * *",
                       help="DAG schedule interval (default: 0 6 * * *)")
    parser.add_argument("--max-active-runs", type=int, default=1,
                       help="Maximum active DAG runs (default: 1)")
    parser.add_argument("--catchup", action="store_true",
                       help="Enable catchup (default: False)")

    args = parser.parse_args()

    try:
        # Create the DAG
        dag = create_dynamic_ingestion_dag(
            config_files=args.config_files,
            dag_id=args.dag_id
        )

        # Generate DAG file content
        dag_content = f"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable, metrics

# Generated DAG from {', '.join(args.config_files)}
# DO NOT EDIT MANUALLY - Regenerate with: python3 scripts/airflow/generate_dynamic_dag.py

DEFAULT_ARGS = {{
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=60),
    "execution_timeout": timedelta(minutes=45),
}}

with DAG(
    dag_id="{args.dag_id}",
    description="Dynamic data ingestion DAG generated from configuration",
    default_args=DEFAULT_ARGS,
    schedule_interval="{args.schedule}",
    start_date=datetime(2024, 1, 1),
    catchup={str(args.catchup).lower()},
    max_active_runs={args.max_active_runs},
    tags=["aurum", "ingestion", "dynamic"],
) as dag:

    # Create tasks based on configuration files
    # This DAG is dynamically generated from:
    # {chr(10).join([f'#   - {f}' for f in args.config_files])}

    # TODO: Implement task creation logic here
    # For now, create placeholder tasks

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # Placeholder task - replace with actual dynamic task creation
    placeholder = EmptyOperator(task_id="dynamic_ingestion_placeholder")

    start >> placeholder >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.{args.dag_id}")
"""

        # Write to file or stdout
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(dag_content)
            print(f"Generated DAG file: {args.output}")
        else:
            print(dag_content)

        print("
📋 DAG Configuration:"        print(f"  • DAG ID: {args.dag_id}")
        print(f"  • Schedule: {args.schedule}")
        print(f"  • Max Active Runs: {args.max_active_runs}")
        print(f"  • Catchup: {args.catchup}")
        print(f"  • Config Files: {len(args.config_files)} files")
        for i, config_file in enumerate(args.config_files, 1):
            print(f"    {i}. {config_file}")

        print("
🚀 Next Steps:"        print("  1. Review the generated DAG file")
        print("  2. Test with: airflow dags test {args.dag_id}")
        print("  3. Deploy to Airflow environment")
        print("  4. Monitor with: airflow dags list | grep {args.dag_id}")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
