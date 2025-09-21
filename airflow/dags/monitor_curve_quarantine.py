"""Daily monitor for quarantined curve rows using Trino."""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from pathlib import Path
import subprocess

DEFAULT_ARGS = {
    "owner": "platform-ops",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

REPO_ROOT = Path(__file__).resolve().parents[2]


def run_monitor(**context):
    env = os.environ.copy()
    threshold = env.get("AURUM_QUARANTINE_THRESHOLD", "500")
    cmd = [
        "python",
        str(REPO_ROOT / "scripts" / "ops" / "monitor_quarantine.py"),
        "--threshold",
        threshold,
    ]
    print(f"Running quarantine monitor: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT, env=env)


with DAG(
    dag_id="monitor_curve_quarantine",
    description="Alert on spikes in curve quarantine volume",
    default_args=DEFAULT_ARGS,
    schedule_interval=os.getenv("AURUM_QUARANTINE_MONITOR_SCHEDULE", "0 6 * * *"),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    tags=["monitoring", "curves"],
) as dag:
    PythonOperator(
        task_id="check_quarantine_counts",
        python_callable=run_monitor,
    )

__all__ = ["dag"]
