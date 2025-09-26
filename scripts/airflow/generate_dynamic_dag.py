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

# Note: dynamic generation below does not require aurum.airflow_factory.


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
        # Load datasets from config files (minimal schema: {"datasets": [{...}]})
        import json
        from typing import Any, Dict, List

        datasets: List[Dict[str, Any]] = []
        for cfg_path in args.config_files:
            raw = json.loads(Path(cfg_path).read_text(encoding="utf-8"))
            items = raw.get("datasets", [])
            if isinstance(items, list):
                datasets.extend(items)

        # Consider only ISO-related entries when present
        iso_like = [
            ds for ds in datasets
            if isinstance(ds, dict) and (
                ds.get("type") in {"iso_price", "iso_metrics", "iso"} or ds.get("iso")
            )
        ]

        cfg_list_literal = ",\n        ".join([json.dumps(ds, ensure_ascii=False) for ds in iso_like])

        # Generate DAG file content using a placeholder template to avoid f-string brace issues
        files_list = "\n".join([f"#   - {p}" for p in args.config_files])
        tmpl = r"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.airflow_utils import build_failure_callback, build_preflight_callable
from aurum.airflow_utils import iso as iso_utils

# Generated DAG from:
__FILES__
# DO NOT EDIT MANUALLY - Regenerate with: python3 scripts/airflow/generate_dynamic_dag.py

DEFAULT_ARGS = {
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

DATASETS = [
__CFG_LIST__
]

with DAG(
    dag_id="__DAG_ID__",
    description="Dynamic ISO ingestion DAG generated from configuration",
    default_args=DEFAULT_ARGS,
    schedule_interval="__SCHEDULE__",
    start_date=datetime(2024, 1, 1),
    catchup=__CATCHUP__,
    max_active_runs=__MAX_ACTIVE_RUNS__,
    tags=["aurum", "ingestion", "dynamic", "iso"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    preflight = PythonOperator(
        task_id="preflight",
        python_callable=build_preflight_callable(
            required_variables=("aurum_kafka_bootstrap", "aurum_schema_registry"),
        ),
    )

    # Register sources best-effort
    sources = []
    for ds in DATASETS:
        iso_code = (ds.get("iso") or "").lower()
        dataset = (ds.get("dataset") or "").lower()
        src_name = ds.get("source_name") or (f"iso.{iso_code}.{dataset}" if iso_code and dataset else ds.get("name", "unknown"))
        sources.append(
            iso_utils.IngestSource(
                name=src_name,
                description=ds.get("description", src_name),
                schedule=ds.get("schedule"),
                target=ds.get("topic"),
            )
        )
    register_sources = PythonOperator(
        task_id="register_sources",
        python_callable=lambda: iso_utils.register_sources(tuple(sources)),
    )

    tails = []
    for ds in DATASETS:
        iso_code = (ds.get("iso") or "").lower()
        dataset = (ds.get("dataset") or "").lower()
        job_name = ds.get("job_name") or ("{}_{}_to_kafka".format(iso_code, dataset))
        task_prefix = ds.get("task_prefix") or job_name
        source_name = ds.get("source_name") or ("iso.{}.{}".format(iso_code, dataset))
        pool = ds.get("pool")
        env_map = ds.get("env", {})
        env_entries = [f"{k}=\"{v}\"" for k, v in env_map.items()]
        render, execute, watermark = iso_utils.create_seatunnel_ingest_chain(
            task_prefix,
            job_name=job_name,
            source_name=source_name,
            env_entries=env_entries,
            pool=pool,
            watermark_policy=ds.get("watermark_policy", "hour"),
        )
        preflight >> register_sources >> render >> execute >> watermark
        tails.append(watermark)

    if tails:
        tails >> end
    else:
        register_sources >> end

    dag.on_failure_callback = build_failure_callback(source="aurum.airflow.__DAG_ID__")
"""

        dag_content = (
            tmpl
            .replace("__FILES__", files_list)
            .replace("__CFG_LIST__", cfg_list_literal)
            .replace("__DAG_ID__", args.dag_id)
            .replace("__SCHEDULE__", args.schedule)
            .replace("__CATCHUP__", "True" if args.catchup else "False")
            .replace("__MAX_ACTIVE_RUNS__", str(args.max_active_runs))
        )

        # Write to file or stdout
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(dag_content)
            print(f"Generated DAG file: {args.output}")
        else:
            print(dag_content)

        print("📋 DAG Configuration:")
        print(f"  • DAG ID: {args.dag_id}")
        print(f"  • Schedule: {args.schedule}")
        print(f"  • Max Active Runs: {args.max_active_runs}")
        print(f"  • Catchup: {args.catchup}")
        print(f"  • Config Files: {len(args.config_files)} files")
        for i, config_file in enumerate(args.config_files, 1):
            print(f"    {i}. {config_file}")

        print("🚀 Next Steps:")
        print("  1. Review the generated DAG file")
        print("  2. Test with: airflow dags test {args.dag_id}")
        print("  3. Deploy to Airflow environment")
        print("  4. Monitor with: airflow dags list | grep {args.dag_id}")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
