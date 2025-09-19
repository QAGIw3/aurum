"""Airflow DAG scaffolding for the daily vendor curve ingestion pipeline."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
from datetime import date, datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from aurum.dq import enforce_expectation_suite
from aurum.db import register_ingest_source, update_ingest_watermark
from aurum.parsers.runner import parse_files, write_output
from aurum.lakefs_client import ensure_branch, commit_branch, tag_commit

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "aurum-data",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["aurum-ops@example.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_lakefs_branch(**context: Any) -> None:
    """Placeholder for lakeFS branch creation logic."""
    execution_date = context["ds"]
    branch_name = f"eod_{execution_date.replace('-', '')}"
    repo = os.environ.get("AURUM_LAKEFS_REPO")
    if not repo:
        print("lakeFS repo not configured; skipping branch creation")
        return
    source_branch = os.environ.get("AURUM_LAKEFS_SOURCE_BRANCH", "main")
    ensure_branch(repo, branch_name, source_branch)
    os.environ["AURUM_LAKEFS_BRANCH"] = branch_name
    print(f"Ensured lakeFS branch {branch_name} from {source_branch}")


def parse_vendor_workbook(vendor: str, **context: Any) -> None:
    """Parse vendor workbooks using shared parser utilities."""
    execution_date = date.fromisoformat(context["ds"])
    drop_dir = Path(os.environ.get("AURUM_VENDOR_DROP_DIR", "/opt/airflow/data/vendor"))
    output_root_env = os.environ.get("AURUM_PARSED_OUTPUT_DIR", "/opt/airflow/data/processed")
    output_uri_env = os.environ.get("AURUM_PARSED_OUTPUT_URI")
    output_format = os.environ.get("AURUM_OUTPUT_FORMAT", "parquet")

    pattern = f"EOD_{vendor.upper()}_*.xlsx"
    files = sorted(drop_dir.glob(pattern))
    if not files:
        print(f"No workbooks found for vendor={vendor} in {drop_dir}")
        return

    source_name = f"vendor_{vendor.lower()}"
    try:
        register_ingest_source(
            source_name,
            description=f"Vendor curves for {vendor.upper()}",
            schedule="0 12 * * 1-5",
            target="iceberg.market.curve_observation",
        )
    except Exception as exc:  # pragma: no cover - best effort registration
        print(f"Failed to register ingest source {source_name}: {exc}")

    df = parse_files(files, as_of=execution_date)
    if df.empty:
        print(f"Parsed zero rows for vendor={vendor}; skipping write")
        return

    suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "curve_schema.json"
    try:
        enforce_expectation_suite(df, suite_path, suite_name="curve_schema")
    except Exception as exc:
        raise RuntimeError(f"Great Expectations validation failed for vendor {vendor}: {exc}") from exc

    sub_path = f"{execution_date.strftime('%Y%m%d')}/{vendor}"
    if output_uri_env:
        target = f"{output_uri_env.rstrip('/')}/{sub_path}"
    else:
        output_root = Path(output_root_env)
        target = output_root / sub_path

    write_output(df, target, as_of=execution_date, fmt=output_format)

    watermark_dt = datetime.combine(execution_date, datetime.min.time(), tzinfo=timezone.utc)
    try:
        update_ingest_watermark(source_name, "asof_date", watermark_dt)
    except Exception as exc:  # pragma: no cover - best effort watermark update
        print(f"Failed to update ingest watermark for {source_name}: {exc}")


def run_great_expectations(**context: Any) -> None:
    """Placeholder for Great Expectations checkpoint execution."""
    checkpoint_name = "curve_ingestion"
    print(f"Would run GE checkpoint {checkpoint_name}")


def emit_openlineage_events(**context: Any) -> None:
    """Placeholder for emitting OpenLineage events to Marquez."""
    print("Would emit OpenLineage events for curve ingestion run")


def update_openmetadata(**context: Any) -> None:
    """Register datasets/tables in OpenMetadata using the helper script."""
    repo_root = Path(__file__).resolve().parents[2]
    default_payload = repo_root / "scripts" / "metadata" / "metadata_payload_template.json"

    payload_path = Path(os.environ.get("AURUM_METADATA_PAYLOAD", str(default_payload)))
    if not payload_path.exists():
        print(f"Metadata payload not found at {payload_path}; skipping registration")
        return

    server = os.environ.get("OPENMETADATA_SERVER", os.environ.get("AURUM_OPENMETADATA_SERVER", "http://openmetadata:8585/api"))
    token = os.environ.get("OPENMETADATA_TOKEN") or os.environ.get("AURUM_OPENMETADATA_TOKEN")

    cmd = [
        "python",
        "scripts/metadata/register_metadata.py",
        "--server",
        server,
        "--input",
        str(payload_path),
    ]
    if token:
        cmd.extend(["--token", token])

    try:
        subprocess.run(cmd, check=True, cwd=repo_root)
        print(f"Registered metadata payload {payload_path} with OpenMetadata at {server}")
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"OpenMetadata registration failed: {exc}") from exc


def merge_branch(**context: Any) -> None:
    """Placeholder for merging lakeFS branch back to main once validation passes."""
    repo = os.environ.get("AURUM_LAKEFS_REPO")
    branch = os.environ.get("AURUM_LAKEFS_BRANCH")
    if not repo or not branch:
        print("lakeFS repo/branch not configured; skipping commit")
        return
    execution_date = context["ds"]
    message = f"Ingest vendor curves {execution_date}"
    commit_id = commit_branch(
        repo,
        branch,
        message,
        metadata={"execution_date": execution_date},
    )
    tag_name = os.environ.get("AURUM_LAKEFS_TAG") or f"ingest/{execution_date}"
    try:
        tag_commit(repo, tag_name, commit_id)
    except Exception as exc:  # pragma: no cover - tagging optional
        print(f"Failed to tag commit {commit_id}: {exc}")
    print(f"Committed lakeFS branch {branch} with {commit_id}")


with DAG(
    dag_id="ingest_vendor_curves_eod",
    description="Daily ingestion of vendor forward curve workbooks into Iceberg.",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 12 * * 1-5",  # weekdays at 12:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=True,
    max_active_runs=1,
    tags=["aurum", "curves", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")

    lakefs_branch = PythonOperator(
        task_id="lakefs_branch",
        python_callable=create_lakefs_branch,
    )

    parse_pw = PythonOperator(
        task_id="parse_pw",
        python_callable=parse_vendor_workbook,
        op_kwargs={"vendor": "pw"},
    )

    parse_eugp = PythonOperator(
        task_id="parse_eugp",
        python_callable=parse_vendor_workbook,
        op_kwargs={"vendor": "eugp"},
    )

    parse_rp = PythonOperator(
        task_id="parse_rp",
        python_callable=parse_vendor_workbook,
        op_kwargs={"vendor": "rp"},
    )

    ge_validate = PythonOperator(
        task_id="ge_validate",
        python_callable=run_great_expectations,
    )

    openlineage_emit = PythonOperator(
        task_id="openlineage_emit",
        python_callable=emit_openlineage_events,
    )

    openmetadata_update = PythonOperator(
        task_id="openmetadata_update",
        python_callable=update_openmetadata,
    )

    lakefs_merge = PythonOperator(
        task_id="lakefs_merge_tag",
        python_callable=merge_branch,
    )

    end = EmptyOperator(task_id="end")

    chain(
        start,
        lakefs_branch,
        [parse_pw, parse_eugp, parse_rp],
        ge_validate,
        openlineage_emit,
        openmetadata_update,
        lakefs_merge,
        end,
    )
