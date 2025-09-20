"""Airflow DAG scaffolding for the daily vendor curve ingestion pipeline."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Optional

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

"""
Note: imports of aurum.* are deferred into task functions to ensure Airflow pods
use the real package mounted at /opt/airflow/src instead of DAG-time stubs.
"""

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
    # Defer import
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.lakefs_client import ensure_branch  # type: ignore

        ensure_branch(repo, branch_name, source_branch)
    except Exception as exc:  # pragma: no cover
        print(f"lakeFS ensure_branch failed: {exc}")
    os.environ["AURUM_LAKEFS_BRANCH"] = branch_name
    print(f"Ensured lakeFS branch {branch_name} from {source_branch}")


def _is_truthy(value: str | None) -> bool:
    return value is not None and value.lower() in {"1", "true", "yes", "on"}


def parse_vendor_workbook(vendor: str, **context: Any) -> None:
    """Parse vendor workbooks using shared parser utilities."""
    execution_date = date.fromisoformat(context["ds"])
    drop_dir = Path(os.environ.get("AURUM_VENDOR_DROP_DIR", "/opt/airflow/data/vendor"))
    output_root_env = os.environ.get("AURUM_PARSED_OUTPUT_DIR", "/opt/airflow/data/processed")
    output_uri_env = os.environ.get("AURUM_PARSED_OUTPUT_URI")
    output_format = os.environ.get("AURUM_OUTPUT_FORMAT", "parquet")
    ti = context.get("ti")

    pattern = f"EOD_{vendor.upper()}_*.xlsx"
    files = sorted(drop_dir.glob(pattern))
    if ti:
        ti.xcom_push(key="input_files", value=[str(path) for path in files])
    if not files:
        print(f"No workbooks found for vendor={vendor} in {drop_dir}")
        if ti:
            ti.xcom_push(key="rows", value=0)
        return

    source_name = f"vendor_{vendor.lower()}"
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import register_ingest_source  # type: ignore

        register_ingest_source(
            source_name,
            description=f"Vendor curves for {vendor.upper()}",
            schedule="0 12 * * 1-5",
            target="iceberg.market.curve_observation",
        )
    except Exception as exc:  # pragma: no cover - best effort registration
        print(f"Failed to register ingest source {source_name}: {exc}")

    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.parsers.runner import parse_files  # type: ignore

        df = parse_files(files, as_of=execution_date)
    except Exception as exc:  # pragma: no cover - ensure failure is surfaced
        raise RuntimeError(f"Failed to parse vendor files: {exc}") from exc
    if df.empty:
        print(f"Parsed zero rows for vendor={vendor}; skipping write")
        if ti:
            ti.xcom_push(key="rows", value=0)
        return

    suite_path = Path(__file__).resolve().parents[2] / "ge" / "expectations" / "curve_schema.json"
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.dq import enforce_expectation_suite  # type: ignore

        enforce_expectation_suite(df, suite_path, suite_name="curve_schema")
    except Exception as exc:
        raise RuntimeError(f"Great Expectations validation failed for vendor {vendor}: {exc}") from exc

    sub_path = f"{execution_date.strftime('%Y%m%d')}/{vendor}"
    if output_uri_env:
        target = f"{output_uri_env.rstrip('/')}/{sub_path}"
    else:
        output_root = Path(output_root_env)
        target = output_root / sub_path

    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.parsers.runner import write_output  # type: ignore

        write_output(df, target, as_of=execution_date, fmt=output_format)

        should_append = _is_truthy(os.getenv("AURUM_EOD_WRITE_ICEBERG", os.getenv("AURUM_WRITE_ICEBERG")))
        if should_append:
            from aurum.parsers.iceberg_writer import write_to_iceberg  # type: ignore

            write_to_iceberg(df)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"Failed to write parsed output: {exc}") from exc

    watermark_dt = datetime.combine(execution_date, datetime.min.time(), tzinfo=timezone.utc)
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.db import update_ingest_watermark  # type: ignore

        update_ingest_watermark(source_name, "asof_date", watermark_dt)
    except Exception as exc:  # pragma: no cover - best effort watermark update
        print(f"Failed to update ingest watermark for {source_name}: {exc}")

    if ti:
        ti.xcom_push(key="rows", value=len(df))


def run_great_expectations(**context: Any) -> None:
    ti = context["ti"]
    ds = context["ds"]
    vendor_tasks = ["parse_pw", "parse_eugp", "parse_rp"]
    total_rows = 0
    for task_id in vendor_tasks:
        value = ti.xcom_pull(task_ids=task_id, key="rows")
        if value is None:
            continue
        try:
            total_rows += int(value)
        except (TypeError, ValueError):
            continue

    status = "skipped"
    if total_rows == 0:
        print("No vendor rows parsed; skipping curve GE checkpoint")
    else:
        try:
            import sys
            src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
            if src_path and src_path not in sys.path:
                sys.path.insert(0, src_path)
            from aurum.dq import enforce_expectation_suite  # type: ignore
            import pandas as pd  # type: ignore

            repo_root = Path(__file__).resolve().parents[2]
            suite_path = repo_root / "ge" / "expectations" / "curve_business.yml"
            output_root = Path(os.environ.get("AURUM_PARSED_OUTPUT_DIR", "/opt/airflow/data/processed"))
            run_folder = output_root / ds.replace("-", "")
            frames: list[pd.DataFrame] = []
            if run_folder.exists():
                for parquet_path in run_folder.rglob("*.parquet"):
                    try:
                        frames.append(pd.read_parquet(parquet_path))
                    except Exception as exc:  # pragma: no cover - optional read
                        print(f"Failed to read {parquet_path}: {exc}")
            if frames:
                sample_df = pd.concat(frames, ignore_index=True)
                enforce_expectation_suite(sample_df, suite_path, suite_name="curve_business")
                status = "passed"
            else:
                print(f"No written parquet files found in {run_folder}; skipping curve GE checkpoint")
        except ModuleNotFoundError as exc:
            print(f"Skipping curve expectations: {exc}")
            status = "skipped"
        except Exception as exc:
            ti.xcom_push(key="ge_status", value="failed")
            raise RuntimeError(f"Curve expectations failed: {exc}") from exc

    ti.xcom_push(key="ge_status", value=status)


def validate_scenario_outputs(**context: Any) -> None:
    ti = context["ti"]
    repo_root = Path(__file__).resolve().parents[2]
    suite_path = repo_root / "ge" / "expectations" / "scenario_output.json"
    try:
        import pandas as pd  # type: ignore
        from aurum.dq import enforce_expectation_suite  # type: ignore
        from pyiceberg.catalog import load_catalog  # type: ignore
    except ModuleNotFoundError as exc:
        print(f"Skipping scenario expectations: {exc}")
        ti.xcom_push(key="scenario_ge_status", value="skipped")
        return

    branch = os.getenv("AURUM_ICEBERG_BRANCH", "main")
    catalog_name = os.getenv("AURUM_ICEBERG_CATALOG", "nessie")
    table_name = os.getenv("AURUM_SCENARIO_ICEBERG_TABLE", "iceberg.market.scenario_output")
    uri = os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1")
    warehouse = os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")
    props = {
        "uri": uri,
        "warehouse": warehouse,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch,
    }
    props = {k: v for k, v in props.items() if v is not None}
    table_identifier = table_name if "@" in table_name else f"{table_name}@{branch}"
    try:
        catalog = load_catalog(catalog_name, **props)
        table = catalog.load_table(table_identifier)
        scanner = table.scan()
        if hasattr(scanner, "limit"):
            scanner = scanner.limit(5000)
        arrow_table = scanner.to_arrow()
        df = arrow_table.to_pandas()
    except Exception as exc:
        ti.xcom_push(key="scenario_ge_status", value="failed")
        raise RuntimeError(f"Failed to load scenario output table: {exc}") from exc

    if df.empty:
        print("Scenario output table empty; skipping expectations")
        ti.xcom_push(key="scenario_ge_status", value="skipped")
        return

    enforce_expectation_suite(df, suite_path, suite_name="scenario_output")
    ti.xcom_push(key="scenario_ge_status", value="passed")


def emit_openlineage_events(**context: Any) -> None:
    endpoint = os.getenv("OPENLINEAGE_ENDPOINT")
    if not endpoint:
        print("OPENLINEAGE_ENDPOINT not set; skipping OpenLineage emission")
        return
    try:
        import requests  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError("requests package is required for OpenLineage emission") from exc

    namespace = os.getenv("OPENLINEAGE_NAMESPACE", "aurum")
    job_name = os.getenv("OPENLINEAGE_JOB_NAME", "airflow.ingest_vendor_curves_eod")
    run_id = context.get("run_id")
    ds = context.get("ds")
    tenant = os.getenv("OPENLINEAGE_TENANT", os.getenv("AURUM_DEFAULT_TENANT", "aurum"))
    code_version = os.getenv("AURUM_CODE_VERSION") or os.getenv("GIT_COMMIT")

    ti = context.get("ti")
    vendor_tasks = os.getenv("AURUM_VENDOR_TASK_IDS", "parse_pw,parse_eugp,parse_rp").split(",")
    input_files: set[str] = set()
    total_rows = 0
    if ti:
        for task_id in vendor_tasks:
            task_name = task_id.strip()
            if not task_name:
                continue
            files = ti.xcom_pull(task_ids=task_name, key="input_files") or []
            for entry in files:
                if entry:
                    input_files.add(str(entry))
            rows = ti.xcom_pull(task_ids=task_name, key="rows")
            if rows is not None:
                try:
                    total_rows += int(rows)
                except (TypeError, ValueError):
                    continue

    def _as_uri(path: str) -> str:
        try:
            resolved = Path(path).resolve(strict=False)
            if resolved.is_absolute():
                return resolved.as_uri()
        except Exception:
            pass
        return path

    inputs = [
        {
            "namespace": os.getenv("OPENLINEAGE_INPUT_NAMESPACE", "file"),
            "name": _as_uri(file_path),
            "facets": {},
        }
        for file_path in sorted(input_files)
    ]

    output_facets: Dict[str, Any] = {}
    if total_rows:
        output_facets["outputStatistics"] = {
            "type": "OutputStatisticsDatasetFacet",
            "rowCount": total_rows,
        }

    run_facets: Dict[str, Any] = {
        "aurumMetadata": {
            "type": "CustomFacet",
            "codeVersion": code_version or "unknown",
            "asOfDate": ds,
            "tenant": tenant,
        }
    }
    if ds:
        run_facets["nominalTime"] = {
            "type": "NominalTimeRunFacet",
            "nominalTime": f"{ds}T00:00:00Z",
        }

    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "run": {
            "runId": run_id or os.getenv("OPENLINEAGE_RUN_ID", "unknown"),
            "facets": run_facets,
        },
        "job": {
            "namespace": namespace,
            "name": job_name,
        },
        "inputs": inputs,
        "outputs": [
            {
                "namespace": "nessie",
                "name": "iceberg.market.curve_observation",
                "facets": output_facets,
            }
        ],
    }

    response = requests.post(endpoint, json=event, timeout=5)
    try:
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover - http error
        raise RuntimeError(f"OpenLineage emission failed: {exc}") from exc

    if ti:
        ti.xcom_push(key="openlineage_status", value="sent")
    print(f"OpenLineage event sent to {endpoint}")


def preview_nessie_diff(**context: Any) -> None:
    if not _is_truthy(os.getenv("AURUM_NESSIE_DIFF_PREVIEW", "0")):
        print("Nessie diff preview disabled")
        return

    branch = os.environ.get("AURUM_LAKEFS_BRANCH")
    if not branch:
        print("No lakeFS branch set; skipping Nessie diff preview")
        return
    target = os.getenv("AURUM_NESSIE_BASE_REF", "main")
    nessie_uri = os.getenv("AURUM_NESSIE_URI")
    if not nessie_uri:
        print("AURUM_NESSIE_URI not configured; skipping diff preview")
        return

    try:
        import requests  # type: ignore
    except ModuleNotFoundError:
        print("requests not available; skipping Nessie diff preview")
        return

    diff_url = nessie_uri.rstrip("/") + f"/trees/diff?fromRef={branch}&toRef={target}"
    try:
        response = requests.get(diff_url, timeout=5)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # pragma: no cover - best effort preview
        print(f"Failed to fetch Nessie diff: {exc}")
        return

    diffs = payload.get("diffs") or payload.get("entries") or []
    if not diffs:
        print(f"No Nessie diff detected between {branch} and {target}")
        return

    preview = diffs[:5]
    print(f"Nessie diff preview ({branch} -> {target}):")
    for entry in preview:
        kind = entry.get("type") or entry.get("kind") or "unknown"
        name = entry.get("name") or entry.get("key") or entry
        print(f" - {kind}: {name}")
    remaining = len(diffs) - len(preview)
    if remaining > 0:
        print(f"... {remaining} additional diff entries omitted")


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
        ti = context.get("ti")
        if ti:
            ti.xcom_push(key="openmetadata_status", value="registered")
        print(f"Registered metadata payload {payload_path} with OpenMetadata at {server}")
    except subprocess.CalledProcessError as exc:
        ti = context.get("ti")
        if ti:
            ti.xcom_push(key="openmetadata_status", value="failed")
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
    ti = context.get("ti")
    if ti:
        row_counts = []
        for task_id in ["parse_pw", "parse_eugp", "parse_rp"]:
            value = ti.xcom_pull(task_ids=task_id, key="rows")
            try:
                row_counts.append(int(value) if value is not None else 0)
            except (TypeError, ValueError):
                row_counts.append(0)
        total_rows = sum(row_counts)
        ge_status = ti.xcom_pull(task_ids="ge_validate", key="ge_status")
        scenario_status = ti.xcom_pull(task_ids="ge_validate_scenarios", key="scenario_ge_status")
        if total_rows <= 0:
            raise RuntimeError("Aborting lakeFS merge: zero rows ingested")
        if ge_status not in {"passed", "skipped"}:
            raise RuntimeError("Aborting lakeFS merge: curve Great Expectations did not pass")
        if scenario_status not in {"passed", "skipped", None}:
            raise RuntimeError("Aborting lakeFS merge: scenario Great Expectations did not pass")
    metadata = {"execution_date": execution_date}
    ti = context.get("ti")
    vendor_tasks = {"pw": "parse_pw", "eugp": "parse_eugp", "rp": "parse_rp"}
    total_rows = 0
    if ti:
        for vendor, task_id in vendor_tasks.items():
            rows = ti.xcom_pull(task_ids=task_id, key="rows")
            if rows is None:
                continue
            try:
                int_rows = int(rows)
            except (TypeError, ValueError):
                continue
            metadata[f"{vendor}_rows"] = str(int_rows)
            total_rows += int_rows
    if total_rows:
        metadata["total_rows"] = str(total_rows)
    if ge_status:
        metadata["curve_ge_status"] = ge_status
    if scenario_status:
        metadata["scenario_ge_status"] = scenario_status or "skipped"
    try:
        import sys
        src_path = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
        if src_path and src_path not in sys.path:
            sys.path.insert(0, src_path)
        from aurum.lakefs_client import commit_branch, open_pull_request, merge_pull_request, tag_commit  # type: ignore

        commit_id = commit_branch(repo, branch, message, metadata=metadata)
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"lakeFS commit failed: {exc}")

    pr_id: Optional[int] = None
    if _is_truthy(os.getenv("AURUM_LAKEFS_CREATE_PR", "1")):
        target_branch = os.getenv("AURUM_LAKEFS_SOURCE_BRANCH", "main")
        pr_title = os.getenv("AURUM_LAKEFS_PR_TITLE", f"Ingest vendor curves {execution_date}")
        description_lines = [
            f"Execution date: {execution_date}",
            f"Total rows: {metadata.get('total_rows', '0')}",
            f"Curve GE status: {metadata.get('curve_ge_status', 'unknown')}",
            f"Scenario GE status: {metadata.get('scenario_ge_status', 'unknown')}",
        ]
        try:
            pr = open_pull_request(
                repo,
                source=branch,
                target=target_branch,
                title=pr_title,
                description="\n".join(description_lines),
            )
            if isinstance(pr, dict):
                pr_id = pr.get("id")
                print(f"Opened lakeFS pull request {pr_id} for {branch} -> {target_branch}")
                if ti and pr_id is not None:
                    ti.xcom_push(key="lakefs_pr_id", value=pr_id)
        except Exception as exc:  # pragma: no cover - PR creation optional
            print(f"Failed to open lakeFS pull request: {exc}")

    if pr_id is not None and _is_truthy(os.getenv("AURUM_LAKEFS_AUTO_MERGE", "0")):
        try:
            merge_pull_request(repo, pr_id)
            print(f"Merged lakeFS pull request {pr_id}")
        except Exception as exc:  # pragma: no cover - merge optional
            print(f"Failed to merge pull request {pr_id}: {exc}")

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

    ge_validate_scenarios = PythonOperator(
        task_id="ge_validate_scenarios",
        python_callable=validate_scenario_outputs,
    )

    openlineage_emit = PythonOperator(
        task_id="openlineage_emit",
        python_callable=emit_openlineage_events,
    )

    openmetadata_update = PythonOperator(
        task_id="openmetadata_update",
        python_callable=update_openmetadata,
    )

    nessie_diff = PythonOperator(
        task_id="nessie_diff_preview",
        python_callable=preview_nessie_diff,
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
        ge_validate_scenarios,
        openlineage_emit,
        openmetadata_update,
        nessie_diff,
        lakefs_merge,
        end,
    )
