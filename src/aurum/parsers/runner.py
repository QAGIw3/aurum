"""Helpers to parse vendor workbooks in bulk."""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:  # optional dependency
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None

from . import vendor_curves
from .enrichment import build_dlq_records, enrich_units_currency, partition_quarantine
from .utils import detect_asof
from .vendor_curves.schema import CANONICAL_COLUMNS

ICEBERG_ENV_FLAG = "AURUM_WRITE_ICEBERG"
AURUM_QUARANTINE_DIR_ENV = "AURUM_QUARANTINE_DIR"
AURUM_QUARANTINE_FORMAT_ENV = "AURUM_QUARANTINE_FORMAT"
AURUM_WRITE_DLQ_JSON_ENV = "AURUM_WRITE_DLQ_JSON"
QUARANTINE_COLUMN = "quarantine_reason"

LOG = logging.getLogger(__name__)

VENDOR_HINTS: Sequence[Tuple[str, str]] = (
    ("_PW_", "pw"),
    ("_EUGP_", "eugp"),
    ("_RP_", "rp"),
    ("_SIMPLE_", "simple"),
)


def detect_vendor(path: Path) -> str:
    name = path.name.upper()
    for marker, vendor in VENDOR_HINTS:
        if marker in name:
            return vendor
    raise ValueError(f"Could not detect vendor from filename '{path.name}'")


def detect_asof_from_workbook(path: Path) -> date:
    xl = pd.ExcelFile(path)
    for sheet in xl.sheet_names[:4]:
        df = xl.parse(sheet, header=None, nrows=12)
        text_rows: List[str] = []
        for _, row in df.iterrows():
            for value in row.tolist():
                if isinstance(value, str) and value.strip():
                    text_rows.append(value)
        if not text_rows:
            continue
        try:
            return detect_asof(text_rows)
        except ValueError:
            continue
    raise ValueError(f"Could not infer as-of date within workbook '{path}'")


def parse_files(
    paths: Iterable[Path], *, as_of: Optional[date] = None, max_workers: Optional[int] = None
) -> pd.DataFrame:
    path_list = [Path(raw_path) for raw_path in paths]
    if not path_list:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)

    env_workers = os.getenv("AURUM_PARSER_MAX_WORKERS")
    worker_limit: Optional[int] = max_workers
    if worker_limit is None and env_workers:
        try:
            worker_limit = int(env_workers)
        except ValueError:
            LOG.warning("Invalid AURUM_PARSER_MAX_WORKERS value: %s", env_workers)
            worker_limit = None

    if worker_limit is None or worker_limit <= 0:
        worker_limit = min(4, len(path_list))
    else:
        worker_limit = min(worker_limit, len(path_list))

    if worker_limit < 1:
        worker_limit = 1

    def _parse_path(path: Path) -> pd.DataFrame:
        if not path.exists():
            raise FileNotFoundError(path)
        vendor = detect_vendor(path)
        detected_asof = as_of or detect_asof_from_workbook(path)
        LOG.info("Parsing %s (vendor=%s asof=%s)", path, vendor, detected_asof)
        df = vendor_curves.parse(vendor, str(path), detected_asof)
        if df.empty:
            LOG.warning("No data parsed from %s", path)
        return df

    frames: List[pd.DataFrame] = []
    if worker_limit == 1:
        for path in path_list:
            df = _parse_path(path)
            if not df.empty:
                frames.append(df)
    else:
        with ThreadPoolExecutor(max_workers=worker_limit) as executor:
            for df in executor.map(_parse_path, path_list):
                if isinstance(df, pd.DataFrame) and not df.empty:
                    frames.append(df)
    if not frames:
        return pd.DataFrame(columns=CANONICAL_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    if "contract_month" in combined.columns:
        combined["contract_month"] = pd.to_datetime(
            combined["contract_month"], errors="coerce"
        ).dt.date
    return combined


def write_output(
    df: pd.DataFrame,
    output_dir: Path | str,
    *,
    as_of: Optional[date],
    fmt: str,
    prefix: str = "curves",
) -> Path | str:
    suffix = "parquet" if fmt == "parquet" else fmt
    asof_segment = as_of.strftime("%Y%m%d") if as_of else datetime.utcnow().strftime("%Y%m%d")
    destination = str(output_dir)
    filename = f"{prefix}_{asof_segment}.{suffix}"

    if destination.startswith("s3://"):
        if boto3 is None:
            raise RuntimeError("boto3 is required for S3 output")
        bucket, key_prefix = _split_s3_uri(destination)
        key = f"{key_prefix}{filename}" if key_prefix.endswith("/") else f"{key_prefix}/{filename}" if key_prefix else filename
        client = _build_s3_client()
        if fmt == "parquet":
            try:
                import pyarrow  # noqa: F401
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("pyarrow is required for parquet output") from exc
            bufb = io.BytesIO()
            df.to_parquet(bufb, index=False)
            body = bufb.getvalue()
        elif fmt == "csv":
            bufs = io.StringIO()
            df.to_csv(bufs, index=False)
            body = bufs.getvalue().encode("utf-8")
        else:
            raise ValueError(f"Unsupported format '{fmt}'")
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return f"s3://{bucket}/{key}"

    path = Path(destination) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        try:
            df.to_parquet(path, index=False)
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("pyarrow is required for parquet output") from exc
    elif fmt == "csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported format '{fmt}'")
    return path


def _split_s3_uri(uri: str) -> tuple[str, str]:
    without_scheme = uri[len("s3://") :]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    key_prefix = parts[1] if len(parts) > 1 else ""
    return bucket, key_prefix.rstrip("/")


def _build_s3_client():
    session = boto3.session.Session(
        aws_access_key_id=os.getenv("AURUM_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("AURUM_S3_SECRET_KEY"),
        region_name=os.getenv("AURUM_S3_REGION", "us-east-1"),
    )
    endpoint = os.getenv("AURUM_S3_ENDPOINT")
    return session.client("s3", endpoint_url=endpoint)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Parse Aurum vendor workbooks into canonical data")
    parser.add_argument("files", nargs="+", type=Path, help="Paths to vendor workbooks")
    parser.add_argument("--as-of", dest="as_of", help="Override as-of date (YYYY-MM-DD)")
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        type=Path,
        default=Path("./artifacts"),
        help="Where to write combined dataset",
    )
    parser.add_argument(
        "--format",
        dest="fmt",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        help="Number of parallel parser workers (default: min(4, file count) or env AURUM_PARSER_MAX_WORKERS)",
    )
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate parsed dataframe against an expectation suite (Great Expectations-like JSON)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse (and optionally validate) only; print a summary and exit without writing",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="After writing output, print a one-line parse summary to stdout",
    )
    parser.add_argument(
        "--suite",
        dest="suite",
        type=Path,
        help="Path to expectation suite JSON (defaults to ge/expectations/curve_schema.json)",
    )
    parser.add_argument(
        "--write-iceberg",
        action="store_true",
        help="Append parsed data to the configured Iceberg table via Nessie",
    )
    parser.add_argument(
        "--iceberg-table",
        dest="iceberg_table",
        help="Fully qualified Iceberg table (overrides env)",
    )
    parser.add_argument(
        "--iceberg-branch",
        dest="iceberg_branch",
        help="Iceberg/Nessie branch to write to (overrides env)",
    )
    parser.add_argument(
        "--lakefs-commit",
        action="store_true",
        help="Commit parsed rows to lakeFS using aurum.lakefs_client",
    )
    parser.add_argument(
        "--lakefs-branch",
        dest="lakefs_branch",
        help="Target lakeFS branch (defaults to env or eod_<asof>)",
    )
    parser.add_argument(
        "--lakefs-message",
        dest="lakefs_message",
        help="Commit message to use when lakeFS commit occurs",
    )
    parser.add_argument(
        "--lakefs-tag",
        dest="lakefs_tag",
        help="Optional tag name to apply to the lakeFS commit",
    )
    parser.add_argument(
        "--quarantine-dir",
        dest="quarantine_dir",
        type=Path,
        help="Directory (or s3 URI) to write quarantined rows; defaults to env AURUM_QUARANTINE_DIR or <output_dir>/quarantine",
    )
    parser.add_argument(
        "--quarantine-format",
        dest="quarantine_format",
        choices=["parquet", "csv"],
        default=os.getenv(AURUM_QUARANTINE_FORMAT_ENV, "parquet"),
        help="File format for quarantined rows (default parquet)",
    )
    parser.add_argument(
        "--dlq-json-dir",
        dest="dlq_json_dir",
        type=Path,
        help="Directory to write DLQ JSON payloads (defaults to quarantine directory)",
    )
    parser.add_argument(
        "--no-dlq-json",
        action="store_true",
        help="Disable writing DLQ JSON payloads when quarantined rows are detected",
    )

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    as_of: Optional[date] = None
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)

    raw_df = parse_files(args.files, as_of=as_of, max_workers=args.workers)
    if raw_df.empty:
        LOG.warning("No rows parsed")

    enriched_df = enrich_units_currency(raw_df)
    clean_df, quarantine_df = partition_quarantine(enriched_df)
    df = clean_df
    publish_df = df.drop(columns=[QUARANTINE_COLUMN], errors="ignore")

    selected_asof = as_of or _infer_asof(df if not df.empty else enriched_df)

    quarantine_dir = _resolve_quarantine_dir(args)
    if not quarantine_df.empty and quarantine_dir is not None:
        quarantine_path = write_output(
            quarantine_df,
            quarantine_dir,
            as_of=selected_asof,
            fmt=args.quarantine_format,
            prefix="quarantine_curves",
        )
        LOG.warning("Quarantined %s rows to %s", len(quarantine_df), quarantine_path)

        emit_dlq_json = (
            not args.no_dlq_json
            and os.getenv(AURUM_WRITE_DLQ_JSON_ENV, "1").lower() in {"1", "true", "yes"}
        )
        if emit_dlq_json:
            dlq_dir = args.dlq_json_dir if args.dlq_json_dir is not None else quarantine_dir
            dlq_records = list(build_dlq_records(quarantine_df))
            if dlq_records and dlq_dir is not None:
                try:
                    dlq_path = _write_dlq_json(dlq_records, dlq_dir, selected_asof)
                    LOG.warning("Wrote %s DLQ payloads to %s", len(dlq_records), dlq_path)
                except RuntimeError as exc:
                    LOG.warning("Skipping DLQ JSON write: %s", exc)
    elif not quarantine_df.empty:
        LOG.warning(
            "Detected %s quarantined rows but no quarantine directory resolved; rows retained for validation only",
            len(quarantine_df),
        )

    # Optional data validation prior to writing output
    if args.validate:
        try:
            from aurum.dq import enforce_expectation_suite  # type: ignore

            suites: List[Tuple[Path, str]] = []
            if args.suite:
                suites.append((args.suite, args.suite.stem))
            else:
                repo_root = Path(__file__).resolve().parents[3]
                suites.append((repo_root / "ge" / "expectations" / "curve_schema.json", "curve_schema"))
                suites.append((repo_root / "ge" / "expectations" / "curve_landing.json", "curve_landing"))
            for suite_path, suite_name in suites:
                enforce_expectation_suite(publish_df, suite_path, suite_name=suite_name)
                LOG.info("Validation succeeded against %s", suite_path)
        except Exception as exc:  # pragma: no cover - surface error condition
            raise RuntimeError(f"Validation failed: {exc}") from exc
    if args.dry_run:
        distinct_curves = len(publish_df["curve_key"].unique()) if (not publish_df.empty and "curve_key" in publish_df.columns) else 0
        quarantined = len(quarantine_df)
        print(
            f"Parsed {len(publish_df)} rows; distinct curves: {distinct_curves}; as_of={selected_asof}; quarantined={quarantined}"
        )
        return 0

    path = write_output(publish_df, args.output_dir, as_of=selected_asof, fmt=args.fmt)
    LOG.info("Wrote %s rows to %s", len(df), path)
    if args.summary:
        distinct_curves = len(publish_df["curve_key"].unique()) if (not publish_df.empty and "curve_key" in publish_df.columns) else 0
        quarantined = len(quarantine_df)
        print(
            f"Parsed {len(publish_df)} rows; distinct curves: {distinct_curves}; as_of={selected_asof}; quarantined={quarantined}"
        )

    if args.write_iceberg or os.getenv(ICEBERG_ENV_FLAG):
        try:
            from .iceberg_writer import write_to_iceberg as iceberg_write
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "pyiceberg is required for Iceberg writes; install with 'pip install aurum[iceberg]'"
            ) from exc

        iceberg_write(
            df,
            table=args.iceberg_table,
            branch=args.iceberg_branch,
        )
        LOG.info("Appended %s rows to Iceberg table", len(df))

    if _should_commit_lakefs(args):
        commit_ref = _commit_lakefs(df, selected_asof, args)
        LOG.info("Committed lakeFS branch with ref %s", commit_ref)
    return 0


def _infer_asof(df: pd.DataFrame) -> date:
    if df.empty or 'asof_date' not in df.columns:
        return date.today()
    series = pd.to_datetime(df['asof_date'], errors='coerce')
    if series.notna().any():
        return series.dropna().iloc[0].date()
    return date.today()


def _should_commit_lakefs(args: argparse.Namespace) -> bool:
    if getattr(args, 'lakefs_commit', False):
        return True
    return os.getenv('AURUM_LAKEFS_COMMIT', '').lower() in {'1', 'true', 'yes'}


def _commit_lakefs(df: pd.DataFrame, as_of: date, args: argparse.Namespace) -> str:
    from ..lakefs_client import commit_branch, ensure_branch, tag_commit

    repo = os.getenv('AURUM_LAKEFS_REPO')
    if not repo:
        raise RuntimeError('AURUM_LAKEFS_REPO must be set for lakeFS commits')
    source_branch = os.getenv('AURUM_LAKEFS_SOURCE_BRANCH', 'main')
    branch = getattr(args, 'lakefs_branch', None) or os.getenv('AURUM_LAKEFS_BRANCH') or f"eod_{as_of.strftime('%Y%m%d')}"
    message = getattr(args, 'lakefs_message', None) or os.getenv('AURUM_LAKEFS_COMMIT_MESSAGE') or f'Ingest curves for {as_of.isoformat()}'

    ensure_branch(repo, branch, source_branch)
    metadata = {
        'asof_date': as_of.isoformat(),
        'rows': str(len(df)),
    }
    commit_id = commit_branch(repo, branch, message, metadata)

    tag_name = getattr(args, 'lakefs_tag', None) or os.getenv('AURUM_LAKEFS_TAG')
    if tag_name:
        tag_commit(repo, tag_name, commit_id)

    return commit_id


def _resolve_quarantine_dir(args: argparse.Namespace) -> Path | str | None:
    if getattr(args, "quarantine_dir", None) is not None:
        return args.quarantine_dir
    env_override = os.getenv(AURUM_QUARANTINE_DIR_ENV)
    if env_override:
        return env_override
    output_dir = getattr(args, "output_dir", None)
    if output_dir is None:
        return None
    return Path(output_dir) / "quarantine"


def _write_dlq_json(records: Iterable[dict], directory: Path | str, as_of: Optional[date]) -> Path:
    original = directory
    directory_path = Path(directory)
    if isinstance(original, str) and original.startswith("s3://"):
        raise RuntimeError("DLQ JSON output only supports local filesystem paths")
    directory_path.mkdir(parents=True, exist_ok=True)
    asof_segment = as_of.strftime("%Y%m%d") if as_of else datetime.utcnow().strftime("%Y%m%d")
    path = directory_path / f"aurum.curve.observation.dlq_{asof_segment}.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record))
            fh.write("\n")
    return path


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
