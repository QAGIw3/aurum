"""Helpers to parse vendor workbooks in bulk."""
from __future__ import annotations

import argparse
import io
import logging
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd

try:  # optional dependency
    import boto3
except ImportError:  # pragma: no cover
    boto3 = None

from . import vendor_curves
from .utils import detect_asof

ICEBERG_ENV_FLAG = "AURUM_WRITE_ICEBERG"
from .vendor_curves.schema import CANONICAL_COLUMNS

LOG = logging.getLogger(__name__)

VENDOR_HINTS: Sequence[Tuple[str, str]] = (
    ("_PW_", "pw"),
    ("_EUGP_", "eugp"),
    ("_RP_", "rp"),
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


def parse_files(paths: Iterable[Path], *, as_of: Optional[date] = None) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for raw_path in paths:
        path = Path(raw_path)
        if not path.exists():
            raise FileNotFoundError(path)
        vendor = detect_vendor(path)
        detected_asof = as_of or detect_asof_from_workbook(path)
        LOG.info("Parsing %s (vendor=%s asof=%s)", path, vendor, detected_asof)
        df = vendor_curves.parse(vendor, str(path), detected_asof)
        if df.empty:
            LOG.warning("No data parsed from %s", path)
            continue
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
) -> Path | str:
    suffix = "parquet" if fmt == "parquet" else fmt
    asof_segment = as_of.strftime("%Y%m%d") if as_of else datetime.utcnow().strftime("%Y%m%d")
    destination = str(output_dir)
    filename = f"curves_{asof_segment}.{suffix}"

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
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            body = buffer.getvalue()
        elif fmt == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            body = buffer.getvalue().encode("utf-8")
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
    parser.add_argument("--verbose", action="store_true")
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

    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    as_of: Optional[date] = None
    if args.as_of:
        as_of = date.fromisoformat(args.as_of)

    df = parse_files(args.files, as_of=as_of)
    if df.empty:
        LOG.warning("No rows parsed")
    selected_asof = as_of or _infer_asof(df)
    path = write_output(df, args.output_dir, as_of=selected_asof, fmt=args.fmt)
    LOG.info("Wrote %s rows to %s", len(df), path)

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


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
