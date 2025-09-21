"""Helpers for maintaining Iceberg tables via Airflow tasks."""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Optional

LOGGER = logging.getLogger(__name__)


def _catalog_properties() -> tuple[str, str, dict[str, Any]]:
    branch = os.getenv("AURUM_ICEBERG_BRANCH", "main")
    catalog_name = os.getenv("AURUM_ICEBERG_CATALOG", "nessie")
    warehouse = os.getenv("AURUM_S3_WAREHOUSE", "s3://aurum/curated/iceberg")
    props = {
        "uri": os.getenv("AURUM_NESSIE_URI", "http://nessie:19121/api/v1"),
        "warehouse": warehouse,
        "s3.endpoint": os.getenv("AURUM_S3_ENDPOINT"),
        "s3.access-key-id": os.getenv("AURUM_S3_ACCESS_KEY"),
        "s3.secret-access-key": os.getenv("AURUM_S3_SECRET_KEY"),
        "nessie.ref": branch,
    }
    props = {k: v for k, v in props.items() if v is not None}
    return branch, catalog_name, props


def _load_catalog_table(table_name: str):
    try:
        from pyiceberg.catalog import load_catalog  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError("pyiceberg is required for Iceberg maintenance tasks") from exc

    branch, catalog_name, props = _catalog_properties()
    table_identifier = table_name if "@" in table_name else f"{table_name}@{branch}"
    catalog = load_catalog(catalog_name, **props)
    return catalog.load_table(table_identifier)


def _collect_expired_snapshot_ids(table, expire_ts_ms: int) -> list[int]:
    snapshots: Iterable[Any] = getattr(table, "snapshots", lambda: [])()
    result: list[int] = []
    for snapshot in snapshots:
        timestamp = getattr(snapshot, "timestamp_ms", None)
        snapshot_id = getattr(snapshot, "snapshot_id", None)
        if timestamp is None or snapshot_id is None:
            continue
        if timestamp < expire_ts_ms:
            result.append(snapshot_id)
    return result


def expire_snapshots(
    table_name: str,
    *,
    older_than_days: int = 14,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Expire snapshots older than the provided threshold."""

    table = _load_catalog_table(table_name)
    threshold = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    expire_ts_ms = int(threshold.timestamp() * 1000)

    if dry_run:
        eligible = _collect_expired_snapshot_ids(table, expire_ts_ms)
        LOGGER.info(
            "Dry run: %s snapshot(s) eligible for expiry on %s",
            len(eligible),
            table_name,
        )
        return {"dry_run": True, "eligible_snapshots": len(eligible)}

    result = table.expire_snapshots(expire_timestamp_ms=expire_ts_ms)
    deleted_snapshots = getattr(result, "deleted_snapshot_ids", []) or []
    deleted_data_files = getattr(result, "deleted_data_files", []) or []
    LOGGER.info(
        "Expired %s snapshots and removed %s data files from %s",
        len(deleted_snapshots),
        len(deleted_data_files),
        table_name,
    )
    return {
        "deleted_snapshots": len(deleted_snapshots),
        "deleted_data_files": len(deleted_data_files),
    }


def rewrite_data_files(
    table_name: str,
    *,
    target_file_size_mb: int = 128,
) -> Dict[str, Any]:
    """Compact small data files using Iceberg's rewrite action."""

    table = _load_catalog_table(table_name)
    size_bytes = max(target_file_size_mb, 16) * 1024 * 1024

    action = getattr(table, "rewrite_data_files", None)
    if action is None:
        raise RuntimeError("rewrite_data_files action not available on table")

    rewrite_builder = action()

    if hasattr(rewrite_builder, "with_target_file_size_in_bytes"):
        rewrite_builder = rewrite_builder.with_target_file_size_in_bytes(size_bytes)
    elif hasattr(rewrite_builder, "with_target_file_size_bytes"):
        rewrite_builder = rewrite_builder.with_target_file_size_bytes(size_bytes)
    elif hasattr(rewrite_builder, "options"):
        rewrite_builder = rewrite_builder.options(target_file_size_bytes=size_bytes)

    result = rewrite_builder.execute()
    rewritten = getattr(result, "rewritten_data_files", []) or []
    added = getattr(result, "added_data_files", []) or []
    LOGGER.info(
        "Compacted %s files into %s files on %s",
        len(rewritten),
        len(added),
        table_name,
    )
    return {
        "rewritten_files": len(rewritten),
        "added_files": len(added),
        "target_file_size_mb": target_file_size_mb,
    }


def purge_orphan_files(
    table_name: str,
    *,
    older_than_hours: int = 24,
) -> Dict[str, Any]:
    """Remove orphan data files that are older than the configured threshold."""

    table = _load_catalog_table(table_name)
    action = getattr(table, "remove_orphan_files", None)
    if action is None:
        raise RuntimeError("remove_orphan_files action not available on table")

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(older_than_hours, 1))
    remover = action()
    # pyiceberg may expose different helper methods depending on version
    if hasattr(remover, "older_than"):
        remover = remover.older_than(cutoff)
    elif hasattr(remover, "older_than_ts"):
        remover = remover.older_than_ts(int(cutoff.timestamp() * 1000))

    try:
        result = remover.execute()
    except Exception as exc:
        LOGGER.warning("Orphan purge failed for %s: %s", table_name, exc)
        raise

    removed_candidates = None
    for attr in ("orphan_file_location", "orphan_file_locations", "results", "result"):
        removed_candidates = getattr(result, attr, None)
        if removed_candidates:
            break

    if isinstance(removed_candidates, int):
        removed_count = removed_candidates
    elif removed_candidates is None:
        removed_count = 0
    else:
        removed_count = len(list(removed_candidates))

    LOGGER.info(
        "Removed %s orphan files from %s (older than %s hours)",
        removed_count,
        table_name,
        older_than_hours,
    )
    return {
        "removed_orphans": removed_count,
        "older_than_hours": older_than_hours,
    }
