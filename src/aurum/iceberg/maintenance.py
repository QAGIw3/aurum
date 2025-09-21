"""Helpers for maintaining Iceberg tables via Airflow tasks."""
from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, Optional

LOGGER = logging.getLogger(__name__)

try:  # Optional dependency at runtime
    from aurum.airflow_utils import metrics as airflow_metrics  # type: ignore
except Exception:  # pragma: no cover - avoid hard dependency for tests
    airflow_metrics = None  # type: ignore


def _table_tag(table_name: str) -> str:
    """Return a StatsD-friendly table identifier."""

    return table_name.replace(".", "_").replace("-", "_")


def _emit_metrics(
    action: str,
    table_name: str,
    dry_run: bool,
    duration_ms: float,
    outcome: Dict[str, Any],
    error: Optional[BaseException],
) -> None:
    if airflow_metrics is None:
        return

    tags: Dict[str, str] = {
        "table": _table_tag(table_name),
        "action": action,
        "dry_run": "true" if dry_run else "false",
    }
    try:
        airflow_metrics.increment("iceberg.maintenance.invocations", tags=tags)
        airflow_metrics.timing("iceberg.maintenance.duration_ms", duration_ms, tags=tags)
        if error is None:
            airflow_metrics.increment("iceberg.maintenance.success", tags=tags)
            for key, value in outcome.items():
                if isinstance(value, (int, float)):
                    airflow_metrics.gauge(
                        f"iceberg.maintenance.{key}",
                        float(value),
                        tags=tags,
                    )
        else:
            failure_tags = {**tags, "error": error.__class__.__name__}
            airflow_metrics.increment("iceberg.maintenance.failure", tags=failure_tags)
    except Exception:  # pragma: no cover - best effort only
        LOGGER.debug("Failed to emit maintenance metrics", exc_info=True)


def _run_with_metrics(
    action: str,
    table_name: str,
    dry_run: bool,
    operation: Callable[[], Dict[str, Any]],
) -> Dict[str, Any]:
    start = time.perf_counter()
    outcome: Dict[str, Any] = {}
    error: Optional[BaseException] = None
    try:
        outcome = operation()
        return outcome
    except BaseException as exc:  # pragma: no cover - propagate after metrics
        error = exc
        raise
    finally:
        duration_ms = (time.perf_counter() - start) * 1000.0
        _emit_metrics(action, table_name, dry_run, duration_ms, outcome, error)


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


def _as_count(value: Any) -> int:
    if isinstance(value, int):
        return value
    if value is None:
        return 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value)
    try:
        return len(list(value))
    except Exception:
        return 0


def expire_snapshots(
    table_name: str,
    *,
    older_than_days: int = 14,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Expire snapshots older than the provided threshold."""
    threshold = datetime.now(timezone.utc) - timedelta(days=older_than_days)
    expire_ts_ms = int(threshold.timestamp() * 1000)

    def _operation() -> Dict[str, Any]:
        table = _load_catalog_table(table_name)

        if dry_run:
            eligible = _collect_expired_snapshot_ids(table, expire_ts_ms)
            LOGGER.info(
                "Dry run: %s snapshot(s) eligible for expiry on %s",
                len(eligible),
                table_name,
            )
            return {
                "dry_run": True,
                "eligible_snapshots": len(eligible),
                "older_than_days": older_than_days,
            }

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
            "older_than_days": older_than_days,
        }

    return _run_with_metrics("expire_snapshots", table_name, dry_run, _operation)


def rewrite_data_files(
    table_name: str,
    *,
    target_file_size_mb: int = 128,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Compact small data files using Iceberg's rewrite action."""

    size_bytes = max(target_file_size_mb, 16) * 1024 * 1024

    def _operation() -> Dict[str, Any]:
        table = _load_catalog_table(table_name)
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

        if dry_run:
            LOGGER.info(
                "Dry run: would compact files on %s to ~%s MB",
                table_name,
                target_file_size_mb,
            )
            return {
                "dry_run": True,
                "target_file_size_mb": target_file_size_mb,
            }

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

    return _run_with_metrics("rewrite_data_files", table_name, dry_run, _operation)


def rewrite_manifests(
    table_name: str,
    *,
    min_count_to_merge: int = 4,
    max_group_size_mb: int = 512,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Rewrite manifest files to reduce metadata fragmentation."""

    min_count = max(1, min_count_to_merge)
    max_group_bytes = max(max_group_size_mb, 32) * 1024 * 1024

    def _operation() -> Dict[str, Any]:
        table = _load_catalog_table(table_name)
        action = getattr(table, "rewrite_manifests", None)
        if action is None:
            raise RuntimeError("rewrite_manifests action not available on table")

        builder = action()

        if hasattr(builder, "with_min_input_files"):
            builder = builder.with_min_input_files(min_count)
        elif hasattr(builder, "options"):
            builder = builder.options(min_input_files=min_count)

        # Prefer explicit configuration when supported by the runtime
        if hasattr(builder, "with_target_size_in_bytes"):
            builder = builder.with_target_size_in_bytes(max_group_bytes)
        elif hasattr(builder, "with_max_file_group_size_in_bytes"):
            builder = builder.with_max_file_group_size_in_bytes(max_group_bytes)
        elif hasattr(builder, "options"):
            builder = builder.options(max_file_group_size_in_bytes=max_group_bytes)

        if dry_run:
            LOGGER.info(
                "Dry run: would rewrite manifests on %s (min_count=%s, max_group_mb=%s)",
                table_name,
                min_count,
                max_group_size_mb,
            )
            return {
                "dry_run": True,
                "min_count_to_merge": min_count,
                "max_group_size_mb": max_group_size_mb,
            }

        result = builder.execute()
        added = _as_count(getattr(result, "added_manifests", None))
        if added == 0:
            added = _as_count(getattr(result, "added_manifest_files", None))
        deleted = _as_count(getattr(result, "deleted_manifests", None))
        if deleted == 0:
            deleted = _as_count(getattr(result, "deleted_manifest_files", None))

        LOGGER.info(
            "Rewrote manifests on %s (added=%s, deleted=%s)",
            table_name,
            added,
            deleted,
        )
        return {
            "added_manifests": added,
            "deleted_manifests": deleted,
            "min_count_to_merge": min_count,
            "max_group_size_mb": max_group_size_mb,
        }

    return _run_with_metrics("rewrite_manifests", table_name, dry_run, _operation)


def purge_orphan_files(
    table_name: str,
    *,
    older_than_hours: int = 24,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Remove orphan data files that are older than the configured threshold."""

    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(older_than_hours, 1))

    def _operation() -> Dict[str, Any]:
        table = _load_catalog_table(table_name)
        action = getattr(table, "remove_orphan_files", None)
        if action is None:
            raise RuntimeError("remove_orphan_files action not available on table")

        if dry_run:
            LOGGER.info(
                "Dry run: would purge orphan files older than %s hour(s) on %s",
                older_than_hours,
                table_name,
            )
            return {
                "dry_run": True,
                "older_than_hours": older_than_hours,
            }

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

        removed_count = _as_count(removed_candidates)

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

    return _run_with_metrics("purge_orphan_files", table_name, dry_run, _operation)
