"""Shared SQL query builders for service-layer usage."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Sequence


_CATALOG_SELECT_COLUMNS: tuple[str, ...] = (
    "tenant_id",
    "series_id",
    "provider",
    "dataset_code",
    "status",
    "iso_code",
    "iso_market",
    "iso_product",
    "iso_location_type",
    "iso_location_name",
    "iso_location_id",
    "canonical_region_id",
    "geography_type",
    "category",
    "start_ts",
    "end_ts",
    "last_observation_ts",
    "frequency",
    "tags",
)

_CATALOG_EQUAL_FILTERS: dict[str, str] = {
    "provider": "provider",
    "dataset_code": "dataset_code",
    "status": "status",
    "iso_code": "iso_code",
    "iso_market": "iso_market",
    "iso_product": "iso_product",
    "iso_location_type": "iso_location_type",
    "iso_location_name": "iso_location_name",
    "iso_location_id": "iso_location_id",
    "canonical_region_id": "canonical_region_id",
    "geography_type": "geography_type",
    "category": "category",
}

_CATALOG_ILIKE_FILTERS: dict[str, str] = {
    "series_id": "series_id",
    "provider_name": "provider_name",
    "dataset_name": "dataset_name",
}

_CATALOG_TIME_FILTERS: dict[str, tuple[str, str]] = {
    "start_ts_from": ("start_ts", ">="),
    "start_ts_to": ("start_ts", "<="),
    "end_ts_from": ("end_ts", ">="),
    "end_ts_to": ("end_ts", "<="),
    "last_obs_from": ("last_observation_ts", ">="),
    "last_obs_to": ("last_observation_ts", "<="),
}


def _safe_literal(value: str) -> str:
    return value.replace("'", "''")


def _escape_like_pattern(value: str) -> str:
    """Escape user supplied text for safe LIKE pattern usage."""

    return value.replace("\\", "\\\\").replace("%", "%%").replace("_", "__")


def _timestamp_literal(value: Any) -> str:
    """Serialise a Python value into a Trino timestamp literal."""

    if isinstance(value, datetime):
        iso = value.isoformat(sep=" ", timespec="microseconds")
    elif isinstance(value, date):
        iso = datetime(value.year, value.month, value.day).isoformat(sep=" ")
    else:
        iso = str(value)
    return f"TIMESTAMP '{_safe_literal(iso)}'"


def build_series_catalog_query(
    *,
    tenant_id: str,
    filters: Mapping[str, Any] | None,
    limit: int,
    offset: int,
    order_by: Sequence[str] | None = None,
) -> str:
    """Build a paginated query against the external series catalog view."""

    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    safe_tenant = _safe_literal(tenant_id)
    clauses = [f"tenant_id = '{safe_tenant}'"]
    filters = filters or {}

    for key, column in _CATALOG_EQUAL_FILTERS.items():
        value = filters.get(key)
        if value is None:
            continue
        clauses.append(f"{column} = '{_safe_literal(str(value))}'")

    for key, column in _CATALOG_ILIKE_FILTERS.items():
        value = filters.get(key)
        if not value:
            continue
        pattern = _escape_like_pattern(str(value))
        clauses.append(f"lower({column}) LIKE lower('%{pattern}%')")

    tags = filters.get("tags")
    if tags:
        for raw_tag in tags:
            if not raw_tag:
                continue
            clauses.append(f"contains(tags, '{_safe_literal(str(raw_tag))}')")

    for key, (column, operator) in _CATALOG_TIME_FILTERS.items():
        value = filters.get(key)
        if value is None:
            continue
        clauses.append(f"{column} {operator} {_timestamp_literal(value)}")

    where_clause = " WHERE " + " AND ".join(clauses)
    order_columns = list(order_by) if order_by else ["provider", "series_id"]
    select_list = ", ".join(_CATALOG_SELECT_COLUMNS)
    order_clause = ", ".join(order_columns)

    return (
        f"SELECT {select_list} "
        "FROM iceberg.market.external_series_catalog "
        f"{where_clause} "
        f"ORDER BY {order_clause} "
        f"LIMIT {limit} OFFSET {offset}"
    )


__all__ = ["build_series_catalog_query"]
