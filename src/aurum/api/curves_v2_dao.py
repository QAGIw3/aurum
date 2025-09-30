from __future__ import annotations

"""DAO for v2 Curves listing backed by Trino.

Provides minimal queries to support v2 curves listing with pagination and an
optional name filter. The results are intentionally simple to avoid coupling to
domain specifics while we modularize the curves area.
"""

from typing import Any, Dict, List, Optional

from aurum.core.settings import get_settings as _core_get_settings
from aurum.data import QueryResult

from .database.backend_selector import get_data_backend
from .database.trino_client import get_trino_client


def _safe_like(value: str) -> str:
    """Escape input for use inside a LIKE pattern.

    Doubles percent/underscore to avoid unintended wildcards and quotes to keep
    the literal safe. Intended for quick filtering; proper parameter binding is
    preferred where available.
    """
    escaped = value.replace("%", "%%").replace("_", "__").replace("'", "''")
    return f"%{escaped}%"


async def list_curves(
    *,
    tenant_id: str,
    offset: int,
    limit: int,
    name_filter: Optional[str] = None,
) -> QueryResult:
    """Return a list of curve summaries with paging and optional name filter."""

    where = ""
    filters = []
    if tenant_id:
        safe_tenant = tenant_id.replace("'", "''")
        filters.append(f"tenant_id = '{safe_tenant}'")
    if name_filter:
        pattern = _safe_like(name_filter)
        filters.append(f"lower(curve_key) LIKE lower('{pattern}')")

    if filters:
        where = " WHERE " + " AND ".join(filters)

    # Summarize by curve_key; created_at uses the latest ingest timestamp
    sql = (
        "SELECT tenant_id, curve_key AS id, curve_key AS name, "
        "CAST(count(*) AS bigint) AS data_points, "
        "max(_ingest_ts) AS created_at "
        "FROM iceberg.market.curve_observation"
        f"{where} "
        "GROUP BY tenant_id, curve_key "
        "ORDER BY tenant_id, name ASC "
        f"LIMIT {max(1, int(limit))} OFFSET {max(0, int(offset))}"
    )

    settings = _core_get_settings()

    try:
        backend = get_data_backend(settings)
    except Exception:
        backend = None

    if backend is not None:
        result = await backend.execute_query(sql)
        if isinstance(result, QueryResult):
            return result

    # Fallback to legacy Trino client (dict rows) when backend selector is unavailable
    client = get_trino_client()
    rows = await client.execute_query(sql)

    columns = ["tenant_id", "id", "name", "data_points", "created_at"]
    normalized_rows: List[tuple[Any, ...]] = []
    try:
        for row in rows:
            if isinstance(row, dict):
                normalized_rows.append(tuple(row.get(col) for col in columns))
            else:
                normalized_rows.append(tuple(row))
    except Exception:
        normalized_rows = [tuple(row) for row in rows]

    metadata = {
        "backend": "trino",
        "fallback": True,
    }
    return QueryResult(columns=columns, rows=normalized_rows, metadata=metadata)
