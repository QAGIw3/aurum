from __future__ import annotations

"""DAO for v2 Curves listing backed by Trino.

Provides minimal queries to support v2 curves listing with pagination and an
optional name filter. The results are intentionally simple to avoid coupling to
domain specifics while we modularize the curves area.
"""

from typing import Any, Dict, List, Optional, Tuple

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
    offset: int,
    limit: int,
    name_filter: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return a list of curve summaries with paging and optional name filter."""

    client = get_trino_client()

    where = ""
    if name_filter:
        pattern = _safe_like(name_filter)
        where = f" WHERE lower(curve_key) LIKE lower('{pattern}')"

    # Summarize by curve_key; created_at uses the latest ingest timestamp
    sql = (
        "SELECT curve_key AS id, curve_key AS name, "
        "CAST(count(*) AS bigint) AS data_points, "
        "max(_ingest_ts) AS created_at "
        "FROM iceberg.market.curve_observation"
        f"{where} "
        "GROUP BY curve_key "
        "ORDER BY name ASC "
        f"LIMIT {max(1, int(limit))} OFFSET {max(0, int(offset))}"
    )

    rows = await client.execute_query(sql)
    return rows

