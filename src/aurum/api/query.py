from __future__ import annotations

"""SQL query builders for Aurum data services."""

from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, Optional

from .http.pagination import MAX_PAGE_SIZE


ORDER_COLUMNS = [
    "curve_key",
    "tenor_label",
    "contract_month",
    "asof_date",
    "price_type",
]

DIFF_ORDER_COLUMNS = [
    "curve_key",
    "tenor_label",
    "contract_month",
]


def _safe_literal(value: str) -> str:
    return value.replace("'", "''")


def build_filter_clause(filters: Dict[str, Optional[str]]) -> str:
    clauses = [f"{col} = '{_safe_literal(val)}'" for col, val in filters.items() if val is not None]
    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _order_expression(column: str, *, alias: str = "") -> str:
    qualified = f"{alias}{column}" if alias else column
    if column in {"contract_month", "asof_date"}:
        return f"coalesce(cast({qualified} as date), DATE '0001-01-01')"
    if column in {"period_start", "period_end", "ingest_ts"}:
        return f"coalesce(cast({qualified} as timestamp), TIMESTAMP '0001-01-01 00:00:00')"
    if column in {"tenor_label", "price_type"}:
        return f"coalesce({qualified}, '')"
    return qualified


def _literal_for_column(column: str, value: Any) -> str:
    if column in {"asof_date", "contract_month"}:
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return f"DATE '{_safe_literal(value.isoformat())}'"
        return "DATE '0001-01-01'"
    if column in {"period_start", "period_end", "ingest_ts"}:
        if isinstance(value, datetime):
            aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            iso = aware.astimezone(timezone.utc).isoformat(sep=" ", timespec="microseconds")
            return f"TIMESTAMP '{_safe_literal(iso)}'"
        if value:
            return f"TIMESTAMP '{_safe_literal(str(value))}'"
        return "TIMESTAMP '0001-01-01 00:00:00'"
    if isinstance(value, datetime):
        aware = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        iso = aware.astimezone(timezone.utc).isoformat(sep=" ", timespec="microseconds")
        return f"TIMESTAMP '{_safe_literal(iso)}'"
    safe_val = _safe_literal(str(value or ""))
    return f"'{safe_val}'"


def build_keyset_clause(
    cursor: Optional[Dict[str, Any]],
    *,
    alias: str = "",
    order_columns: Iterable[str] = ORDER_COLUMNS,
    comparison: str = ">",
) -> str:
    if not cursor:
        return ""
    alias_prefix = alias
    if alias_prefix and not alias_prefix.endswith("."):
        alias_prefix = f"{alias_prefix}."
    columns = list(order_columns)
    clauses = []
    for idx, column in enumerate(columns):
        if column not in cursor:
            continue
        literal = _literal_for_column(column, cursor.get(column))
        expr = _order_expression(column, alias=alias_prefix)
        base_condition = f"{expr} {comparison} {literal}"
        if idx == 0:
            clauses.append(base_condition)
            continue
        equals_chain = []
        for prev in columns[:idx]:
            prev_literal = _literal_for_column(prev, cursor.get(prev))
            prev_expr = _order_expression(prev, alias=alias_prefix)
            equals_chain.append(f"{prev_expr} = {prev_literal}")
        chain = " AND ".join(equals_chain + [base_condition])
        clauses.append(f"({chain})")
    if not clauses:
        return ""
    return " AND (" + " OR ".join(clauses) + ")"


def build_curve_query(
    *,
    asof: Optional[date],
    curve_key: Optional[str],
    asset_class: Optional[str],
    iso: Optional[str],
    location: Optional[str],
    market: Optional[str],
    product: Optional[str],
    block: Optional[str],
    tenor_type: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> str:
    limit = max(1, min(limit, MAX_PAGE_SIZE))
    base = "iceberg.market.curve_observation"
    filters: Dict[str, Optional[str]] = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where = build_filter_clause(filters)
    select_cols = (
        "curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "cast(asof_date as date) as asof_date, mid, bid, ask, price_type"
    )
    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in ORDER_COLUMNS)

    comparison_cursor = cursor_after
    comparison = ">"
    effective_offset = offset
    if cursor_before:
        comparison_cursor = cursor_before
        comparison = "<"
        effective_offset = 0

    if asof:
        asof_clause = f"asof_date = DATE '{asof.isoformat()}'"
        where_final = where + (" AND " if where else " WHERE ") + asof_clause
        where_final += build_keyset_clause(
            comparison_cursor,
            alias="",
            order_columns=ORDER_COLUMNS,
            comparison=comparison,
        )
        if comparison_cursor:
            effective_offset = 0
        return (
            f"SELECT {select_cols} FROM {base}{where_final}{order_clause} "
            f"LIMIT {limit} OFFSET {effective_offset}"
        )

    inner = (
        f"SELECT {select_cols}, "
        "row_number() OVER (PARTITION BY curve_key, tenor_label ORDER BY asof_date DESC, _ingest_ts DESC) rn "
        f"FROM {base}{where}"
    )
    keyset_clause = build_keyset_clause(
        comparison_cursor,
        alias="t",
        order_columns=ORDER_COLUMNS,
        comparison=comparison,
    )
    if comparison_cursor:
        effective_offset = 0
    return (
        "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
        f"FROM ({inner}) t WHERE rn = 1{keyset_clause}{order_clause} "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )


def build_curve_diff_query(
    *,
    asof_a: date,
    asof_b: date,
    curve_key: Optional[str],
    asset_class: Optional[str],
    iso: Optional[str],
    location: Optional[str],
    market: Optional[str],
    product: Optional[str],
    block: Optional[str],
    tenor_type: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]] = None,
) -> str:
    limit = max(1, min(limit, MAX_PAGE_SIZE))
    base = "iceberg.market.curve_observation"
    filters = {
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where = build_filter_clause(filters)
    asof_in = (
        f"(DATE '{asof_a.isoformat()}', DATE '{asof_b.isoformat()}')"
    )
    where_final = where + f" AND asof_date IN {asof_in}" if where else f" WHERE asof_date IN {asof_in}"
    cte = (
        "WITH base AS ("
        " SELECT curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "        cast(asof_date as date) as asof_date, mid"
        f" FROM {base}{where_final}"
        ")"
    )
    keyset_clause = build_keyset_clause(cursor_after, alias="a", order_columns=DIFF_ORDER_COLUMNS)
    effective_offset = 0 if cursor_after else offset
    return (
        f"{cte} "
        "SELECT a.curve_key, a.tenor_label, a.tenor_type, a.contract_month, "
        "a.asof_date as asof_a, a.mid as mid_a, "
        "b.asof_date as asof_b, b.mid as mid_b, "
        "(b.mid - a.mid) as diff_abs, "
        "CASE WHEN a.mid IS NOT NULL AND a.mid <> 0 THEN (b.mid - a.mid) / a.mid ELSE NULL END as diff_pct "
        "FROM base a JOIN base b ON a.curve_key = b.curve_key AND a.tenor_label = b.tenor_label "
        f"WHERE a.asof_date = DATE '{asof_a.isoformat()}' AND b.asof_date = DATE '{asof_b.isoformat()}' "
        f"{keyset_clause} "
        "ORDER BY a.curve_key, a.tenor_label, a.contract_month "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )
