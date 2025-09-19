from __future__ import annotations

"""Service layer for curve queries backed by Trino, with optional Redis caching."""

import hashlib
import json
import time
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import CacheConfig, TrinoConfig


def _require_trino():
    try:
        from trino.dbapi import connect  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "The 'trino' package is required for API queries. Install via 'pip install aurum[api]'."
        ) from exc
    return connect


def _maybe_redis_client(cache_cfg: CacheConfig):
    if not cache_cfg.redis_url:
        return None
    try:  # pragma: no cover - exercised in integration
        import redis  # type: ignore

        return redis.Redis.from_url(cache_cfg.redis_url)
    except ModuleNotFoundError:
        return None


def _safe_literal(value: str) -> str:
    # conservative escaping for SQL literal
    return value.replace("'", "''")


def _build_where(filters: Dict[str, Optional[str]]) -> str:
    clauses: List[str] = []
    for col, val in filters.items():
        if val is None:
            continue
        clauses.append(f"{col} = '{_safe_literal(val)}'")
    if not clauses:
        return ""
    return " WHERE " + " AND ".join(clauses)


def _build_sql(
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
) -> str:
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
    where = _build_where(filters)
    select_cols = (
        "curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "cast(asof_date as date) as asof_date, mid, bid, ask, price_type"
    )

    order_clause = " ORDER BY curve_key, tenor_label"
    if asof:
        asof_clause = f"asof_date = DATE '{asof.isoformat()}'"
        where_final = where + (" AND " if where else " WHERE ") + asof_clause
        return f"SELECT {select_cols} FROM {base}{where_final}{order_clause} LIMIT {limit} OFFSET {offset}"

    # latest per (curve_key, tenor_label)
    inner = (
        f"SELECT {select_cols}, "
        f"row_number() OVER (PARTITION BY curve_key, tenor_label ORDER BY asof_date DESC, _ingest_ts DESC) rn "
        f"FROM {base}{where}"
    )
    return (
        "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
        f"FROM ({inner}) t WHERE rn = 1{order_clause} LIMIT {limit} OFFSET {offset}"
    )


def _cache_key(params: Dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def query_curves(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
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
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], float]:
    params = {
        "asof": asof,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
        "limit": limit,
        "offset": offset,
    }
    sql = _build_sql(**params)

    # try cache
    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    if client is not None:
        cache_key = f"curves:{_cache_key({**params, 'sql': sql})}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover - integration path
            data = json.loads(cached)
            return data, 0.0

    connect = _require_trino()
    start = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        columns = [c[0] for c in cur.description]
        for rec in cur.fetchall():
            row = {col: val for col, val in zip(columns, rec)}
            # normalize optional fields present in OpenAPI
            rows.append(row)
    elapsed = (time.perf_counter() - start) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover - integration path
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
        except Exception:
            pass

    return rows, elapsed


def _build_sql_diff(
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
) -> str:
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
    where = _build_where(filters)
    asof_in = (
        f"(DATE '{asof_a.isoformat()}', DATE '{asof_b.isoformat()}')"
    )
    # base CTE for both dates
    cte = (
        "WITH base AS ("
        " SELECT curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "        cast(asof_date as date) as asof_date, mid"
        f" FROM {base}{where} AND asof_date IN {asof_in}"
        ")"
    )
    sql = (
        f"{cte} "
        "SELECT a.curve_key, a.tenor_label, a.tenor_type, a.contract_month, "
        "a.asof_date as asof_a, a.mid as mid_a, "
        "b.asof_date as asof_b, b.mid as mid_b, "
        "(b.mid - a.mid) as diff_abs, "
        "CASE WHEN a.mid IS NOT NULL AND a.mid <> 0 THEN (b.mid - a.mid) / a.mid ELSE NULL END as diff_pct "
        "FROM base a JOIN base b ON a.curve_key = b.curve_key AND a.tenor_label = b.tenor_label "
        f"WHERE a.asof_date = DATE '{asof_a.isoformat()}' AND b.asof_date = DATE '{asof_b.isoformat()}' "
        "ORDER BY a.curve_key, a.tenor_label "
        f"LIMIT {limit} OFFSET {offset}"
    )
    return sql


def query_curves_diff(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
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
    offset: int = 0,
) -> Tuple[List[Dict[str, Any]], float]:
    params = {
        "asof_a": asof_a,
        "asof_b": asof_b,
        "curve_key": curve_key,
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
        "limit": limit,
        "offset": offset,
    }
    sql = _build_sql_diff(**params)

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    if client is not None:
        cache_key = f"curves-diff:{_cache_key({**params, 'sql': sql})}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover
            data = json.loads(cached)
            return data, 0.0

    connect = _require_trino()
    start = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        columns = [c[0] for c in cur.description]
        for rec in cur.fetchall():
            row = {col: val for col, val in zip(columns, rec)}
            rows.append(row)
    elapsed = (time.perf_counter() - start) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
        except Exception:
            pass

    return rows, elapsed


__all__ = ["query_curves", "query_curves_diff", "TrinoConfig", "CacheConfig"]
 
def query_dimensions(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    asof: Optional[date],
    asset_class: Optional[str],
    iso: Optional[str],
    location: Optional[str],
    market: Optional[str],
    product: Optional[str],
    block: Optional[str],
    tenor_type: Optional[str],
    per_dim_limit: int = 1000,
) -> Dict[str, List[str]]:
    filters: Dict[str, Optional[str]] = {
        "asset_class": asset_class,
        "iso": iso,
        "location": location,
        "market": market,
        "product": product,
        "block": block,
        "tenor_type": tenor_type,
    }
    where = _build_where(filters)
    base = "iceberg.market.curve_observation"
    if asof:
        asof_clause = f"asof_date = DATE '{asof.isoformat()}'"
        where = where + (" AND " if where else " WHERE ") + asof_clause

    dims = ["asset_class", "iso", "location", "market", "product", "block", "tenor_type"]

    cache_key = None
    client = _maybe_redis_client(cache_cfg)
    params = {
        "asof": asof.isoformat() if asof else None,
        **filters,
        "limit": per_dim_limit,
    }
    if client is not None:
        cache_key = f"dimensions:{_cache_key(params)}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover
            return json.loads(cached)

    connect = _require_trino()
    results: Dict[str, List[str]] = {}
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        for dim in dims:
            clause = where + (" AND " if where else " WHERE ") + f"{dim} IS NOT NULL"
            sql = f"SELECT DISTINCT {dim} FROM {base}{clause} LIMIT {per_dim_limit}"
            cur.execute(sql)
            values = [row[0] for row in cur.fetchall() if row and row[0] is not None]
            results[dim] = values

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(results))
        except Exception:
            pass
    return results
