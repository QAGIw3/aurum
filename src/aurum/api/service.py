from __future__ import annotations

"""Service layer for curve queries backed by Trino, with optional Redis caching."""

import hashlib
import json
import logging
import time
from calendar import monthrange
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .config import CacheConfig, TrinoConfig

LOGGER = logging.getLogger(__name__)


def _require_trino():
    try:
        from trino.dbapi import connect  # type: ignore
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise RuntimeError(
            "The 'trino' package is required for API queries. Install via 'pip install aurum[api]'."
        ) from exc
    return connect


def _maybe_redis_client(cache_cfg: CacheConfig):
    try:  # pragma: no cover - exercised in integration
        import redis  # type: ignore
    except ModuleNotFoundError:
        return None

    try:
        if cache_cfg.mode == "sentinel" and cache_cfg.sentinel_endpoints and cache_cfg.sentinel_master:
            from redis.sentinel import Sentinel  # type: ignore

            sentinel = Sentinel(
                cache_cfg.sentinel_endpoints,
                username=cache_cfg.username,
                password=cache_cfg.password,
                socket_timeout=2.0,
            )
            client = sentinel.master_for(cache_cfg.sentinel_master, db=cache_cfg.db)
        elif cache_cfg.mode == "cluster" and cache_cfg.cluster_nodes:
            try:
                from redis.cluster import RedisCluster  # type: ignore
            except Exception:
                LOGGER.warning("Redis cluster mode requested but redis-py cluster support is unavailable")
                return None

            startup_nodes = []
            for node in cache_cfg.cluster_nodes:
                host, _, port = node.partition(":")
                if not host:
                    continue
                try:
                    startup_nodes.append({"host": host, "port": int(port or "6379")})
                except ValueError:
                    continue
            if not startup_nodes:
                return None
            client = RedisCluster(
                startup_nodes=startup_nodes,
                username=cache_cfg.username,
                password=cache_cfg.password,
                decode_responses=False,
            )
        elif cache_cfg.redis_url:
            client = redis.Redis.from_url(
                cache_cfg.redis_url,
                username=cache_cfg.username,
                password=cache_cfg.password,
                db=cache_cfg.db,
            )
        else:
            return None
        client.ping()
        return client
    except Exception:
        LOGGER.debug("Redis client initialization failed", exc_info=True)
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

SCENARIO_OUTPUT_ORDER_COLUMNS = [
    "scenario_id",
    "curve_key",
    "tenor_label",
    "contract_month",
    "metric",
    "run_id",
]

EIA_SERIES_ORDER_COLUMNS = [
    "series_id",
    "period_start",
    "period",
]


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
    if column in {"contract_month", "asof_date"}:
        if value:
            return f"DATE '{_safe_literal(str(value))}'"
        return "DATE '0001-01-01'"
    if column in {"period_start", "period_end", "ingest_ts"}:
        if isinstance(value, datetime):
            iso = value.isoformat(sep=" ", timespec="microseconds")
        elif value:
            iso = str(value)
        else:
            iso = "0001-01-01 00:00:00"
        return f"TIMESTAMP '{_safe_literal(iso)}'"
    safe_val = _safe_literal(str(value or ""))
    return f"'{safe_val}'"


def _build_keyset_clause(
    cursor: Optional[Dict[str, Any]],
    *,
    alias: str = "",
    order_columns: Iterable[str],
    comparison: str = ">",
) -> str:
    if not cursor:
        return ""

    alias_prefix = alias
    if alias_prefix and not alias_prefix.endswith("."):
        alias_prefix = f"{alias_prefix}."

    clauses: List[str] = []
    columns = list(order_columns)

    for idx, column in enumerate(columns):
        if column not in cursor:
            continue
        literal = _literal_for_column(column, cursor.get(column))
        expr = _order_expression(column, alias=alias_prefix)
        base_condition = f"{expr} {comparison} {literal}"
        if idx == 0:
            clauses.append(base_condition)
            continue
        equals_chain: List[str] = []
        for prev in columns[:idx]:
            prev_literal = _literal_for_column(prev, cursor.get(prev))
            prev_expr = _order_expression(prev, alias=alias_prefix)
            equals_chain.append(f"{prev_expr} = {prev_literal}")
        chain = " AND ".join(equals_chain + [base_condition])
        clauses.append(f"({chain})")

    if not clauses:
        return ""

    return " AND (" + " OR ".join(clauses) + ")"


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
    cursor_after: Optional[Dict[str, Any]],
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
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

    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in ORDER_COLUMNS)
    comparison_cursor = cursor_after
    comparison = ">"
    if cursor_before:
        comparison_cursor = cursor_before
        comparison = "<"
        offset = 0

    if asof:
        asof_clause = f"asof_date = DATE '{asof.isoformat()}'"
        where_final = where + (" AND " if where else " WHERE ") + asof_clause
        where_final += _build_keyset_clause(
            comparison_cursor,
            alias="",
            order_columns=ORDER_COLUMNS,
            comparison=comparison,
        )
        effective_offset = 0 if comparison_cursor else offset
        return (
            f"SELECT {select_cols} FROM {base}{where_final}{order_clause} "
            f"LIMIT {limit} OFFSET {effective_offset}"
        )

    inner = (
        f"SELECT {select_cols}, "
        f"row_number() OVER (PARTITION BY curve_key, tenor_label ORDER BY asof_date DESC, _ingest_ts DESC) rn "
        f"FROM {base}{where}"
    )
    keyset_clause = _build_keyset_clause(
        comparison_cursor,
        alias="t",
        order_columns=ORDER_COLUMNS,
        comparison=comparison,
    )
    effective_offset = 0 if comparison_cursor else offset
    return (
        "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
        f"FROM ({inner}) t WHERE rn = 1{keyset_clause}{order_clause} "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )


def _cache_key(params: Dict[str, Any]) -> str:
    payload = json.dumps(params, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _format_timestamp_literal(value: datetime) -> str:
    return value.isoformat(sep=" ", timespec="microseconds")


def _build_sql_eia_series(
    *,
    series_id: Optional[str],
    frequency: Optional[str],
    area: Optional[str],
    sector: Optional[str],
    dataset: Optional[str],
    unit: Optional[str],
    source: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]],
    cursor_before: Optional[Dict[str, Any]],
    descending: bool,
) -> str:
    base = "timescale.public.eia_series_timeseries"
    conditions: list[str] = []
    if series_id:
        conditions.append(f"series_id = '{_safe_literal(series_id)}'")
    if frequency:
        conditions.append(f"upper(frequency) = '{_safe_literal(frequency.upper())}'")
    if area:
        conditions.append(f"area = '{_safe_literal(area)}'")
    if sector:
        conditions.append(f"sector = '{_safe_literal(sector)}'")
    if dataset:
        conditions.append(f"dataset = '{_safe_literal(dataset)}'")
    if unit:
        conditions.append(f"unit = '{_safe_literal(unit)}'")
    if source:
        conditions.append(f"source = '{_safe_literal(source)}'")
    if start:
        conditions.append(f"period_start >= TIMESTAMP '{_safe_literal(_format_timestamp_literal(start))}'")
    if end:
        conditions.append(f"period_start <= TIMESTAMP '{_safe_literal(_format_timestamp_literal(end))}'")

    where = ""
    if conditions:
        where = " WHERE " + " AND ".join(conditions)

    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in EIA_SERIES_ORDER_COLUMNS)

    comparison_cursor = cursor_after
    comparison = ">"
    if cursor_before:
        comparison_cursor = cursor_before
        comparison = "<"
        offset = 0

    keyset_clause = _build_keyset_clause(
        comparison_cursor,
        alias="",
        order_columns=EIA_SERIES_ORDER_COLUMNS,
        comparison=comparison,
    )
    where_final = where
    if keyset_clause:
        if where_final:
            where_final += keyset_clause
        else:
            where_final = " WHERE 1=1" + keyset_clause

    effective_offset = 0 if comparison_cursor else offset
    select_cols = (
        "series_id, period, period_start, period_end, frequency, value, raw_value, unit, "
        "canonical_unit, canonical_currency, canonical_value, conversion_factor, "
        "area, sector, seasonal_adjustment, description, source, dataset, metadata, ingest_ts"
    )
    return (
        f"SELECT {select_cols} FROM {base}{where_final}{order_clause} "
        f"LIMIT {limit} OFFSET {effective_offset}"
    )


def query_eia_series(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    series_id: Optional[str],
    frequency: Optional[str],
    area: Optional[str],
    sector: Optional[str],
    dataset: Optional[str],
    unit: Optional[str],
    source: Optional[str],
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int,
    offset: int = 0,
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> Tuple[List[Dict[str, Any]], float]:
    params = {
        "series_id": series_id,
        "frequency": frequency,
        "area": area,
        "sector": sector,
        "dataset": dataset,
        "unit": unit,
        "source": source,
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "limit": limit,
        "offset": offset,
        "cursor_after": cursor_after,
        "cursor_before": cursor_before,
        "descending": descending,
    }
    sql = _build_sql_eia_series(
        series_id=series_id,
        frequency=frequency,
        area=area,
        sector=sector,
        dataset=dataset,
        unit=unit,
        source=source,
        start=start,
        end=end,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
        cursor_before=cursor_before,
        descending=descending,
    )

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    if client is not None:
        cache_key = f"{prefix}eia-series:{_cache_key({**params, 'sql': sql})}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover
            data = json.loads(cached)
            return data, 0.0

    connect = _require_trino()
    start_time = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        columns = [c[0] for c in cur.description]
        for rec in cur.fetchall():
            row = {col: val for col, val in zip(columns, rec)}
            rows.append(row)
    elapsed = (time.perf_counter() - start_time) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
        except Exception:
            pass

    return rows, elapsed


def query_eia_series_dimensions(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    series_id: Optional[str],
    frequency: Optional[str],
    area: Optional[str],
    sector: Optional[str],
    dataset: Optional[str],
    unit: Optional[str],
    source: Optional[str],
) -> Tuple[Dict[str, List[str]], float]:
    conditions: list[str] = []
    if series_id:
        conditions.append(f"series_id = '{_safe_literal(series_id)}'")
    if frequency:
        conditions.append(f"upper(frequency) = '{_safe_literal(frequency.upper())}'")
    if area:
        conditions.append(f"area = '{_safe_literal(area)}'")
    if sector:
        conditions.append(f"sector = '{_safe_literal(sector)}'")
    if dataset:
        conditions.append(f"dataset = '{_safe_literal(dataset)}'")
    if unit:
        conditions.append(f"unit = '{_safe_literal(unit)}'")
    if source:
        conditions.append(f"source = '{_safe_literal(source)}'")

    where = ""
    if conditions:
        where = " WHERE " + " AND ".join(conditions)

    sql = (
        "SELECT "
        "ARRAY_AGG(DISTINCT dataset) FILTER (WHERE dataset IS NOT NULL) AS dataset_values, "
        "ARRAY_AGG(DISTINCT area) FILTER (WHERE area IS NOT NULL) AS area_values, "
        "ARRAY_AGG(DISTINCT sector) FILTER (WHERE sector IS NOT NULL) AS sector_values, "
        "ARRAY_AGG(DISTINCT unit) FILTER (WHERE unit IS NOT NULL) AS unit_values, "
        "ARRAY_AGG(DISTINCT canonical_unit) FILTER (WHERE canonical_unit IS NOT NULL) AS canonical_unit_values, "
        "ARRAY_AGG(DISTINCT canonical_currency) FILTER (WHERE canonical_currency IS NOT NULL) AS canonical_currency_values, "
        "ARRAY_AGG(DISTINCT frequency) FILTER (WHERE frequency IS NOT NULL) AS frequency_values, "
        "ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) AS source_values "
        f"FROM timescale.public.eia_series_timeseries{where}"
    )

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    params = {
        "series_id": series_id,
        "frequency": frequency,
        "area": area,
        "sector": sector,
        "dataset": dataset,
        "unit": unit,
        "source": source,
    }
    if client is not None:
        cache_key = f"{prefix}eia-series-dimensions:{_cache_key({**params, 'sql': sql})}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover
            payload = json.loads(cached)
            return payload, 0.0

    connect = _require_trino()
    start_time = time.perf_counter()
    results: Dict[str, List[str]] = {
        "dataset": [],
        "area": [],
        "sector": [],
        "unit": [],
        "canonical_unit": [],
        "canonical_currency": [],
        "frequency": [],
        "source": [],
    }
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row:
            mapping = {
                "dataset": row[0],
                "area": row[1],
                "sector": row[2],
                "unit": row[3],
                "canonical_unit": row[4],
                "canonical_currency": row[5],
                "frequency": row[6],
                "source": row[7],
            }
            for key, values in mapping.items():
                if not values:
                    continue
                items = [str(item) for item in values if item is not None]
                results[key] = sorted(set(items))
    elapsed = (time.perf_counter() - start_time) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(results, default=str))
        except Exception:
            pass

    return results, elapsed


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
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
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
        "cursor_after": cursor_after,
        "cursor_before": cursor_before,
        "descending": descending,
    }
    sql = _build_sql(
        asof=asof,
        curve_key=curve_key,
        asset_class=asset_class,
        iso=iso,
        location=location,
        market=market,
        product=product,
        block=block,
        tenor_type=tenor_type,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
        cursor_before=cursor_before,
        descending=descending,
    )

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
    cursor_after: Optional[Dict[str, Any]],
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
    where_final = (
        where + f" AND asof_date IN {asof_in}" if where else f" WHERE asof_date IN {asof_in}"
    )
    cte = (
        "WITH base AS ("
        " SELECT curve_key, tenor_label, tenor_type, cast(contract_month as date) as contract_month, "
        "        cast(asof_date as date) as asof_date, mid"
        f" FROM {base}{where_final}"
        ")"
    )
    keyset_clause = _build_keyset_clause(cursor_after, alias="a", order_columns=DIFF_ORDER_COLUMNS)
    effective_offset = 0 if cursor_after else offset
    sql = (
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
    cursor_after: Optional[Dict[str, Any]] = None,
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
        "cursor_after": cursor_after,
    }
    sql = _build_sql_diff(
        asof_a=asof_a,
        asof_b=asof_b,
        curve_key=curve_key,
        asset_class=asset_class,
        iso=iso,
        location=location,
        market=market,
        product=product,
        block=block,
        tenor_type=tenor_type,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
    )

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
            row.pop("tenant_id", None)
            attribution_val = row.get("attribution")
            if isinstance(attribution_val, str):
                try:
                    row["attribution"] = json.loads(attribution_val)
                except json.JSONDecodeError:
                    row["attribution"] = None
            rows.append(row)
    elapsed = (time.perf_counter() - start) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
            index_key = _scenario_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
            client.sadd(index_key, cache_key)
            client.expire(index_key, cache_cfg.ttl_seconds)
        except Exception:
            pass

    return rows, elapsed


def _scenario_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    tenant_key = tenant_id or "anon"
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}scenario-outputs:index:{tenant_key}:{scenario_id}"


def _scenario_metrics_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    tenant_key = tenant_id or "anon"
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}scenario-metrics:index:{tenant_key}:{scenario_id}"


def _build_sql_scenario_outputs(
    *,
    tenant_id: str,
    scenario_id: str,
    curve_key: Optional[str],
    tenor_type: Optional[str],
    metric: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]],
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> str:
    base = "iceberg.market.scenario_output_latest"
    filters: Dict[str, Optional[str]] = {
        "tenant_id": tenant_id,
        "scenario_id": scenario_id,
        "curve_key": curve_key,
        "tenor_type": tenor_type,
        "metric": metric,
    }
    where = _build_where(filters)
    if not where:
        where = " WHERE 1 = 1"
    comparison_cursor = cursor_after
    comparison = ">"
    if cursor_before:
        comparison_cursor = cursor_before
        comparison = "<"
        offset = 0
    where += _build_keyset_clause(
        comparison_cursor,
        alias="",
        order_columns=SCENARIO_OUTPUT_ORDER_COLUMNS,
        comparison=comparison,
    )
    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(f"{col} {direction}" for col in SCENARIO_OUTPUT_ORDER_COLUMNS)
    effective_offset = 0 if comparison_cursor else offset
    return (
        "SELECT tenant_id, scenario_id, run_id, cast(asof_date as date) as asof_date, curve_key, tenor_type, "
        "cast(contract_month as date) as contract_month, tenor_label, metric, value, band_lower, band_upper, "
        "attribution, version_hash "
        f"FROM {base}{where}{order_clause} LIMIT {limit} OFFSET {effective_offset}"
    )


def query_scenario_outputs(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    tenant_id: str,
    scenario_id: str,
    curve_key: Optional[str],
    tenor_type: Optional[str],
    metric: Optional[str],
    limit: int,
    offset: int = 0,
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> Tuple[List[Dict[str, Any]], float]:
    if not tenant_id:
        raise ValueError("tenant_id is required")

    params = {
        "tenant_id": tenant_id,
        "scenario_id": scenario_id,
        "curve_key": curve_key,
        "tenor_type": tenor_type,
        "metric": metric,
        "limit": limit,
        "offset": offset,
        "cursor_after": cursor_after,
        "cursor_before": cursor_before,
        "descending": descending,
    }
    sql = _build_sql_scenario_outputs(
        tenant_id=tenant_id,
        scenario_id=scenario_id,
        curve_key=curve_key,
        tenor_type=tenor_type,
        metric=metric,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
        cursor_before=cursor_before,
        descending=descending,
    )

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    if client is not None:
        cache_key = f"{prefix}scenario-outputs:{_cache_key({**params, 'sql': sql})}"
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
            index_key = _scenario_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
            client.sadd(index_key, cache_key)
            client.expire(index_key, cache_cfg.ttl_seconds)
        except Exception:
            pass

    return rows, elapsed


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _coerce_date(value: Any, fallback: date) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError:
            return fallback
    return fallback


def _month_end(day: date) -> date:
    last_day = monthrange(day.year, day.month)[1]
    return day.replace(day=last_day)


def _month_offset(start: date, end: date) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _extract_currency(row: Dict[str, Any]) -> Optional[str]:
    currency = row.get("metric_currency")
    if currency:
        currency_str = str(currency).strip()
        if currency_str:
            return currency_str
    unit = row.get("metric_unit")
    if unit:
        parts = str(unit).split("/", 1)
        candidate = parts[0].strip()
        if candidate:
            return candidate
    return None


def _compute_irr(cashflows: List[float], *, tolerance: float = 1e-6, max_iterations: int = 80) -> Optional[float]:
    if not cashflows:
        return None
    if all(cf >= 0 for cf in cashflows) or all(cf <= 0 for cf in cashflows):
        return None

    def npv(rate: float) -> float:
        total = 0.0
        for idx, cf in enumerate(cashflows):
            total += cf / (1.0 + rate) ** idx
        return total

    low, high = -0.9999, 10.0
    f_low = npv(low)
    f_high = npv(high)
    if f_low == 0:
        return low
    if f_high == 0:
        return high
    if f_low * f_high > 0:
        return None

    mid = (low + high) / 2.0
    for _ in range(max_iterations):
        mid = (low + high) / 2.0
        f_mid = npv(mid)
        if abs(f_mid) < tolerance:
            return mid
        if f_low * f_mid < 0:
            high = mid
            f_high = f_mid
        else:
            low = mid
            f_low = f_mid
    return mid


def query_ppa_valuation(
    trino_cfg: TrinoConfig,
    *,
    scenario_id: str,
    tenant_id: Optional[str] = None,
    asof_date: Optional[date] = None,
    metric: Optional[str] = "mid",
    options: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    base = "mart_scenario_output"
    filters: Dict[str, Optional[str]] = {"scenario_id": scenario_id}
    if tenant_id:
        filters["tenant_id"] = tenant_id
    target_metric = metric or "mid"
    filters["metric"] = target_metric
    where = _build_where(filters)
    if asof_date:
        clause = f"asof_date = DATE '{asof_date.isoformat()}'"
        where = where + (" AND " if where else " WHERE ") + clause

    sql = (
        "SELECT cast(contract_month as date) as contract_month, "
        "cast(asof_date as date) as asof_date, tenor_label, metric, value, "
        "metric_currency, metric_unit, metric_unit_denominator, curve_key, tenor_type, run_id "
        f"FROM {base}{where} "
        "ORDER BY contract_month NULLS LAST, tenor_label"
    )

    connect = _require_trino()
    start = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        columns = [col[0] for col in cur.description]
        for rec in cur.fetchall():
            rows.append({col: val for col, val in zip(columns, rec)})
    elapsed = (time.perf_counter() - start) * 1000.0

    if not rows:
        return [], elapsed

    opts = options or {}
    ppa_price = _coerce_float(opts.get("ppa_price"), 0.0)
    volume = _coerce_float(opts.get("volume_mwh"), 1.0)
    discount_rate = _coerce_float(opts.get("discount_rate"), 0.0)
    upfront_cost = _coerce_float(opts.get("upfront_cost"), 0.0)
    if discount_rate <= -1.0:
        discount_rate = 0.0
    monthly_rate = (1.0 + discount_rate) ** (1.0 / 12.0) - 1.0 if discount_rate else 0.0

    fallback_date = asof_date or date.today()
    base_period: Optional[date] = None
    latest_period: Optional[date] = None
    npv_total = -upfront_cost
    cashflows_by_offset: Dict[int, float] = {}
    metrics_out: List[Dict[str, Any]] = []
    currency_hint: Optional[str] = None
    curve_hint: Optional[str] = None
    run_hint: Optional[str] = None
    tenor_hint: Optional[str] = None

    for row in rows:
        if str(row.get("metric")) != target_metric:
            continue
        period_candidate = row.get("contract_month") or row.get("asof_date") or fallback_date
        period = _coerce_date(period_candidate, fallback_date)
        base_period = base_period or period
        latest_period = period
        offset = _month_offset(base_period, period) + 1
        price_value = _coerce_float(row.get("value"), 0.0)
        cashflow_value = (price_value - ppa_price) * volume
        cashflows_by_offset[offset] = cashflows_by_offset.get(offset, 0.0) + cashflow_value
        discount_factor = (1.0 + monthly_rate) ** offset if monthly_rate else 1.0
        npv_total += cashflow_value / discount_factor
        currency = _extract_currency(row)
        if currency:
            currency_hint = currency_hint or currency
        curve = row.get("curve_key")
        if curve:
            curve_hint = curve_hint or curve
        run_id = row.get("run_id")
        if run_id:
            run_hint = run_hint or run_id
        tenor = row.get("tenor_type")
        if tenor:
            tenor_hint = tenor_hint or tenor
        metrics_out.append(
            {
                "period_start": period,
                "period_end": _month_end(period),
                "metric": "cashflow",
                "value": round(cashflow_value, 4),
                "currency": currency or currency_hint,
                "unit": (currency or currency_hint),
                "curve_key": curve or curve_hint,
                "run_id": row.get("run_id") or run_hint,
                "tenor_type": row.get("tenor_type") or tenor_hint,
            }
        )

    if not metrics_out:
        return [], elapsed

    summary_start = base_period or fallback_date
    summary_end = _month_end(latest_period) if latest_period else summary_start
    currency_summary = currency_hint or metrics_out[0].get("currency")
    metrics_out.append(
        {
            "period_start": summary_start,
            "period_end": summary_end,
            "metric": "NPV",
            "value": round(npv_total, 4),
            "currency": currency_summary,
            "unit": currency_summary,
            "curve_key": curve_hint,
            "run_id": run_hint,
            "tenor_type": tenor_hint,
        }
    )

    if cashflows_by_offset:
        max_offset = max(cashflows_by_offset)
        series = [-upfront_cost] + [cashflows_by_offset.get(idx, 0.0) for idx in range(1, max_offset + 1)]
        irr = _compute_irr(series)
        if irr is not None:
            metrics_out.append(
                {
                    "period_start": summary_start,
                    "period_end": summary_end,
                    "metric": "IRR",
                    "value": round(irr, 6),
                    "currency": None,
                    "unit": "ratio",
                    "curve_key": curve_hint,
                    "run_id": run_hint,
                    "tenor_type": tenor_hint,
                }
            )

    return metrics_out, elapsed


def query_scenario_metrics_latest(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    scenario_id: str,
    tenant_id: Optional[str] = None,
    metric: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    base = "iceberg.market.scenario_output_latest_by_metric"
    filters: Dict[str, Optional[str]] = {"scenario_id": scenario_id}
    if tenant_id:
        filters["tenant_id"] = tenant_id
    if metric:
        filters["metric"] = metric
    where = _build_where(filters)
    sql = (
        "SELECT tenant_id, scenario_id, curve_key, metric, tenor_label, "
        "latest_value, latest_band_lower, latest_band_upper, "
        "cast(latest_asof_date as date) as latest_asof_date "
        f"FROM {base}{where} ORDER BY metric, tenor_label"
    )

    params = {
        "scenario_id": scenario_id,
        "tenant_id": tenant_id,
        "metric": metric,
        "sql": sql,
    }

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    if client is not None:
        cache_key = f"scenario-metrics:{_cache_key(params)}"
        cached = client.get(cache_key)
        if cached:
            data = json.loads(cached)
            return data, 0.0

    connect = _require_trino()
    start = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        cur.execute(sql)
        columns = [col[0] for col in cur.description]
        for rec in cur.fetchall():
            rows.append({col: val for col, val in zip(columns, rec)})
    elapsed = (time.perf_counter() - start) * 1000.0

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
            index_key = _scenario_metrics_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
            client.sadd(index_key, cache_key)
            client.expire(index_key, cache_cfg.ttl_seconds)
        except Exception:
            pass

    return rows, elapsed


def invalidate_scenario_outputs_cache(
    cache_cfg: CacheConfig,
    tenant_id: Optional[str],
    scenario_id: str,
) -> None:
    client = _maybe_redis_client(cache_cfg)
    if client is None:
        return
    index_key = _scenario_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
    metrics_index_key = _scenario_metrics_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
    try:  # pragma: no cover
        members = client.smembers(index_key)
        if members:
            keys = [member.decode("utf-8") if isinstance(member, bytes) else member for member in members]
            if keys:
                client.delete(*keys)
        client.delete(index_key)
        metric_members = client.smembers(metrics_index_key)
        if metric_members:
            metric_keys = [member.decode("utf-8") if isinstance(member, bytes) else member for member in metric_members]
            if metric_keys:
                client.delete(*metric_keys)
        client.delete(metrics_index_key)
    except Exception:
        return


__all__ = [
    "query_curves",
    "query_curves_diff",
    "query_scenario_outputs",
    "query_eia_series",
    "query_eia_series_dimensions",
    "query_ppa_valuation",
    "query_scenario_metrics_latest",
    "invalidate_scenario_outputs_cache",
    "TrinoConfig",
    "CacheConfig",
]

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
    include_counts: bool = False,
) -> Tuple[Dict[str, List[str]], Optional[Dict[str, List[Dict[str, Any]]]]]:
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
        "include_counts": include_counts,
    }
    if client is not None:
        cache_key = f"dimensions:{_cache_key(params)}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover
            payload = json.loads(cached)
            return payload.get("values", {}), payload.get("counts")

    connect = _require_trino()
    results: Dict[str, List[str]] = {}
    counts: Dict[str, List[Dict[str, Any]]] | None = {} if include_counts else None
    with connect(host=trino_cfg.host, port=trino_cfg.port, user=trino_cfg.user, http_scheme=trino_cfg.http_scheme) as conn:  # type: ignore[arg-type]
        cur = conn.cursor()
        for dim in dims:
            clause = where + (" AND " if where else " WHERE ") + f"{dim} IS NOT NULL"
            sql = f"SELECT DISTINCT {dim} FROM {base}{clause} LIMIT {per_dim_limit}"
            cur.execute(sql)
            values = [row[0] for row in cur.fetchall() if row and row[0] is not None]
            results[dim] = values
            if include_counts and counts is not None:
                count_sql = (
                    f"SELECT {dim} as value, COUNT(*) as count FROM {base}{clause} "
                    f"GROUP BY {dim} ORDER BY count DESC LIMIT {per_dim_limit}"
                )
                cur.execute(count_sql)
                counts[dim] = [
                    {"value": row[0], "count": int(row[1])}
                    for row in cur.fetchall()
                    if row and row[0] is not None
                ]

    if client is not None and cache_key is not None:
        try:  # pragma: no cover
            client.setex(
                cache_key,
                cache_cfg.ttl_seconds,
                json.dumps({"values": results, "counts": counts}, default=str),
            )
        except Exception:
            pass
    return results, counts if include_counts else None
