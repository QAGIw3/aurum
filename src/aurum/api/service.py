from __future__ import annotations

"""Service layer for curve queries backed by Trino, with optional Redis caching."""

import hashlib
import json
import logging
import os
import threading
import time
import asyncio
from calendar import monthrange
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency
    import psycopg
except ModuleNotFoundError:  # pragma: no cover - allow stubbing in tests
    psycopg = None  # type: ignore[assignment]

from aurum.core import AurumSettings
from aurum.core.pagination import Cursor as _Cursor
from aurum.telemetry import get_tracer

from .config import CacheConfig, TrinoConfig
from .trino_client import get_trino_client
from .query import (
    DIFF_ORDER_COLUMNS,
    build_curve_diff_query,
    build_curve_query,
    build_filter_clause,
    build_keyset_clause,
)
from .state import configure as configure_state, get_settings

LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as _PromCounter
except Exception:  # pragma: no cover - metrics optional
    _PromCounter = None  # type: ignore[assignment]


if _PromCounter:
    SCENARIO_CACHE_HITS = _PromCounter(
        "aurum_scenario_cache_hits_total",
        "Scenario cache hits",
    )
    SCENARIO_CACHE_MISSES = _PromCounter(
        "aurum_scenario_cache_misses_total",
        "Scenario cache misses",
    )
    SCENARIO_CACHE_INVALIDATIONS = _PromCounter(
        "aurum_scenario_cache_invalidations_total",
        "Scenario cache invalidations",
    )
else:  # pragma: no cover - metrics optional
    SCENARIO_CACHE_HITS = None  # type: ignore[assignment]
    SCENARIO_CACHE_MISSES = None  # type: ignore[assignment]
    SCENARIO_CACHE_INVALIDATIONS = None  # type: ignore[assignment]

# Generic service cache metrics (for non-ISO, non-scenario caches)
if _PromCounter:
    SERVICE_CACHE_COUNTER = _PromCounter(
        "aurum_service_cache_events_total",
        "Service cache events (hits/misses) by endpoint.",
        ["endpoint", "event"],
    )
else:  # pragma: no cover
    SERVICE_CACHE_COUNTER = None  # type: ignore[assignment]


class _Singleflight:
    """Simple singleflight helper to deduplicate concurrent fetches."""

    def __init__(self) -> None:
        self._locks: Dict[str, threading.Lock] = {}
        self._counts: Dict[str, int] = {}
        self._guard = threading.Lock()

    @contextmanager
    def acquire(self, key: str):
        with self._guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._locks[key] = lock
                self._counts[key] = 0
            self._counts[key] += 1
        lock.acquire()
        try:
            yield
        finally:
            lock.release()
            with self._guard:
                self._counts[key] -= 1
                if self._counts[key] <= 0:
                    self._locks.pop(key, None)
                    self._counts.pop(key, None)


_SCENARIO_SINGLEFLIGHT = _Singleflight()


def _settings() -> AurumSettings:
    try:
        return get_settings()
    except RuntimeError:
        settings = AurumSettings.from_env()
        configure_state(settings)
        return settings


def _timescale_dsn() -> str:
    return _settings().database.timescale_dsn


# Deprecated legacy helper removed; all Trino access goes through the pooled client


def _maybe_redis_client(cache_cfg: CacheConfig):
    try:  # pragma: no cover - exercised in integration
        import redis  # type: ignore
    except ModuleNotFoundError:
        LOGGER.debug("Redis package not available; caching disabled")
        return None

    socket_timeout = float(cache_cfg.socket_timeout)
    connect_timeout = float(cache_cfg.connect_timeout)
    client_kwargs = {
        "username": cache_cfg.username,
        "password": cache_cfg.password,
        "socket_timeout": socket_timeout,
        "socket_connect_timeout": connect_timeout,
        "socket_keepalive": True,
        "retry_on_timeout": True,
    }
    client_kwargs = {key: value for key, value in client_kwargs.items() if value is not None}

    def _log_skip(reason: str) -> None:
        LOGGER.debug("Skipping Redis client initialization: %s", reason)

    try:
        mode = (cache_cfg.mode or "standalone").lower()
        if mode == "sentinel":
            if not cache_cfg.sentinel_endpoints or not cache_cfg.sentinel_master:
                _log_skip("sentinel configuration incomplete")
                return None
            from redis.sentinel import Sentinel  # type: ignore

            sentinel = Sentinel(cache_cfg.sentinel_endpoints, **client_kwargs)
            client = sentinel.master_for(cache_cfg.sentinel_master, db=cache_cfg.db, **client_kwargs)
        elif mode == "cluster":
            if not cache_cfg.cluster_nodes:
                _log_skip("cluster startup nodes not provided")
                return None
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
                    LOGGER.debug("Ignoring invalid Redis cluster node definition: %s", node)
                    continue
            if not startup_nodes:
                LOGGER.warning("Redis cluster configuration yielded no usable startup nodes")
                return None
            cluster_kwargs = dict(client_kwargs)
            cluster_kwargs.setdefault("decode_responses", False)
            client = RedisCluster(startup_nodes=startup_nodes, **cluster_kwargs)
        elif cache_cfg.redis_url:
            client = redis.Redis.from_url(cache_cfg.redis_url, db=cache_cfg.db, **client_kwargs)
        else:
            _log_skip("no Redis URL or mode configuration provided")
            return None
        client.ping()
        LOGGER.debug("Redis client initialized using mode '%s'", mode)
        return client
    except Exception as exc:
        LOGGER.warning("Redis client initialization failed: %s", exc)
        LOGGER.debug("Redis client initialization failure details", exc_info=True)
        return None


def _eia_series_base_table() -> str:
    return _settings().database.eia_series_base_table


def _fetch_timescale_rows(sql: str, params: Mapping[str, object]) -> Tuple[List[Dict[str, Any]], float]:
    start_time = time.perf_counter()
    rows: List[Dict[str, Any]] = []
    if psycopg is None:  # pragma: no cover - guard when dependency missing
        raise RuntimeError("psycopg is required for Timescale queries")
    with psycopg.connect(_timescale_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [col[0] for col in cur.description]
            for record in cur.fetchall():
                row: Dict[str, Any] = {}
                for column, value in zip(columns, record):
                    if isinstance(value, datetime):
                        if value.tzinfo is None:
                            value = value.replace(tzinfo=timezone.utc)
                        row[column] = value
                    elif isinstance(value, date):
                        row[column] = value
                    elif column == "metadata" and isinstance(value, str):
                        try:
                            row[column] = json.loads(value)
                        except json.JSONDecodeError:
                            row[column] = None
                    else:
                        row[column] = value
                rows.append(row)
    elapsed_ms = (time.perf_counter() - start_time) * 1000.0
    return rows, elapsed_ms

SCENARIO_OUTPUT_ORDER_COLUMNS = [
    "scenario_id",
    "curve_key",
    "tenor_label",
    "contract_month",
    "metric",
    "run_id",
]

SCENARIO_METRIC_ORDER_COLUMNS = [
    "metric",
    "tenor_label",
    "curve_key",
]

EIA_SERIES_ORDER_COLUMNS = [
    "series_id",
    "period_start",
    "period",
]


def _build_where(filters: Dict[str, Optional[str]]) -> str:
    return build_filter_clause(filters)


def _safe_literal(value: str) -> str:
    return value.replace("'", "''")


def _build_keyset_clause(
    cursor: Optional[Dict[str, Any]],
    *,
    alias: str = "",
    order_columns: Iterable[str],
    comparison: str = ">",
) -> str:
    return build_keyset_clause(
        cursor,
        alias=alias,
        order_columns=order_columns,
        comparison=comparison,
    )


def _execute_trino_query(
    trino_cfg: TrinoConfig,
    query: str,
    params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Execute a Trino query using the unified client pool (blocking)."""
    client = get_trino_client(trino_cfg)
    return client.execute_query_sync(query, params=params, use_cache=True)


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
    return build_curve_query(
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
    canonical_unit: Optional[str] = None,
    canonical_currency: Optional[str] = None,
    source: Optional[str] = None,
    start: Optional[datetime],
    end: Optional[datetime],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]],
    cursor_before: Optional[Dict[str, Any]],
    descending: bool,
) -> str:
    base = _eia_series_base_table()
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
        conditions.append(f"unit_raw = '{_safe_literal(unit)}'")
    if canonical_unit:
        conditions.append(f"unit_normalized = '{_safe_literal(canonical_unit)}'")
    if canonical_currency:
        conditions.append(f"currency_normalized = '{_safe_literal(canonical_currency)}'")
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
        "series_id, period, period_start, period_end, frequency, value, raw_value, "
        "unit_raw AS unit, unit_normalized AS canonical_unit, "
        "currency_normalized AS canonical_currency, value AS canonical_value, conversion_factor, "
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
    canonical_unit: Optional[str] = None,
    canonical_currency: Optional[str] = None,
    source: Optional[str] = None,
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
        "canonical_unit": canonical_unit,
        "canonical_currency": canonical_currency,
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
        canonical_unit=canonical_unit,
        canonical_currency=canonical_currency,
        source=source,
        start=start,
        end=end,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
        cursor_before=cursor_before,
        descending=descending,
    )

    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    key = f"{prefix}eia-series:{_cache_key({**params, 'sql': sql})}"
    start_time = time.perf_counter()
    manager = get_global_cache_manager()
    rows: List[Dict[str, Any]] = cache_get_or_set_sync(
        manager,
        key,
        lambda: _execute_trino_query(trino_cfg, sql),
        cache_cfg.ttl_seconds,
    )
    elapsed = (time.perf_counter() - start_time) * 1000.0
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
    canonical_unit: Optional[str] = None,
    canonical_currency: Optional[str] = None,
    source: Optional[str] = None,
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
        conditions.append(f"unit_raw = '{_safe_literal(unit)}'")
    if canonical_unit:
        conditions.append(f"unit_normalized = '{_safe_literal(canonical_unit)}'")
    if canonical_currency:
        conditions.append(f"currency_normalized = '{_safe_literal(canonical_currency)}'")
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
        "ARRAY_AGG(DISTINCT unit_raw) FILTER (WHERE unit_raw IS NOT NULL) AS unit_values, "
        "ARRAY_AGG(DISTINCT unit_normalized) FILTER (WHERE unit_normalized IS NOT NULL) AS canonical_unit_values, "
        "ARRAY_AGG(DISTINCT currency_normalized) FILTER (WHERE currency_normalized IS NOT NULL) AS canonical_currency_values, "
        "ARRAY_AGG(DISTINCT frequency) FILTER (WHERE frequency IS NOT NULL) AS frequency_values, "
        "ARRAY_AGG(DISTINCT source) FILTER (WHERE source IS NOT NULL) AS source_values "
        f"FROM {_eia_series_base_table()}{where}"
    )

    manager = get_global_cache_manager()
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    params = {
        "series_id": series_id,
        "frequency": frequency,
        "area": area,
        "sector": sector,
        "dataset": dataset,
        "unit": unit,
        "canonical_unit": canonical_unit,
        "canonical_currency": canonical_currency,
        "source": source,
    }
    cache_key = f"{prefix}eia-series-dimensions:{_cache_key({**params, 'sql': sql})}"
    if manager is not None:
        cached_payload = cache_get_sync(manager, cache_key)
        if cached_payload is not None:
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="eia_series_dimensions", event="hit").inc()
                except Exception:
                    pass
            return cached_payload, 0.0

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
    try:
        rows = _execute_trino_query(trino_cfg, sql)
        row = rows[0] if rows else None
        if row:
            mapping = {
                "dataset": row.get("dataset_values"),
                "area": row.get("area_values"),
                "sector": row.get("sector_values"),
                "unit": row.get("unit_values"),
                "canonical_unit": row.get("canonical_unit_values"),
                "canonical_currency": row.get("canonical_currency_values"),
                "frequency": row.get("frequency_values"),
                "source": row.get("source_values"),
            }
            for key, values in mapping.items():
                if not values:
                    continue
                items = [str(item) for item in values if item is not None]
                results[key] = sorted(set(items))
    except Exception:
        # Fall through with empty results; upstream caller handles
        pass
    elapsed = (time.perf_counter() - start_time) * 1000.0

    if manager is not None:
        try:
            cache_set_sync(manager, cache_key, results, cache_cfg.ttl_seconds)
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="eia_series_dimensions", event="miss").inc()
                except Exception:
                    pass
        except Exception:
            LOGGER.debug("CacheManager set failed for EIA series dimensions", exc_info=True)

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

    # try cache via CacheManager
    manager = get_global_cache_manager()
    cache_key = f"curves:{_cache_key({**params, 'sql': sql})}"
    if manager is not None:
        cached_rows = cache_get_sync(manager, cache_key)
        if cached_rows is not None:
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="curves", event="hit").inc()
                except Exception:
                    pass
            return cached_rows, 0.0

    tracer = get_tracer("aurum.api.service")
    with tracer.start_as_current_span("query_curves.execute") as span:
        span.set_attribute("aurum.curves.limit", effective_limit)
        span.set_attribute("aurum.curves.offset", effective_offset)
        span.set_attribute("aurum.curves.has_cursor", bool(cursor_after or cursor_before))
        start = time.perf_counter()
        rows = _execute_trino_query(trino_cfg, sql)
        elapsed = (time.perf_counter() - start) * 1000.0
        span.set_attribute("aurum.curves.elapsed_ms", elapsed)
        span.set_attribute("aurum.curves.row_count", len(rows))

    if manager is not None:
        try:
            cache_set_sync(manager, cache_key, rows, cache_cfg.ttl_seconds)
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="curves", event="miss").inc()
                except Exception:
                    pass
        except Exception:
            LOGGER.debug("CacheManager set failed for curves", exc_info=True)

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
    return build_curve_diff_query(
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

    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    key = f"{prefix}curves-diff:{_cache_key({**params, 'sql': sql})}"
    start = time.perf_counter()
    manager = get_global_cache_manager()
    rows: List[Dict[str, Any]] = cache_get_or_set_sync(
        manager,
        key,
        lambda: _execute_trino_query(trino_cfg, sql),
        cache_cfg.ttl_seconds,
    )
    # Normalize fields
    for row in rows:
        row.pop("tenant_id", None)
        attribution_val = row.get("attribution")
        if isinstance(attribution_val, str):
            try:
                row["attribution"] = json.loads(attribution_val)
            except json.JSONDecodeError:
                row["attribution"] = None
    elapsed = (time.perf_counter() - start) * 1000.0

    return rows, elapsed


def _scenario_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    tenant_key = tenant_id or "anon"
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}scenario-outputs:index:{tenant_key}:{scenario_id}"


def _scenario_metrics_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    tenant_key = tenant_id or "anon"
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}scenario-metrics:index:{tenant_key}:{scenario_id}"


def _scenario_cache_version_key(namespace: str, tenant_id: Optional[str]) -> str:
    tenant_key = tenant_id or "anon"
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}scenario-cache:version:{tenant_key}"


def _get_scenario_cache_version(client, namespace: str, tenant_id: Optional[str]) -> str:
    if client is None:
        return "1"
    key = _scenario_cache_version_key(namespace, tenant_id)
    try:  # pragma: no cover - external IO
        value = client.get(key)
        if value is None:
            client.set(key, "1")
            return "1"
        if isinstance(value, bytes):
            return value.decode("utf-8") or "1"
        return str(value)
    except Exception:
        return "1"


def _bump_scenario_cache_version(client, namespace: str, tenant_id: Optional[str]) -> None:
    if client is None:
        return
    key = _scenario_cache_version_key(namespace, tenant_id)
    try:  # pragma: no cover - external IO
        client.incr(key)
        if SCENARIO_CACHE_INVALIDATIONS:
            try:
                SCENARIO_CACHE_INVALIDATIONS.inc()
            except Exception:
                pass
    except Exception:
        LOGGER.debug("Failed to increment scenario cache version", exc_info=True)


def _publish_cache_invalidation_event(tenant_id: Optional[str], scenario_id: str) -> None:
    topic = os.getenv("AURUM_SCENARIO_CACHE_INVALIDATION_TOPIC", "")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "")
    if not topic or not bootstrap:
        return
    try:  # pragma: no cover - optional dependency
        from confluent_kafka import Producer  # type: ignore
    except Exception:
        LOGGER.debug("Kafka producer unavailable for cache invalidation events", exc_info=True)
        return

    payload = json.dumps(
        {
            "tenant_id": tenant_id,
            "scenario_id": scenario_id,
            "timestamp": time.time(),
        }
    ).encode("utf-8")

    try:  # pragma: no cover - external IO
        producer = Producer({"bootstrap.servers": bootstrap})
        producer.produce(topic=topic, value=payload)
        producer.flush(0.5)
    except Exception:
        LOGGER.debug("Failed to publish cache invalidation event", exc_info=True)


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
    """Build SQL for scenario outputs with keyset pagination.

    Uses `iceberg.market.scenario_output_latest` as the base. When a cursor is
    provided, applies a keyset clause over `SCENARIO_OUTPUT_ORDER_COLUMNS`.
    If `cursor_before` is set, comparison is flipped and `offset` is reset to 0.
    """
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
    """Execute scenario outputs query with optional caching.

    - Builds SQL via `_build_sql_scenario_outputs` with cursor/offset semantics
    - Uses a Redis versioned key (`scenario-outputs:v{N}`) to avoid stale reads
      after writes and a singleflight guard to collapse concurrent cache misses
    - Returns (rows, elapsed_ms)
    """
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
    singleflight_key = None
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    if client is not None:
        version = _get_scenario_cache_version(client, cache_cfg.namespace, tenant_id)
        cache_key = f"{prefix}scenario-outputs:v{version}:{_cache_key({**params, 'sql': sql})}"
        cached = client.get(cache_key)
        if cached:  # pragma: no cover - best effort cache usage
            try:
                data = json.loads(cached)
                if SCENARIO_CACHE_HITS:
                    SCENARIO_CACHE_HITS.inc()
                return data, 0.0
            except Exception:
                LOGGER.debug("Failed to decode scenario outputs cache payload", exc_info=True)
        singleflight_key = f"sf:{cache_key}"
    else:
        singleflight_key = f"sf:scenario-outputs:{tenant_id}:{scenario_id}:{hash(sql)}"

    with _SCENARIO_SINGLEFLIGHT.acquire(singleflight_key):
        if client is not None and cache_key is not None:
            cached = client.get(cache_key)
            if cached:  # pragma: no cover - best effort cache usage
                try:
                    data = json.loads(cached)
                    if SCENARIO_CACHE_HITS:
                        SCENARIO_CACHE_HITS.inc()
                    return data, 0.0
                except Exception:
                    LOGGER.debug("Failed to decode scenario outputs cache payload", exc_info=True)

        if SCENARIO_CACHE_MISSES:
            try:
                SCENARIO_CACHE_MISSES.inc()
            except Exception:
                pass

    tracer = get_tracer("aurum.api.service")
    with tracer.start_as_current_span("query_curves_diff.execute") as span:
        span.set_attribute("aurum.curves_diff.limit", limit)
        span.set_attribute("aurum.curves_diff.has_cursor", bool(cursor_after))
        start = time.perf_counter()
        rows = _execute_trino_query(trino_cfg, sql)
        elapsed = (time.perf_counter() - start) * 1000.0
        span.set_attribute("aurum.curves_diff.elapsed_ms", elapsed)
        span.set_attribute("aurum.curves_diff.row_count", len(rows))

        if client is not None and cache_key is not None:
            try:  # pragma: no cover
                client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
                index_key = _scenario_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
                client.sadd(index_key, cache_key)
                client.expire(index_key, cache_cfg.ttl_seconds)
            except Exception:
                LOGGER.debug("Failed to populate scenario cache", exc_info=True)

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

    start = time.perf_counter()
    rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
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


def query_ppa_contract_valuations(
    trino_cfg: TrinoConfig,
    *,
    ppa_contract_id: str,
    scenario_id: Optional[str] = None,
    metric: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
    tenant_id: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    filters: Dict[str, Optional[str]] = {
        "ppa_contract_id": ppa_contract_id,
        "tenant_id": tenant_id,
    }
    if scenario_id:
        filters["scenario_id"] = scenario_id
    if metric:
        filters["metric"] = metric
    where = _build_where(filters)

    sql = (
        "SELECT cast(asof_date as date) as asof_date, "
        "cast(period_start as date) as period_start, "
        "cast(period_end as date) as period_end, "
        "scenario_id, tenant_id, curve_key, metric, value, cashflow, npv, irr, version_hash, _ingest_ts "
        "FROM iceberg.market.ppa_valuation"
        f"{where} "
        "ORDER BY asof_date DESC, scenario_id, metric, period_start DESC "
        f"LIMIT {int(limit)} OFFSET {int(offset)}"
    )

    start = time.perf_counter()
    rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
    # Normalize numeric fields
    for record in rows:
        for key in ("value", "cashflow", "npv"):
            if record.get(key) is not None:
                record[key] = float(record[key])
        if record.get("irr") is not None:
            record["irr"] = float(record["irr"])
    elapsed = (time.perf_counter() - start) * 1000.0
    return rows, elapsed


def _build_sql_scenario_metrics_latest(
    *,
    tenant_id: Optional[str],
    scenario_id: str,
    metric: Optional[str],
    limit: int,
    offset: int,
    cursor_after: Optional[Dict[str, Any]],
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> str:
    base = "iceberg.market.scenario_output_latest_by_metric"
    filters: Dict[str, Optional[str]] = {"scenario_id": scenario_id, "metric": metric}
    if tenant_id:
        filters["tenant_id"] = tenant_id
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
        order_columns=SCENARIO_METRIC_ORDER_COLUMNS,
        comparison=comparison,
    )
    direction = "DESC" if descending else "ASC"
    order_clause = " ORDER BY " + ", ".join(
        f"{col} {direction}" for col in SCENARIO_METRIC_ORDER_COLUMNS
    )
    effective_offset = 0 if comparison_cursor else offset
    return (
        "SELECT tenant_id, scenario_id, curve_key, metric, tenor_label, latest_value, latest_band_lower, latest_band_upper, "
        "cast(latest_asof_date as date) as latest_asof_date "
        f"FROM {base}{where}{order_clause} LIMIT {limit} OFFSET {effective_offset}"
    )


def query_scenario_metrics_latest(
    trino_cfg: TrinoConfig,
    cache_cfg: CacheConfig,
    *,
    scenario_id: str,
    tenant_id: Optional[str] = None,
    metric: Optional[str] = None,
    limit: int,
    offset: int = 0,
    cursor_after: Optional[Dict[str, Any]] = None,
    cursor_before: Optional[Dict[str, Any]] = None,
    descending: bool = False,
) -> Tuple[List[Dict[str, Any]], float]:
    params = {
        "scenario_id": scenario_id,
        "tenant_id": tenant_id,
        "metric": metric,
        "limit": limit,
        "offset": offset,
        "cursor_after": cursor_after,
        "cursor_before": cursor_before,
        "descending": descending,
    }

    sql = _build_sql_scenario_metrics_latest(
        tenant_id=tenant_id,
        scenario_id=scenario_id,
        metric=metric,
        limit=limit,
        offset=offset,
        cursor_after=cursor_after,
        cursor_before=cursor_before,
        descending=descending,
    )
    params["sql"] = sql

    client = _maybe_redis_client(cache_cfg)
    cache_key = None
    singleflight_key = None
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    if client is not None:
        version = _get_scenario_cache_version(client, cache_cfg.namespace, tenant_id)
        cache_key = f"{prefix}scenario-metrics:v{version}:{_cache_key(params)}"
        cached = client.get(cache_key)
        if cached:
            try:
                data = json.loads(cached)
                if SCENARIO_CACHE_HITS:
                    SCENARIO_CACHE_HITS.inc()
                return data, 0.0
            except Exception:
                LOGGER.debug("Failed to decode scenario metrics cache payload", exc_info=True)
        singleflight_key = f"sf:{cache_key}"
    else:
        singleflight_key = f"sf:scenario-metrics:{tenant_id}:{scenario_id}:{hash(sql)}"

    with _SCENARIO_SINGLEFLIGHT.acquire(singleflight_key):
        if client is not None and cache_key is not None:
            cached = client.get(cache_key)
            if cached:
                try:
                    data = json.loads(cached)
                    if SCENARIO_CACHE_HITS:
                        SCENARIO_CACHE_HITS.inc()
                    return data, 0.0
                except Exception:
                    LOGGER.debug("Failed to decode scenario metrics cache payload", exc_info=True)

        if SCENARIO_CACHE_MISSES:
            try:
                SCENARIO_CACHE_MISSES.inc()
            except Exception:
                pass

        start = time.perf_counter()
        rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
        elapsed = (time.perf_counter() - start) * 1000.0

        if client is not None and cache_key is not None:
            try:  # pragma: no cover
                client.setex(cache_key, cache_cfg.ttl_seconds, json.dumps(rows, default=str))
                index_key = _scenario_metrics_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
                client.sadd(index_key, cache_key)
                client.expire(index_key, cache_cfg.ttl_seconds)
            except Exception:
                LOGGER.debug("Failed to populate scenario metrics cache", exc_info=True)

        return rows, elapsed


def invalidate_scenario_outputs_cache(
    cache_cfg: CacheConfig,
    tenant_id: Optional[str],
    scenario_id: str,
) -> None:
    client = _maybe_redis_client(cache_cfg)
    _publish_cache_invalidation_event(tenant_id, scenario_id)
    if client is None:
        return
    _bump_scenario_cache_version(client, cache_cfg.namespace, tenant_id)
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


def _decode_member(member: Any) -> str:
    if isinstance(member, bytes):
        return member.decode("utf-8")
    return str(member)


def invalidate_eia_series_cache(cache_cfg: CacheConfig) -> Dict[str, int]:
    client = _maybe_redis_client(cache_cfg)
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    scopes = {
        "eia-series": f"{prefix}eia-series:index",
        "eia-series-dimensions": f"{prefix}eia-series-dimensions:index",
    }
    results: Dict[str, int] = {key: 0 for key in scopes}
    if client is None:
        return results

    for scope, index_key in scopes.items():
        try:
            members = client.smembers(index_key)
        except Exception:
            continue
        if not members:
            client.delete(index_key)
            continue
        keys: list[str] = []
        for member in members:
            decoded = _decode_member(member)
            if decoded:
                keys.append(decoded)
        if keys:
            try:
                client.delete(*keys)
            except Exception:
                pass
            results[scope] = len(keys)
        try:
            client.srem(index_key, *members)
        except Exception:
            pass
        try:
            if hasattr(client, "scard") and client.scard(index_key) == 0:
                client.delete(index_key)
        except Exception:
            client.delete(index_key)
    return results


def invalidate_dimensions_cache(cache_cfg: CacheConfig) -> int:
    client = _maybe_redis_client(cache_cfg)
    if client is None:
        return 0
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    index_key = f"{prefix}dimensions:index"
    try:
        members = client.smembers(index_key)
    except Exception:
        return 0
    if not members:
        client.delete(index_key)
        return 0
    keys: list[str] = []
    for member in members:
        decoded = _decode_member(member)
        if decoded:
            keys.append(decoded)
    removed = 0
    if keys:
        try:
            client.delete(*keys)
        except Exception:
            pass
        removed = len(keys)
    try:
        client.srem(index_key, *members)
    except Exception:
        pass
    try:
        if hasattr(client, "scard") and client.scard(index_key) == 0:
            client.delete(index_key)
    except Exception:
        client.delete(index_key)
    return removed


def invalidate_metadata_cache(cache_cfg: CacheConfig, prefixes: Sequence[str]) -> Dict[str, int]:
    client = _maybe_redis_client(cache_cfg)
    namespace = cache_cfg.namespace or "aurum"
    index_key = f"{namespace}:metadata:index"
    results: Dict[str, int] = {prefix: 0 for prefix in prefixes}
    if client is None:
        return results
    try:
        members = client.smembers(index_key)
    except Exception:
        return results

    if not members:
        client.delete(index_key)
        return results

    members_list = list(members)
    keys_to_delete: list[str] = []
    matched_members: list[Any] = []
    for raw_member in members_list:
        redis_key = _decode_member(raw_member)
        if not redis_key:
            continue
        for prefix in prefixes:
            expected_prefix = f"{namespace}:metadata:{prefix}"
            if redis_key.startswith(expected_prefix):
                keys_to_delete.append(redis_key)
                matched_members.append(raw_member)
                results[prefix] += 1
                break

    if keys_to_delete:
        try:
            client.delete(*keys_to_delete)
        except Exception:
            pass

    if matched_members:
        try:
            client.srem(index_key, *matched_members)
        except Exception:
            pass

    try:
        if hasattr(client, "scard") and client.scard(index_key) == 0:
            client.delete(index_key)
    except Exception:
        client.delete(index_key)

    return results


if _PromCounter is not None:  # pragma: no cover - optional metrics dependency
    ISO_LMP_CACHE_COUNTER = _PromCounter(
        "aurum_iso_lmp_cache_events_total",
        "ISO LMP cache events grouped by backend and outcome.",
        ["operation", "backend", "event"],
    )
else:  # pragma: no cover - metrics optional
    ISO_LMP_CACHE_COUNTER = None  # type: ignore[assignment]


class _IsoLmpInMemoryCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._store: Dict[str, tuple[float, Any]] = {}

    def get(self, key: str):
        now = time.time()
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at <= now:
                self._store.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl: int) -> None:
        if ttl <= 0:
            return
        expires_at = time.time() + ttl
        with self._lock:
            self._store[key] = (expires_at, value)

    def clear(self) -> None:
        with self._lock:
            self._store.clear()


_ISO_LMP_MEMORY_CACHE = _IsoLmpInMemoryCache()
_ISO_LMP_CACHE_TTL_OVERRIDE: int | None = None
_ISO_LMP_INMEM_TTL_OVERRIDE: int | None = None


def _iso_lmp_effective_ttl(cache_cfg: CacheConfig) -> int:
    if _ISO_LMP_CACHE_TTL_OVERRIDE is not None:
        return max(0, _ISO_LMP_CACHE_TTL_OVERRIDE)
    ttl = _settings().api.cache.iso_lmp_cache_ttl
    if ttl is not None:
        return max(0, ttl)
    return max(0, cache_cfg.ttl_seconds)


def _iso_lmp_memory_ttl() -> int:
    if _ISO_LMP_INMEM_TTL_OVERRIDE is not None:
        return max(0, _ISO_LMP_INMEM_TTL_OVERRIDE)
    ttl = _settings().api.cache.iso_lmp_inmemory_ttl
    return max(0, ttl)


def _iso_lmp_cache_key(operation: str, params: Dict[str, Any], sql: str, namespace: str) -> str:
    payload = dict(params)
    payload["sql"] = sql
    prefix = f"{namespace}:" if namespace else ""
    return f"{prefix}iso-lmp:{operation}:{_cache_key(payload)}"


def _record_iso_lmp_cache_event(operation: str, backend: str, event: str) -> None:
    if ISO_LMP_CACHE_COUNTER is None:  # pragma: no cover - metrics optional
        return
    ISO_LMP_CACHE_COUNTER.labels(operation=operation, backend=backend, event=event).inc()


def _cached_iso_lmp_query(
    operation: str,
    sql: str,
    params: Dict[str, Any],
    *,
    cache_cfg: CacheConfig | None = None,
) -> Tuple[List[Dict[str, Any]], float]:
    cfg = cache_cfg or CacheConfig.from_settings(_settings())
    ttl = _iso_lmp_effective_ttl(cfg)
    memory_ttl = _iso_lmp_memory_ttl()
    namespace = cfg.namespace or ""
    cache_key = _iso_lmp_cache_key(operation, params, sql, namespace)

    if memory_ttl > 0:
        cached_rows = _ISO_LMP_MEMORY_CACHE.get(cache_key)
        if cached_rows is not None:
            _record_iso_lmp_cache_event(operation, "memory", "hit")
            return cached_rows, 0.0
        _record_iso_lmp_cache_event(operation, "memory", "miss")

    manager = None
    if ttl > 0:
        manager = get_global_cache_manager()
        if manager is not None:
            try:
                cached_payload = cache_get_sync(manager, cache_key)
            except Exception:
                LOGGER.debug("ISO LMP CacheManager lookup failed", exc_info=True)
                cached_payload = None
            if cached_payload is not None:
                _record_iso_lmp_cache_event(operation, "redis", "hit")
                if memory_ttl > 0:
                    _ISO_LMP_MEMORY_CACHE.set(cache_key, cached_payload, memory_ttl)
                    _record_iso_lmp_cache_event(operation, "memory", "store")
                return cached_payload, 0.0
            _record_iso_lmp_cache_event(operation, "redis", "miss")

    rows, elapsed = _fetch_timescale_rows(sql, params)
    _record_iso_lmp_cache_event(operation, "database", "query")

    if ttl > 0 and manager is not None:
        try:
            cache_set_sync(manager, cache_key, rows, ttl)
            _record_iso_lmp_cache_event(operation, "redis", "store")
        except Exception:
            LOGGER.debug("ISO LMP CacheManager population failed", exc_info=True)

    if memory_ttl > 0:
        _ISO_LMP_MEMORY_CACHE.set(cache_key, rows, memory_ttl)
        _record_iso_lmp_cache_event(operation, "memory", "store")

    return rows, elapsed


def query_iso_lmp_last_24h(
    *,
    iso_code: str | None = None,
    market: str | None = None,
    location_id: str | None = None,
    limit: int = 500,
    cache_cfg: CacheConfig | None = None,
) -> Tuple[List[Dict[str, Any]], float]:
    sql = (
        "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
        "location_id, location_name, location_type, price_total, price_energy, price_congestion, "
        "price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
        "FROM public.iso_lmp_last_24h WHERE 1 = 1"
    )
    params: Dict[str, Any] = {"limit": min(limit, 2000)}
    if iso_code:
        sql += " AND iso_code = %(iso_code)s"
        params["iso_code"] = iso_code.upper()
    if market:
        sql += " AND market = %(market)s"
        params["market"] = market.upper()
    if location_id:
        sql += " AND upper(location_id) = upper(%(location_id)s)"
        params["location_id"] = location_id
    sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
    return _cached_iso_lmp_query("last-24h", sql, params, cache_cfg=cache_cfg)


def query_iso_lmp_hourly(
    *,
    iso_code: str | None = None,
    market: str | None = None,
    location_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 500,
    cache_cfg: CacheConfig | None = None,
) -> Tuple[List[Dict[str, Any]], float]:
    sql = (
        "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
        "price_max, price_stddev, sample_count FROM public.iso_lmp_hourly WHERE 1 = 1"
    )
    params: Dict[str, Any] = {"limit": min(limit, 2000)}
    if iso_code:
        sql += " AND iso_code = %(iso_code)s"
        params["iso_code"] = iso_code.upper()
    if market:
        sql += " AND market = %(market)s"
        params["market"] = market.upper()
    if location_id:
        sql += " AND upper(location_id) = upper(%(location_id)s)"
        params["location_id"] = location_id
    if start:
        sql += " AND interval_start >= %(start)s"
        params["start"] = start
    if end:
        sql += " AND interval_start <= %(end)s"
        params["end"] = end
    sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
    return _cached_iso_lmp_query("hourly", sql, params, cache_cfg=cache_cfg)


def query_iso_lmp_daily(
    *,
    iso_code: str | None = None,
    market: str | None = None,
    location_id: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    limit: int = 500,
    cache_cfg: CacheConfig | None = None,
) -> Tuple[List[Dict[str, Any]], float]:
    sql = (
        "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
        "price_max, price_stddev, sample_count FROM public.iso_lmp_daily WHERE 1 = 1"
    )
    params: Dict[str, Any] = {"limit": min(limit, 2000)}
    if iso_code:
        sql += " AND iso_code = %(iso_code)s"
        params["iso_code"] = iso_code.upper()
    if market:
        sql += " AND market = %(market)s"
        params["market"] = market.upper()
    if location_id:
        sql += " AND upper(location_id) = upper(%(location_id)s)"
        params["location_id"] = location_id
    if start:
        sql += " AND interval_start >= %(start)s"
        params["start"] = start
    if end:
        sql += " AND interval_start <= %(end)s"
        params["end"] = end
    sql += " ORDER BY interval_start DESC LIMIT %(limit)s"
    return _cached_iso_lmp_query("daily", sql, params, cache_cfg=cache_cfg)


def query_iso_lmp_negative(
    *,
    iso_code: str | None = None,
    market: str | None = None,
    limit: int = 200,
    cache_cfg: CacheConfig | None = None,
) -> Tuple[List[Dict[str, Any]], float]:
    sql = (
        "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
        "location_id, location_name, location_type, price_total, price_energy, price_congestion, "
        "price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
        "FROM public.iso_lmp_negative_7d WHERE 1 = 1"
    )
    params: Dict[str, Any] = {"limit": min(limit, 1000)}
    if iso_code:
        sql += " AND iso_code = %(iso_code)s"
        params["iso_code"] = iso_code.upper()
    if market:
        sql += " AND market = %(market)s"
        params["market"] = market.upper()
    sql += " ORDER BY price_total ASC LIMIT %(limit)s"
    return _cached_iso_lmp_query("negative", sql, params, cache_cfg=cache_cfg)


__all__ = [
    "query_curves",
    "query_curves_diff",
    "query_scenario_outputs",
    "query_eia_series",
    "query_eia_series_dimensions",
    "query_ppa_valuation",
    "query_ppa_contract_valuations",
    "query_scenario_metrics_latest",
    "query_iso_lmp_last_24h",
    "query_iso_lmp_hourly",
    "query_iso_lmp_daily",
    "query_iso_lmp_negative",
    "invalidate_scenario_outputs_cache",
    "invalidate_eia_series_cache",
    "invalidate_dimensions_cache",
    "invalidate_metadata_cache",
    "TrinoConfig",
    "CacheConfig",
]



def query_drought_indices(
    trino_cfg: TrinoConfig,
    *,
    region_type: Optional[str] = None,
    region_id: Optional[str] = None,
    dataset: Optional[str] = None,
    index_id: Optional[str] = None,
    timescale: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 500,
) -> Tuple[List[Dict[str, Any]], float]:
    catalog = trino_cfg.catalog
    base_conditions: List[str] = []
    if region_type:
        base_conditions.append(f"region_type = '{_safe_literal(region_type)}'")
    if region_id:
        base_conditions.append(f"region_id = '{_safe_literal(region_id)}'")
    if dataset:
        base_conditions.append(f"dataset = '{_safe_literal(dataset)}'")
    if index_id:
        base_conditions.append(f"\"index\" = '{_safe_literal(index_id)}'")
    if timescale:
        base_conditions.append(f"timescale = '{_safe_literal(timescale)}'")
    if start_date:
        base_conditions.append(f"valid_date >= DATE '{_safe_literal(start_date.isoformat())}'")
    if end_date:
        base_conditions.append(f"valid_date <= DATE '{_safe_literal(end_date.isoformat())}'")
    where_clause = ""
    if base_conditions:
        where_clause = " AND " + " AND ".join(base_conditions)
    sql = f"""
        WITH ranked AS (
            SELECT
                di.*, ROW_NUMBER() OVER (
                    PARTITION BY di.series_id, di.valid_date
                    ORDER BY di.ingest_ts DESC
                ) AS rn
            FROM {catalog}.environment.drought_index AS di
            WHERE 1 = 1{where_clause}
        )
        SELECT
            series_id,
            dataset,
            "index" AS index,
            timescale,
            CAST(valid_date AS DATE) AS valid_date,
            as_of,
            value,
            unit,
            poc,
            region_type,
            region_id,
            g.region_name,
            g.parent_region_id,
            source_url,
            CAST(metadata AS JSON) AS metadata
        FROM ranked r
        LEFT JOIN {catalog}.ref.geographies g
            ON g.region_type = r.region_type
           AND g.region_id = r.region_id
        WHERE rn = 1
        ORDER BY valid_date DESC, series_id
        LIMIT {int(limit)}
    """
    start_time = time.perf_counter()
    rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                row["metadata"] = json.loads(metadata)
            except json.JSONDecodeError:
                row["metadata"] = None
    elapsed = (time.perf_counter() - start_time) * 1000.0
    return rows, elapsed


def query_drought_usdm(
    trino_cfg: TrinoConfig,
    *,
    region_type: Optional[str] = None,
    region_id: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = 500,
) -> Tuple[List[Dict[str, Any]], float]:
    catalog = trino_cfg.catalog
    conditions: List[str] = []
    if region_type:
        conditions.append(f"region_type = '{_safe_literal(region_type)}'")
    if region_id:
        conditions.append(f"region_id = '{_safe_literal(region_id)}'")
    if start_date:
        conditions.append(f"valid_date >= DATE '{_safe_literal(start_date.isoformat())}'")
    if end_date:
        conditions.append(f"valid_date <= DATE '{_safe_literal(end_date.isoformat())}'")
    where_sql = ""
    if conditions:
        where_sql = " AND " + " AND ".join(conditions)
    sql = f"""
        WITH ranked AS (
            SELECT
                ua.*, ROW_NUMBER() OVER (
                    PARTITION BY ua.region_type, ua.region_id, ua.valid_date
                    ORDER BY ua.ingest_ts DESC
                ) AS rn
            FROM {catalog}.environment.usdm_area ua
            WHERE 1 = 1{where_sql}
        )
        SELECT
            region_type,
            region_id,
            CAST(valid_date AS DATE) AS valid_date,
            as_of,
            d0_frac,
            d1_frac,
            d2_frac,
            d3_frac,
            d4_frac,
            source_url,
            CAST(metadata AS JSON) AS metadata,
            g.region_name,
            g.parent_region_id
        FROM ranked r
        LEFT JOIN {catalog}.ref.geographies g
            ON g.region_type = r.region_type
           AND g.region_id = r.region_id
        WHERE rn = 1
        ORDER BY valid_date DESC
        LIMIT {int(limit)}
    """
    start_time = time.perf_counter()
    rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
    for row in rows:
        metadata = row.get("metadata")
        if isinstance(metadata, str):
            try:
                row["metadata"] = json.loads(metadata)
            except json.JSONDecodeError:
                row["metadata"] = None
    elapsed = (time.perf_counter() - start_time) * 1000.0
    return rows, elapsed


def query_drought_vector_events(
    trino_cfg: TrinoConfig,
    *,
    layer: Optional[str] = None,
    region_type: Optional[str] = None,
    region_id: Optional[str] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 500,
) -> Tuple[List[Dict[str, Any]], float]:
    catalog = trino_cfg.catalog
    conditions: List[str] = []
    if layer:
        conditions.append(f"layer = '{_safe_literal(layer)}'")
    if region_type:
        conditions.append(f"region_type = '{_safe_literal(region_type)}'")
    if region_id:
        conditions.append(f"region_id = '{_safe_literal(region_id)}'")
    if start_time:
        conditions.append(f"valid_start >= TIMESTAMP '{_safe_literal(start_time.isoformat(sep=' ', timespec='seconds'))}'")
    if end_time:
        conditions.append(f"coalesce(valid_end, valid_start) <= TIMESTAMP '{_safe_literal(end_time.isoformat(sep=' ', timespec='seconds'))}'")
    where_sql = ""
    if conditions:
        where_sql = " WHERE " + " AND ".join(conditions)
    sql = f"""
        SELECT
            ve.layer,
            ve.event_id,
            ve.region_type,
            ve.region_id,
            g.region_name,
            g.parent_region_id,
            ve.valid_start,
            ve.valid_end,
            ve.value,
            ve.unit,
            ve.category,
            ve.severity,
            ve.source_url,
            ve.geometry_wkt,
            CAST(ve.properties AS JSON) AS properties
        FROM {catalog}.environment.vector_events ve
        LEFT JOIN {catalog}.ref.geographies g
            ON g.region_type = ve.region_type
           AND g.region_id = ve.region_id
        {where_sql}
        ORDER BY coalesce(ve.valid_start, ve.ingest_ts) DESC
        LIMIT {int(limit)}
    """
    start = time.perf_counter()
    rows: List[Dict[str, Any]] = _execute_trino_query(trino_cfg, sql)
    for row in rows:
        props = row.get("properties")
        if isinstance(props, str):
            try:
                row["properties"] = json.loads(props)
            except json.JSONDecodeError:
                row["properties"] = None
    elapsed = (time.perf_counter() - start) * 1000.0
    return rows, elapsed
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

    manager = get_global_cache_manager()
    prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
    params = {
        "asof": asof.isoformat() if asof else None,
        **filters,
        "limit": per_dim_limit,
        "include_counts": include_counts,
    }
    cache_key = f"{prefix}dimensions:{_cache_key(params)}"
    if manager is not None:
        cached_payload = cache_get_sync(manager, cache_key)
        if cached_payload is not None:
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="dimensions", event="hit").inc()
                except Exception:
                    pass
            return cached_payload.get("values", {}), cached_payload.get("counts")

    results: Dict[str, List[str]] = {}
    counts: Dict[str, List[Dict[str, Any]]] | None = {} if include_counts else None

    if include_counts:
        # Single UNION ALL query with counts
        union_queries = []
        for dim in dims:
            union_queries.append(f"""
                SELECT
                    '{dim}' AS dimension,
                    {dim} AS value,
                    COUNT(*) AS count
                FROM {base}{where + (" AND " if where else " WHERE ") + f"{dim} IS NOT NULL"}
                GROUP BY {dim}
                ORDER BY count DESC
                LIMIT {per_dim_limit}
            """)

        union_sql = " UNION ALL ".join(union_queries)
        sql = f"SELECT dimension, value, count FROM ({union_sql}) ORDER BY dimension, count DESC"
        rows = _execute_trino_query(trino_cfg, sql)

        for dim in dims:
            dim_values: List[str] = []
            dim_counts: List[Dict[str, Any]] = []
            for row in rows:
                if row.get("dimension") == dim:
                    val = row.get("value")
                    dim_values.append(val)
                    dim_counts.append({"value": val, "count": int(row.get("count", 0))})
            results[dim] = dim_values
            if counts is not None:
                counts[dim] = dim_counts
    else:
        # Per-dimension distinct values
        for dim in dims:
            clause = where + (" AND " if where else " WHERE ") + f"{dim} IS NOT NULL"
            sql = f"SELECT DISTINCT {dim} AS value FROM {base}{clause} LIMIT {per_dim_limit}"
            rows = _execute_trino_query(trino_cfg, sql)
            values = [row.get("value") for row in rows if row.get("value") is not None]
            results[dim] = values

    if manager is not None:
        try:
            cache_set_sync(manager, cache_key, {"values": results, "counts": counts}, cache_cfg.ttl_seconds)
            if SERVICE_CACHE_COUNTER:
                try:
                    SERVICE_CACHE_COUNTER.labels(endpoint="dimensions", event="miss").inc()
                except Exception:
                    pass
        except Exception:
            LOGGER.debug("CacheManager set failed for dimensions", exc_info=True)
    return results, counts if include_counts else None


# Async service function stubs for new modules
async def fetch_curve_data(
    asof: Optional[str] = None,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    pagination: Optional[Any] = None,
    cursor: Optional[Any] = None,
    prev_cursor: Optional[str] = None,
) -> Tuple[List[Any], Any]:
    """Fetch curve data using AsyncCurveService."""
    from .async_service import AsyncCurveService
    from .container import get_service

    # Determine effective pagination
    if cursor is not None:
        eff_limit = int(getattr(cursor, "limit", 100))
        eff_offset = int(getattr(cursor, "offset", 0))
    else:
        eff_limit = int(getattr(pagination, "limit", 100)) if pagination is not None else 100
        eff_offset = int(getattr(pagination, "offset", 0)) if pagination is not None else 0

    # Over-fetch by 1 to detect if more pages exist
    fetch_limit = max(1, eff_limit + 1)

    service = get_service(AsyncCurveService)
    points, _raw_meta = await service.fetch_curve_data(
        asof=asof,
        iso=iso,
        market=market,
        location=location,
        product=product,
        block=block,
        limit=fetch_limit,
        offset=eff_offset,
    )

    # Transform CurvePoint objects to dicts for compatibility
    # Trim to page size and compute cursors
    has_more = len(points) > eff_limit
    if has_more:
        points = points[:eff_limit]

    filters = {
        "asof": asof,
        "iso": iso,
        "market": market,
        "location": location,
        "product": product,
        "block": block,
    }
    next_token = None
    prev_token = None
    try:
        if has_more:
            next_token = _Cursor(offset=eff_offset + eff_limit, limit=eff_limit, timestamp=time.time(), filters=filters).to_string()
        if eff_offset > 0:
            prev_offset = max(0, eff_offset - eff_limit)
            prev_token = _Cursor(offset=prev_offset, limit=eff_limit, timestamp=time.time(), filters=filters).to_string()
    except Exception:
        # If cursor encoding fails, fall back to None tokens
        next_token = None
        prev_token = None

    data = [
        {
            "curve_key": point.curve_key,
            "tenor_label": point.tenor_label,
            "asof_date": point.asof_date,
            "mid": point.mid,
            "bid": point.bid,
            "ask": point.ask,
            "currency": point.currency,
            "per_unit": point.per_unit,
            "iso": point.iso,
            "market": point.market,
            "location": point.location,
            "product": point.product,
            "block": point.block,
        }
        for point in points
    ]

    class _CursorMeta:
        def __init__(self, next_cursor: Optional[str], prev_cursor: Optional[str]) -> None:
            self.next_cursor = next_cursor
            self.prev_cursor = prev_cursor

    return data, _CursorMeta(next_token, prev_token)


async def fetch_curve_diff_data(
    asof_a: str,
    asof_b: str,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    pagination: Optional[Any] = None,
) -> Tuple[List[Any], Any]:
    """Fetch curve diff data using AsyncCurveService."""
    from .async_service import AsyncCurveService
    from .container import get_service

    service = get_service(AsyncCurveService)
    # Derive limit/offset from pagination
    eff_limit = int(getattr(pagination, "limit", 100)) if pagination is not None else 100
    eff_offset = int(getattr(pagination, "offset", 0)) if pagination is not None else 0

    points, _meta = await service.fetch_curve_diff(
        asof_a=asof_a,
        asof_b=asof_b,
        iso=iso,
        market=market,
        location=location,
        product=product,
        block=block,
        limit=eff_limit,
        offset=eff_offset,
    )

    # Transform CurveDiffPoint objects to dicts for compatibility
    data = [
        {
            "curve_key": point.curve_key,
            "tenor_label": point.tenor_label,
            "asof_date_a": point.asof_date_a,
            "asof_date_b": point.asof_date_b,
            "mid_a": point.mid_a,
            "mid_b": point.mid_b,
            "mid_diff": point.mid_diff,
            "bid_a": point.bid_a,
            "bid_b": point.bid_b,
            "bid_diff": point.bid_diff,
            "ask_a": point.ask_a,
            "ask_b": point.ask_b,
            "ask_diff": point.ask_diff,
            "currency": point.currency,
            "per_unit": point.per_unit,
            "iso": point.iso,
            "market": point.market,
            "location": point.location,
            "product": point.product,
            "block": point.block,
        }
        for point in points
    ]

    return data, None


async def fetch_curve_strips_data(
    strip_type: str,
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
    product: Optional[str] = None,
    block: Optional[str] = None,
    pagination: Optional[Any] = None,
) -> Tuple[List[Any], Any]:
    """Fetch curve strips data using AsyncCurveService."""
    from .async_service import AsyncCurveService
    from .container import get_service

    service = get_service(AsyncCurveService)
    eff_limit = int(getattr(pagination, "limit", 100)) if pagination is not None else 100
    eff_offset = int(getattr(pagination, "offset", 0)) if pagination is not None else 0

    points, _meta = await service.fetch_curve_strips(
        strip_type=strip_type,
        iso=iso,
        market=market,
        location=location,
        product=product,
        block=block,
        limit=eff_limit,
        offset=eff_offset,
    )

    # Transform CurvePoint objects to dicts for compatibility
    data = [
        {
            "curve_key": point.curve_key,
            "tenor_label": point.tenor_label,
            "asof_date": point.asof_date,
            "mid": point.mid,
            "bid": point.bid,
            "ask": point.ask,
            "currency": point.currency,
            "per_unit": point.per_unit,
            "iso": point.iso,
            "market": point.market,
            "location": point.location,
            "product": point.product,
            "block": point.block,
        }
        for point in points
    ]

    return data, None


async def fetch_metadata_dimensions(
    asof: Optional[str] = None,
    include_counts: bool = False,
) -> Tuple[Dict, Optional[Dict]]:
    """Fetch metadata dimensions using AsyncMetadataService."""
    from .async_service import AsyncMetadataService
    from .container import get_service

    service = get_service(AsyncMetadataService)
    dimensions, counts = await service.get_dimensions(asof=asof)

    if include_counts:
        return dimensions, counts
    else:
        return dimensions, None


async def fetch_iso_locations(
    iso: Optional[str] = None,
    location_ids: Optional[List[str]] = None,
    prefix: Optional[str] = None,
) -> List[Dict]:
    """Fetch ISO locations from dbt dimensions."""
    from .database.trino_client import get_trino_client
    from .config import TrinoConfig

    # Get Trino client
    trino_client = get_trino_client()

    # Build query
    query = """
        SELECT
            iso_code,
            market_code,
            location_code,
            location_name,
            timezone,
            region,
            created_at
        FROM iceberg.market.dim_location
        WHERE 1=1
    """

    params = {}
    if iso:
        query += " AND iso_code = %(iso)s"
        params['iso'] = iso

    if location_ids:
        placeholders = ','.join(['%s'] * len(location_ids))
        query += f" AND location_code IN ({placeholders})"
        params['location_ids'] = location_ids

    if prefix:
        query += " AND location_code LIKE %(prefix)s"
        params['prefix'] = f"{prefix}%"

    query += " ORDER BY iso_code, market_code, location_code"

    rows = await trino_client.execute_query(query, params, use_cache=True)

    return [
        {
            "iso": row.get("iso_code"),
            "market": row.get("market_code"),
            "location_code": row.get("location_code"),
            "location_name": row.get("location_name"),
            "timezone": row.get("timezone"),
            "region": row.get("region"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def fetch_units_canonical() -> List[Dict]:
    """Fetch canonical units from dbt dimensions."""
    from .database.trino_client import get_trino_client

    trino_client = get_trino_client()

    query = """
        SELECT
            unit_code,
            unit_name,
            unit_category,
            conversion_factor,
            base_unit,
            created_at
        FROM iceberg.market.dim_unit
        ORDER BY unit_category, unit_code
    """

    rows = await trino_client.execute_query(query, use_cache=True)

    return [
        {
            "unit_code": row.get("unit_code"),
            "unit_name": row.get("unit_name"),
            "unit_category": row.get("unit_category"),
            "conversion_factor": row.get("conversion_factor"),
            "base_unit": row.get("base_unit"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def fetch_units_mapping(prefix: Optional[str] = None) -> List[Dict]:
    """Fetch units mapping from dbt dimensions."""
    from .database.trino_client import get_trino_client

    trino_client = get_trino_client()

    query = """
        SELECT
            raw_unit,
            canonical_unit,
            conversion_factor,
            confidence,
            created_at
        FROM iceberg.market.unit_mapping
    """

    params = {}
    if prefix:
        query += " WHERE raw_unit LIKE %(prefix)s"
        params['prefix'] = f"{prefix}%"

    query += " ORDER BY raw_unit"

    rows = await trino_client.execute_query(query, params, use_cache=True)

    return [
        {
            "raw_unit": row.get("raw_unit"),
            "canonical_unit": row.get("canonical_unit"),
            "conversion_factor": row.get("conversion_factor"),
            "confidence": row.get("confidence"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def fetch_calendars() -> List[Dict]:
    """Fetch calendars from dbt dimensions."""
    from .database.trino_client import get_trino_client

    trino_client = get_trino_client()

    query = """
        SELECT
            calendar_name,
            calendar_type,
            description,
            timezone,
            business_days,
            holidays,
            created_at
        FROM iceberg.market.dim_calendar
        ORDER BY calendar_name
    """

    rows = await trino_client.execute_query(query, use_cache=True)

    return [
        {
            "calendar_name": row.get("calendar_name"),
            "calendar_type": row.get("calendar_type"),
            "description": row.get("description"),
            "timezone": row.get("timezone"),
            "business_days": row.get("business_days"),
            "holidays": row.get("holidays"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def fetch_calendar_blocks(name: str) -> List[Dict]:
    """Fetch calendar blocks from dbt dimensions."""
    from .database.trino_client import get_trino_client

    trino_client = get_trino_client()

    query = """
        SELECT
            calendar_name,
            block_name,
            block_type,
            start_hour,
            end_hour,
            days_of_week,
            created_at
        FROM iceberg.market.dim_calendar_block
        WHERE calendar_name = %(calendar_name)s
        ORDER BY block_name
    """

    rows = await trino_client.execute_query(query, {"calendar_name": name}, use_cache=True)

    return [
        {
            "calendar_name": row.get("calendar_name"),
            "block_name": row.get("block_name"),
            "block_type": row.get("block_type"),
            "start_hour": row.get("start_hour"),
            "end_hour": row.get("end_hour"),
            "days_of_week": row.get("days_of_week"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def fetch_calendar_hours(
    name: str,
    block: str,
    date: Optional[str] = None,
    end: Optional[str] = None,
) -> List[Dict]:
    """Fetch calendar hours from dbt dimensions."""
    from .database.trino_client import get_trino_client

    trino_client = get_trino_client()

    query = """
        SELECT
            calendar_name,
            block_name,
            hour_date,
            hour_start,
            hour_end,
            is_business_hour,
            created_at
        FROM iceberg.market.dim_calendar_hour
        WHERE calendar_name = %(calendar_name)s
          AND block_name = %(block_name)s
    """

    params = {"calendar_name": name, "block_name": block}

    if date:
        query += " AND hour_date >= DATE(%(start_date)s)"
        params["start_date"] = date

    if end:
        query += " AND hour_date <= DATE(%(end_date)s)"
        params["end_date"] = end

    query += " ORDER BY hour_date, hour_start"

    rows = await trino_client.execute_query(query, params, use_cache=True)

    return [
        {
            "calendar_name": row.get("calendar_name"),
            "block_name": row.get("block_name"),
            "hour_date": row.get("hour_date"),
            "hour_start": row.get("hour_start"),
            "hour_end": row.get("hour_end"),
            "is_business_hour": row.get("is_business_hour"),
            "created_at": row.get("created_at"),
        }
        for row in rows
    ]


async def invalidate_curve_cache(
    iso: Optional[str] = None,
    market: Optional[str] = None,
    location: Optional[str] = None,
) -> None:
    """Invalidate curve cache using CacheManager."""
    from .cache.cache import CacheManager
    from .container import get_service

    cache_manager = get_service(CacheManager)

    # Build cache key pattern based on parameters
    key_pattern = "curves:"

    if iso:
        key_pattern += f"{hash(iso)}:"
    else:
        key_pattern += "*:"  # Wildcard for any ISO

    if market:
        key_pattern += f"{hash(market)}:"
    else:
        key_pattern += "*:"  # Wildcard for any market

    if location:
        key_pattern += f"{hash(location)}"
    else:
        key_pattern += "*"  # Wildcard for any location

    # Delete matching cache keys
    deleted_count = await cache_manager.delete_pattern(key_pattern)

    LOGGER.info(
        "Invalidated curve cache",
        extra={
            "iso": iso,
            "market": market,
            "location": location,
            "deleted_keys": deleted_count,
        }
    )
from .cache.global_manager import get_global_cache_manager
from .cache.utils import cache_get_or_set_sync, cache_get_sync, cache_set_sync
