"""Feature-flagged Trino client with simplified and legacy options.

Provides both simplified and complex Trino clients with feature flags
for gradual migration and comprehensive monitoring.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path
from collections import OrderedDict
from dataclasses import dataclass, field, fields, replace
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

try:
    import trino  # type: ignore
    import trino.dbapi  # type: ignore
except ImportError:
    # Trino is optional for import-time tooling (e.g., docs). Runtime paths
    # that require Trino will raise a clear error when invoked.
    trino = None  # type: ignore

from ..exceptions import ServiceUnavailableException
from .connection_pool import ConnectionPool
from ..telemetry.context import (
    get_correlation_id,
    get_request_id,
    get_tenant_id,
    log_structured,
)
from ...common.circuit_breaker import CircuitBreaker, CircuitBreakerState
from ...external.collect.base import RetryConfig
from ...observability.metrics import (
    increment_trino_queries,
    increment_trino_queue_rejections,
    observe_trino_connection_acquire_time,
    observe_trino_query_duration,
    set_trino_circuit_breaker_state,
    set_trino_connection_pool_active,
    set_trino_connection_pool_idle,
    set_trino_connection_pool_utilization,
    set_trino_pool_saturation,
    set_trino_pool_saturation_status,
    set_trino_request_queue_depth,
    increment_trino_hot_cache_hits,
    increment_trino_hot_cache_misses,
    set_trino_prepared_cache_entries,
    TRINO_PREPARED_CACHE_ENTRIES,
    TRINO_PREPARED_CACHE_EVICTIONS,
)
from ...observability.enhanced_tracing import get_current_trace_context

from .config import TrinoConfig, TrinoCatalogConfig, TrinoCatalogType, TrinoAccessLevel
from ...core.settings import AurumSettings, get_settings

LOGGER = logging.getLogger(__name__)

_catalog_configs: Dict[str, TrinoCatalogConfig] = {}
_catalog_trino_configs: Dict[str, TrinoConfig] = {}

_BOOL_CONCURRENCY_KEYS = {
    "trino_hot_query_cache_enabled",
    "trino_prepared_cache_metrics_enabled",
    "trino_trace_tagging_enabled",
}


def _infer_concurrency_config_from_settings(settings: AurumSettings) -> Dict[str, Any]:
    """Extract Trino concurrency defaults from application settings."""

    inferred: Dict[str, Any] = {}

    def _set(key: str, value: Any) -> None:
        if value is None:
            return
        if key in _BOOL_CONCURRENCY_KEYS:
            inferred[key] = bool(value)
        else:
            inferred[key] = value

    trino_cfg = getattr(settings, "trino", None)
    if trino_cfg is not None:
        _set("trino_hot_query_cache_enabled", getattr(trino_cfg, "hot_cache_enabled", None))
        _set(
            "trino_prepared_cache_metrics_enabled",
            getattr(trino_cfg, "prepared_cache_metrics_enabled", None),
        )
        _set("trino_trace_tagging_enabled", getattr(trino_cfg, "trace_tagging_enabled", None))
        _set("trino_metrics_label", getattr(trino_cfg, "metrics_label", None))
        _set("trino_default_tenant_id", getattr(trino_cfg, "default_tenant_id", None))

    api_cfg = getattr(settings, "api", None)
    concurrency_cfg = getattr(api_cfg, "concurrency", None) if api_cfg is not None else None
    if concurrency_cfg is not None:
        attr_map = {
            "trino_connection_timeout_seconds": "trino_connection_timeout_seconds",
            "trino_query_timeout_seconds": "trino_query_timeout_seconds",
            "trino_connection_acquire_attempts": "trino_connection_acquire_attempts",
            "trino_connection_backoff_seconds": "trino_connection_backoff_seconds",
            "trino_connection_pool_min_size": "trino_connection_pool_min_size",
            "trino_connection_pool_max_size": "trino_connection_pool_max_size",
            "trino_connection_pool_max_idle": "trino_connection_pool_max_idle",
            "trino_connection_idle_timeout_seconds": "trino_connection_idle_timeout_seconds",
            "trino_connection_wait_timeout_seconds": "trino_connection_wait_timeout_seconds",
            "trino_max_retries": "trino_max_retries",
            "trino_retry_delay_seconds": "trino_retry_delay_seconds",
            "trino_max_retry_delay_seconds": "trino_max_retry_delay_seconds",
            "trino_retry_backoff_factor": "trino_retry_backoff_factor",
            "trino_circuit_breaker_failure_threshold": "trino_circuit_breaker_failure_threshold",
            "trino_circuit_breaker_timeout_seconds": "trino_circuit_breaker_timeout_seconds",
            "trino_circuit_breaker_success_threshold": "trino_circuit_breaker_success_threshold",
            "trino_cache_max_entries": "trino_cache_max_entries",
            "trino_cache_ttl_seconds": "trino_cache_ttl_seconds",
            "trino_hot_query_cache_size": "trino_hot_query_cache_size",
            "trino_prepared_statement_cache_size": "trino_prepared_statement_cache_size",
            "trino_hot_query_cache_enabled": "trino_hot_query_cache_enabled",
            "trino_prepared_cache_metrics_enabled": "trino_prepared_cache_metrics_enabled",
            "trino_trace_tagging_enabled": "trino_trace_tagging_enabled",
            "trino_metrics_label": "trino_metrics_label",
        }

        for attr_name, key in attr_map.items():
            if hasattr(concurrency_cfg, attr_name):
                _set(key, getattr(concurrency_cfg, attr_name))

    return inferred


def _create_lock() -> asyncio.Lock:
    """Create asyncio locks even when no loop is yet running."""

    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return asyncio.Lock()


def _get_base_trino_config() -> TrinoConfig:
    """Return the base Trino configuration derived from settings when available."""

    try:
        settings = get_settings()
    except Exception:
        return TrinoConfig()
    return TrinoConfig.from_settings(settings)

# Feature flags for database migration (uppercase primary, lowercase fallback)
DB_FEATURE_FLAGS = {
    "USE_SIMPLE_DB_CLIENT": "AURUM_USE_SIMPLE_DB_CLIENT",
    "ENABLE_DB_MIGRATION_MONITORING": "AURUM_ENABLE_DB_MIGRATION_MONITORING",
    "DB_MIGRATION_PHASE": "AURUM_DB_MIGRATION_PHASE",  # "legacy", "hybrid", "simplified"
}


def _get_db_flag_env(flag_name: str, default: str = "") -> str:
    value = os.getenv(flag_name)
    if value is None:
        value = os.getenv(flag_name.lower())
    return value if value is not None else default


def _set_db_flag_env(flag_name: str, value: str) -> None:
    os.environ[flag_name] = value
    os.environ[flag_name.lower()] = value


@dataclass
class TimeoutConfig:
    """Timeout configuration for Trino connections and queries."""

    connection_timeout: float
    query_timeout: float


@dataclass
class CacheEntry:
    """Record cached query results with expiry information."""

    value: Any
    expires_at: float


class TTLCache:
    """Async-safe TTL cache with basic LRU eviction."""

    def __init__(self, maxsize: int = 128, default_ttl: float = 30.0):
        self.maxsize = maxsize
        self.default_ttl = default_ttl
        self._store: OrderedDict[str, CacheEntry] = OrderedDict()
        self._lock = _create_lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if entry.expires_at <= time.time():
                self._store.pop(key, None)
                return None
            self._store.move_to_end(key)
            return copy.deepcopy(entry.value)

    async def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        ttl_seconds = ttl if ttl is not None else self.default_ttl
        if ttl_seconds <= 0:
            return
        async with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = CacheEntry(copy.deepcopy(value), time.time() + ttl_seconds)
            while len(self._store) > self.maxsize:
                self._store.popitem(last=False)

    async def invalidate(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()


class SimpleTrinoClient:
    """High-level Trino client with pooling, retries, metrics, and caching."""

    def __init__(self, config: TrinoConfig, concurrency_config: Optional[Dict[str, Any]] = None):
        self.config = config
        self.logger = LOGGER
        self._config = concurrency_config or {}

        self.acquire_timeout = float(self._config.get("trino_connection_timeout_seconds", 5.0))
        self.query_timeout = float(self._config.get("trino_query_timeout_seconds", 30.0))
        self.acquire_attempts = int(max(1, self._config.get("trino_connection_acquire_attempts", 3)))
        self.acquire_backoff = float(max(0.01, self._config.get("trino_connection_backoff_seconds", 0.1)))

        pool_min_size = int(max(1, self._config.get("trino_connection_pool_min_size", 1)))
        pool_max_size = int(max(pool_min_size, self._config.get("trino_connection_pool_max_size", 5)))
        pool_max_idle = int(max(pool_min_size, self._config.get("trino_connection_pool_max_idle", pool_min_size)))
        idle_timeout = float(self._config.get("trino_connection_idle_timeout_seconds", 60.0))
        wait_timeout = float(self._config.get("trino_connection_wait_timeout_seconds", self.acquire_timeout))

        self._pool = ConnectionPool(
            config=self.config,
            min_size=pool_min_size,
            max_size=pool_max_size,
            max_idle=pool_max_idle,
            idle_timeout_seconds=idle_timeout,
            wait_timeout_seconds=wait_timeout,
        )

        self.retry_config = RetryConfig(
            max_retries=int(max(0, self._config.get("trino_max_retries", 1))),
            initial_delay=float(self._config.get("trino_retry_delay_seconds", 0.2)),
            max_delay=float(self._config.get("trino_max_retry_delay_seconds", 2.0)),
            backoff_factor=float(self._config.get("trino_retry_backoff_factor", 2.0)),
            max_backoff_seconds=float(self._config.get("trino_max_retry_delay_seconds", 2.0)),
        )

        self.timeout_config = TimeoutConfig(
            connection_timeout=self.acquire_timeout,
            query_timeout=self.query_timeout,
        )

        breaker_failure_threshold = int(max(1, self._config.get("trino_circuit_breaker_failure_threshold", 5)))
        breaker_timeout = float(self._config.get("trino_circuit_breaker_timeout_seconds", 60.0))
        breaker_success_threshold = int(max(1, self._config.get("trino_circuit_breaker_success_threshold", 3)))
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=breaker_failure_threshold,
            recovery_timeout=breaker_timeout,
            success_threshold=breaker_success_threshold,
            name="trino_client",
        )

        cache_max_entries = int(max(1, self._config.get("trino_cache_max_entries", 128)))
        cache_ttl_seconds = float(self._config.get("trino_cache_ttl_seconds", 30.0))
        self._cache = TTLCache(maxsize=cache_max_entries, default_ttl=cache_ttl_seconds)
        self._prepared_statement_max = int(max(1, self._config.get("trino_prepared_statement_cache_size", 64)))
        self._prepared_statements: Dict[int, "OrderedDict[str, Any]"] = {}
        self._prepared_lock = threading.Lock()
        self._hot_cache_max = int(max(1, self._config.get("trino_hot_query_cache_size", 32)))
        self._hot_results: OrderedDict[str, Any] = OrderedDict()
        self._hot_cache_lock = _create_lock()
        self._metrics_label = str(self._config.get("trino_metrics_label", "default"))
        self._enable_hot_cache = bool(self._config.get("trino_hot_query_cache_enabled", True))
        self._enable_trace_tagging = bool(self._config.get("trino_trace_tagging_enabled", True))
        self._enable_prepared_metrics = bool(self._config.get("trino_prepared_cache_metrics_enabled", True))
        if not self._enable_hot_cache:
            self._hot_cache_max = 0
        if not self._enable_hot_cache:
            self._hot_results.clear()

        self._pending_acquires = 0
        self._pending_lock = _create_lock()
        self._default_tenant = self._config.get("trino_default_tenant_id", "default")
        self._closed = False

    @property
    def circuit_breaker_state(self) -> str:
        state = getattr(self.circuit_breaker, "_state", CircuitBreakerState.CLOSED)
        return state.value

    def execute_query_sync(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        use_cache: bool | None = None,
        tenant_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Synchronous helper used by legacy service code paths.
        
        This is deprecated and should only be used from non-async contexts.
        Prefer execute_query() from async code.
        """
        try:
            loop = asyncio.get_running_loop()
            # We're in an async context - this is problematic
            raise RuntimeError(
                "execute_query_sync() called from async context. "
                "Use 'await execute_query()' instead, or call from synchronous code."
            )
        except RuntimeError as e:
            if "execute_query_sync() called from async context" in str(e):
                raise e
            # No event loop running, safe to create one
            async def _run() -> List[Dict[str, Any]]:
                return await self.execute_query(sql, params=params, tenant_id=tenant_id)
            return asyncio.run(_run())

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Alias for execute_query to maintain backwards compatibility."""

        return await self.execute_query(sql, params=params, tenant_id=tenant_id)

    async def execute(self, sql: str, params: Optional[Dict[str, Any]] = None, tenant_id: Optional[str] = None) -> None:
        """Execute a statement without returning results."""

        await self._run_with_retries(sql, params, tenant_id or self._default_tenant, self.query_timeout)
        if not self._is_select_query(sql):
            await self._cache.clear()
            await self._clear_hot_cache()

    async def execute_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        cache_ttl: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results with caching and retries."""

        tenant_label = tenant_id or self._default_tenant
        params_copy: Optional[Dict[str, Any]]
        if isinstance(params, dict):
            params_copy = dict(params)
        else:
            params_copy = params

        query_type = self._classify_query(sql)
        should_cache = False
        ttl = None
        if isinstance(params_copy, dict):
            should_cache, ttl = self._extract_cache_policy(sql, params_copy, cache_ttl)

        cache_key = None
        hot_key = None
        if should_cache:
            cache_key = self._cache_key(sql, params_copy)
            cached_result = await self._cache.get(cache_key)
            if cached_result is not None:
                try:
                    await increment_trino_queries(tenant_label, status="cache_hit")
                    await observe_trino_query_duration(tenant_label, self._classify_query(sql), 0.0)
                except Exception:
                    pass
                return cached_result

        if query_type == "read" and self._enable_hot_cache:
            hot_key = self._cache_key(sql, params_copy)
            hot_result = await self._get_hot_result(hot_key)
            if hot_result is not None:
                try:
                    await increment_trino_queries(tenant_label, status="hot_cache_hit")
                    await observe_trino_query_duration(tenant_label, query_type, 0.0)
                except Exception:
                    pass
                try:
                    await increment_trino_hot_cache_hits(tenant_label)
                except Exception:
                    pass
                return hot_result
            try:
                await increment_trino_hot_cache_misses(tenant_label)
            except Exception:
                pass
        elif self._enable_hot_cache:
            await self._clear_hot_cache()

        result = await self._run_with_retries(sql, params_copy, tenant_label, self.query_timeout)

        if should_cache and cache_key:
            await self._cache.set(cache_key, result, ttl)

        if query_type == "read" and hot_key and self._enable_hot_cache:
            await self._store_hot_result(hot_key, result)

        return result

    async def get_health_status(self) -> Dict[str, Any]:
        """Return a snapshot of pool and circuit breaker health."""

        active = getattr(self._pool, "_active_connections", 0)
        idle = len(getattr(self._pool, "_connections", []))
        total = getattr(self._pool, "_total_connections", active + idle)
        healthy = not self.circuit_breaker.is_open()
        await self._record_pool_metrics()
        await self._sync_circuit_breaker_metric()
        return {
            "healthy": healthy,
            "circuit_breaker_state": self.circuit_breaker_state,
            "pool_size": total,
            "active_connections": active,
            "idle_connections": idle,
            "queued_requests": self._pending_acquires,
        }

    async def close(self) -> None:
        """Close cached connections and clear caches."""

        if self._closed:
            return
        await self._cache.clear()
        await self._clear_hot_cache()
        with self._prepared_lock:
            self._prepared_statements.clear()
            if self._enable_prepared_metrics and TRINO_PREPARED_CACHE_ENTRIES is not None:
                try:
                    TRINO_PREPARED_CACHE_ENTRIES.set(0)
                except Exception:
                    pass
        await self._pool.close_all()
        self._closed = True

    async def _run_with_retries(
        self,
        sql: str,
        params: Optional[Dict[str, Any]],
        tenant_label: str,
        query_timeout: float,
    ) -> List[Dict[str, Any]]:
        attempt = 0
        last_error: Optional[Exception] = None

        while attempt <= self.retry_config.max_retries:
            if self.circuit_breaker.is_open():
                try:
                    await set_trino_circuit_breaker_state("open")
                except Exception:
                    pass
                raise ServiceUnavailableException("trino", detail="Circuit breaker open")

            try:
                start = time.perf_counter()
                rows = await self._execute_query_with_timeout(sql, params, query_timeout, tenant_label)
                duration = time.perf_counter() - start
                self.circuit_breaker.record_success()
                await self._sync_circuit_breaker_metric()
                try:
                    await increment_trino_queries(tenant_label, status="success")
                    await observe_trino_query_duration(tenant_label, self._classify_query(sql), duration)
                except Exception:
                    pass
                return rows
            except ServiceUnavailableException as exc:
                self._record_breaker_failure()
                await self._sync_circuit_breaker_metric()
                detail = getattr(exc, "detail", "") or ""
                status = "rejected"
                if "timeout" in detail.lower():
                    status = "timeout"
                try:
                    await increment_trino_queries(tenant_label, status=status)
                except Exception:
                    pass
                raise
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self._record_breaker_failure()
                await self._sync_circuit_breaker_metric()
                attempt += 1
                status = "retry" if attempt <= self.retry_config.max_retries else "failed"
                try:
                    await increment_trino_queries(tenant_label, status=status)
                except Exception:
                    pass
                if attempt > self.retry_config.max_retries:
                    raise
                backoff_factor = getattr(self.retry_config, "backoff_factor", 2.0)
                base_delay = getattr(self.retry_config, "initial_delay", 0.2)
                delay = min(base_delay * (backoff_factor ** (attempt - 1)), self.retry_config.max_delay)
                await asyncio.sleep(delay)

        if last_error:
            raise last_error
        return []

    async def _execute_query_with_timeout(
        self,
        sql: str,
        params: Optional[Dict[str, Any]],
        query_timeout: float,
        tenant_label: str,
    ) -> List[Dict[str, Any]]:
        conn = await self._acquire_connection()
        try:
            try:
                if query_timeout:
                    return await asyncio.wait_for(
                        self._run_query(conn, sql, params, tenant_label),
                        timeout=query_timeout,
                    )
                return await self._run_query(conn, sql, params, tenant_label)
            except asyncio.TimeoutError as exc:
                try:
                    cancel = getattr(conn, "cancel", None)
                    if callable(cancel):
                        cancel()
                except Exception:  # noqa: BLE001
                    pass
                log_structured(
                    "warning",
                    "trino_query_timeout",
                    sql=sql[:200] if sql else "",
                    timeout_seconds=query_timeout,
                )
                raise ServiceUnavailableException("trino", detail="Query timed out") from exc
        finally:
            await self._release_connection(conn)

    async def _run_query(
        self,
        conn: Any,
        sql: str,
        params: Optional[Dict[str, Any]],
        tenant_label: str,
    ) -> List[Dict[str, Any]]:
        context_info = self._build_query_context(tenant_label)

        def _do_query(context: Dict[str, Optional[str]]) -> List[Dict[str, Any]]:
            cursor = conn.cursor()
            try:
                prepared = None
                bind_params = params or {}
                if isinstance(params, dict) and self._is_select_query(sql):
                    prepared = self._maybe_prepare_statement(conn, cursor, sql)

                header_previous: Dict[str, Optional[str]] = {}
                if self._enable_trace_tagging:
                    header_previous = self._apply_connection_context(conn, context)
                start_time = time.perf_counter()
                try:
                    if prepared is not None and hasattr(cursor, "execute_prepared"):
                        cursor.execute_prepared(prepared, bind_params)
                    else:
                        cursor.execute(sql, bind_params)
                except Exception as exc:  # noqa: BLE001
                    if prepared is not None:
                        self._invalidate_prepared_statement(conn, sql)
                    if hasattr(conn, "rollback"):
                        try:
                            conn.rollback()
                        except Exception:  # noqa: BLE001
                            pass
                    raise
                finally:
                    if self._enable_trace_tagging:
                        self._restore_connection_headers(conn, header_previous)

                description = getattr(cursor, "description", None)
                has_results = bool(description)
                rows = []
                if has_results and hasattr(cursor, "fetchall"):
                    rows = cursor.fetchall()
                else:
                    if hasattr(conn, "commit"):
                        try:
                            conn.commit()
                        except Exception:  # noqa: BLE001
                            pass

                columns = [col[0] for col in description or []]
                duration = time.perf_counter() - start_time
                query_id = None
                stats = getattr(cursor, "stats", None)
                if isinstance(stats, dict):
                    query_id = stats.get("queryId") or stats.get("query_id")
                elif hasattr(cursor, "query_id"):
                    query_id = getattr(cursor, "query_id")
                if query_id:
                    log_structured(
                        "info",
                        "trino_query_completed",
                        query_id=query_id,
                        tenant=context.get("tenant"),
                        request_id=context.get("request_id"),
                        correlation_id=context.get("correlation_id"),
                        traceparent=context.get("traceparent"),
                        duration_seconds=round(duration, 6),
                        catalog=self.config.catalog,
                    )
                if columns:
                    return [dict(zip(columns, row)) for row in rows]
                return rows
            finally:
                try:
                    cursor.close()
                except Exception:  # noqa: BLE001
                    pass

        return await asyncio.to_thread(_do_query, context_info)

    async def _acquire_connection(self) -> Any:
        attempt = 0
        delay = self.acquire_backoff

        while attempt < max(1, self.acquire_attempts):
            attempt += 1
            async with self._pending_lock:
                self._pending_acquires += 1
                try:
                    await set_trino_request_queue_depth(self._pending_acquires)
                except Exception:
                    pass

            started = time.perf_counter()
            try:
                connection = await asyncio.wait_for(self._pool.get_connection(), timeout=self.acquire_timeout)
                try:
                    await observe_trino_connection_acquire_time(time.perf_counter() - started)
                except Exception:
                    pass
                await self._record_pool_metrics()
                return connection
            except asyncio.TimeoutError:
                try:
                    await increment_trino_queue_rejections("acquire_timeout")
                except Exception:
                    pass
                if attempt >= self.acquire_attempts:
                    raise ServiceUnavailableException("trino", detail="Connection acquire timeout") from None
            except ServiceUnavailableException:
                try:
                    await increment_trino_queue_rejections("pool_exhausted")
                except Exception:
                    pass
                if attempt >= self.acquire_attempts:
                    raise
            finally:
                async with self._pending_lock:
                    self._pending_acquires = max(0, self._pending_acquires - 1)
                    try:
                        await set_trino_request_queue_depth(self._pending_acquires)
                    except Exception:
                        pass

            await asyncio.sleep(delay)
            delay = min(delay * 2, self.retry_config.max_delay)

        raise ServiceUnavailableException("trino", detail="Unable to acquire connection")

    async def _release_connection(self, conn: Any) -> None:
        await self._pool.return_connection(conn)
        await self._record_pool_metrics()

    async def _record_pool_metrics(self) -> None:
        active = getattr(self._pool, "_active_connections", 0)
        idle = len(getattr(self._pool, "_connections", []))
        max_size = getattr(self._pool, "max_size", 1)
        utilization = active / float(max_size or 1)
        status = "healthy"
        if utilization >= 0.85:
            status = "critical"
        elif utilization >= 0.6:
            status = "warning"

        try:
            await set_trino_connection_pool_active(active)
            await set_trino_connection_pool_idle(idle)
            await set_trino_connection_pool_utilization(utilization)
            await set_trino_pool_saturation(utilization)
            await set_trino_pool_saturation_status(status)
        except Exception:
            pass

    async def _sync_circuit_breaker_metric(self) -> None:
        try:
            await set_trino_circuit_breaker_state(self.circuit_breaker_state)
        except Exception:
            pass

    def _record_breaker_failure(self) -> None:
        previous_state = getattr(self.circuit_breaker, "_state", CircuitBreakerState.CLOSED)
        self.circuit_breaker.record_failure()
        current_state = getattr(self.circuit_breaker, "_state", CircuitBreakerState.CLOSED)
        failures = getattr(self.circuit_breaker, "_failures", 0)

        if (
            current_state == CircuitBreakerState.CLOSED
            and failures >= self.circuit_breaker.failure_threshold
        ):
            old_state = current_state
            self.circuit_breaker._state = CircuitBreakerState.OPEN
            self.circuit_breaker._state_change_time = time.time()
            try:
                self.circuit_breaker._dispatch_alert(
                    old_state,
                    self.circuit_breaker._state,
                    "Failure threshold reached",
                )
            except Exception:  # noqa: BLE001
                pass

    def _cache_key(self, sql: str, params: Optional[Dict[str, Any]]) -> str:
        params_repr = json.dumps(params, sort_keys=True, default=str) if params else ""
        raw = f"{sql}|{params_repr}"
        return hashlib.sha1(raw.encode()).hexdigest()

    def _classify_query(self, sql: str) -> str:
        if not sql:
            return "unknown"
        prefix = sql.strip().split(None, 1)[0].lower()
        if prefix in {"select", "show", "describe", "with"}:
            return "read"
        if prefix in {"insert", "update", "delete", "merge"}:
            return "write"
        return prefix

    def _parse_cache_control(self, value: str) -> Optional[float]:
        if not value:
            return None
        for part in value.split(","):
            directive = part.strip()
            if directive.startswith("max-age="):
                try:
                    return float(directive.split("=", 1)[1])
                except ValueError:
                    return None
        return None

    def _is_select_query(self, sql: str) -> bool:
        if not sql:
            return False
        return self._classify_query(sql) == "read"

    async def _get_hot_result(self, key: str) -> Optional[List[Dict[str, Any]]]:
        if not self._enable_hot_cache:
            return None
        async with self._hot_cache_lock:
            result = self._hot_results.get(key)
            if result is None:
                return None
            self._hot_results.move_to_end(key)
            return copy.deepcopy(result)

    async def _store_hot_result(self, key: str, value: List[Dict[str, Any]]) -> None:
        if not self._enable_hot_cache:
            return
        async with self._hot_cache_lock:
            self._hot_results[key] = copy.deepcopy(value)
            self._hot_results.move_to_end(key)
            while len(self._hot_results) > self._hot_cache_max:
                self._hot_results.popitem(last=False)

    async def _clear_hot_cache(self) -> None:
        if not self._enable_hot_cache:
            return
        async with self._hot_cache_lock:
            if self._hot_results:
                self._hot_results.clear()

    def _total_prepared_entries_locked(self) -> int:
        return sum(len(cache) for cache in self._prepared_statements.values())

    def _update_prepared_cache_metrics_locked(self) -> None:
        if not self._enable_prepared_metrics or TRINO_PREPARED_CACHE_ENTRIES is None:
            return
        try:
            TRINO_PREPARED_CACHE_ENTRIES.set(self._total_prepared_entries_locked())
        except Exception:
            pass

    def _build_query_context(self, tenant_label: str) -> Dict[str, Optional[str]]:
        tenant = get_tenant_id(default=tenant_label) or tenant_label
        request_id = get_request_id()
        correlation_id = get_correlation_id()
        traceparent: Optional[str] = None
        try:
            trace_context = get_current_trace_context()
        except Exception:
            trace_context = None
        if trace_context is not None:
            traceparent = trace_context.to_traceparent()
        return {
            "tenant": tenant,
            "tenant_label": tenant_label,
            "request_id": request_id,
            "correlation_id": correlation_id,
            "traceparent": traceparent,
        }

    def _apply_connection_context(self, conn: Any, context: Dict[str, Optional[str]]) -> Dict[str, Optional[str]]:
        session = getattr(conn, "_http_session", None)
        if session is None or not hasattr(session, "headers"):
            return {}
        headers = session.headers
        overrides: Dict[str, str] = {}
        tenant = context.get("tenant")
        traceparent = context.get("traceparent")
        if traceparent:
            overrides["X-Trino-Trace-Token"] = traceparent
        if tenant:
            existing_tags = headers.get("X-Trino-Client-Tags")
            tag_set = {tag.strip() for tag in existing_tags.split(",") if tag.strip()} if existing_tags else set()
            tag_set.add(f"tenant:{tenant}")
            overrides["X-Trino-Client-Tags"] = ",".join(sorted(tag_set))
        info_payload = {
            "request_id": context.get("request_id"),
            "correlation_id": context.get("correlation_id"),
            "tenant": tenant,
            "traceparent": traceparent,
        }
        info_payload = {k: v for k, v in info_payload.items() if v}
        if info_payload:
            overrides["X-Trino-Client-Info"] = json.dumps(info_payload, separators=(",", ":"))

        previous: Dict[str, Optional[str]] = {}
        for key, value in overrides.items():
            previous[key] = headers.get(key)
            headers[key] = value
        return previous

    def _restore_connection_headers(self, conn: Any, previous: Dict[str, Optional[str]]) -> None:
        if not previous:
            return
        session = getattr(conn, "_http_session", None)
        if session is None or not hasattr(session, "headers"):
            return
        headers = session.headers
        for key, value in previous.items():
            if value is None:
                headers.pop(key, None)
            else:
                headers[key] = value

    def _maybe_prepare_statement(self, conn: Any, cursor: Any, sql: str) -> Optional[Any]:
        if not hasattr(cursor, "prepare"):
            return None
        conn_id = id(conn)
        with self._prepared_lock:
            cache = self._prepared_statements.setdefault(conn_id, OrderedDict())
            prepared = cache.get(sql)
            if prepared is None:
                try:
                    prepared = cursor.prepare(sql)
                except Exception:  # noqa: BLE001
                    return None
                cache[sql] = prepared
            cache.move_to_end(sql)
            while len(cache) > self._prepared_statement_max:
                cache.popitem(last=False)
                if self._enable_prepared_metrics and TRINO_PREPARED_CACHE_EVICTIONS is not None:
                    try:
                        TRINO_PREPARED_CACHE_EVICTIONS.inc()
                    except Exception:
                        pass
            if self._enable_prepared_metrics:
                self._update_prepared_cache_metrics_locked()
            return prepared

    def _invalidate_prepared_statement(self, conn: Any, sql: str) -> None:
        conn_id = id(conn)
        with self._prepared_lock:
            cache = self._prepared_statements.get(conn_id)
            if not cache:
                return
            cache.pop(sql, None)
            if not cache:
                self._prepared_statements.pop(conn_id, None)
            if self._enable_prepared_metrics:
                self._update_prepared_cache_metrics_locked()

    def _extract_cache_policy(
        self,
        sql: str,
        params: Dict[str, Any],
        explicit_ttl: Optional[float],
    ) -> Tuple[bool, Optional[float]]:
        if explicit_ttl is not None:
            ttl = float(explicit_ttl)
            return ttl > 0, ttl

        ttl = None
        if "_cache_ttl" in params:
            try:
                ttl = float(params.pop("_cache_ttl"))
            except (TypeError, ValueError):
                ttl = None

        if ttl is None and "_cache_control" in params:
            ttl = self._parse_cache_control(str(params.pop("_cache_control")))

        if ttl is None and sql.strip().upper().startswith(("SELECT", "SHOW", "DESCRIBE", "WITH")):
            ttl = self._cache.default_ttl

        return ttl is not None and ttl > 0, ttl


class TrinoClient(SimpleTrinoClient):
    """Legacy Trino client alias for backward compatibility."""
    pass


# Database migration metrics
class DatabaseMigrationMetrics:
    """Track database client migration metrics."""

    def __init__(self):
        self.metrics_file = Path.home() / ".aurum" / "db_migration_metrics.json"
        self.metrics_file.parent.mkdir(parents=True, exist_ok=True)
        self._load_metrics()

    def _load_metrics(self):
        """Load metrics from file."""
        if self.metrics_file.exists():
            try:
                with open(self.metrics_file, 'r') as f:
                    self._metrics = json.load(f)
            except Exception:
                self._metrics = self._default_metrics()
        else:
            self._metrics = self._default_metrics()

    def _default_metrics(self):
        """Get default metrics structure."""
        return {
            "db_migration": {
                "legacy_calls": 0,
                "simplified_calls": 0,
                "errors": 0,
                "performance_ms": [],
                "migration_phase": "legacy"
            }
        }

    def _save_metrics(self):
        """Save metrics to file."""
        try:
            with open(self.metrics_file, 'w') as f:
                json.dump(self._metrics, f, indent=2)
        except Exception as e:
            LOGGER.warning(f"Failed to save DB migration metrics: {e}")

    def record_db_call(self, client_type: str, duration_ms: float, error: bool = False):
        """Record a database client call."""
        if not self.is_monitoring_enabled():
            return

        metrics = self._metrics["db_migration"]
        if client_type == "simplified":
            metrics["simplified_calls"] += 1
        else:
            metrics["legacy_calls"] += 1

        if error:
            metrics["errors"] += 1

        metrics["performance_ms"].append(duration_ms)
        if len(metrics["performance_ms"]) > 1000:
            metrics["performance_ms"] = metrics["performance_ms"][-1000:]

        self._save_metrics()

    def is_monitoring_enabled(self) -> bool:
        """Check if database migration monitoring is enabled."""
        value = _get_db_flag_env(
            DB_FEATURE_FLAGS["ENABLE_DB_MIGRATION_MONITORING"],
            default="false",
        )
        return value.lower() in ("true", "1", "yes")

    def set_migration_phase(self, phase: str):
        """Set migration phase for database layer."""
        self._metrics["db_migration"]["migration_phase"] = phase
        self._save_metrics()


# Global database migration metrics
_db_migration_metrics = DatabaseMigrationMetrics()


def is_db_feature_enabled(flag: str) -> bool:
    """Check if a database feature flag is enabled."""
    return _get_db_flag_env(flag, default="false").lower() in ("true", "1", "yes")


def get_db_migration_phase() -> str:
    """Get current database migration phase."""
    phase = _get_db_flag_env(
        DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"],
        default="legacy",
    )
    _db_migration_metrics.set_migration_phase(phase)
    return phase


# Factory function for creating clients
def create_trino_client(
    config: TrinoConfig,
    concurrency_config: Optional[Dict[str, Any]] = None,
) -> SimpleTrinoClient:
    """Create a simplified Trino client."""
    return SimpleTrinoClient(config, concurrency_config=concurrency_config)


# Hybrid client manager with feature flags
class HybridTrinoClientManager:
    """Hybrid Trino client manager that can use either simplified or legacy clients."""

    _instance: Optional["HybridTrinoClientManager"] = None
    _lock = threading.Lock()

    def __init__(self):
        self._legacy_manager = None
        self._simple_clients: Dict[str, SimpleTrinoClient] = {}
        # Backwards-compatible alias for legacy tests that inspect _clients
        self._clients = self._simple_clients
        self._client_configs: Dict[str, TrinoConfig] = {}
        self._client_settings: Dict[str, Optional[Dict[str, Any]]] = {}
        self._manager_lock = threading.Lock()
        self._migration_phase = get_db_migration_phase()
        self._use_simple = is_db_feature_enabled(DB_FEATURE_FLAGS["USE_SIMPLE_DB_CLIENT"])

    @classmethod
    def get_instance(cls) -> "HybridTrinoClientManager":
        """Get singleton instance of HybridTrinoClientManager."""

        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _build_config_key(self, config: TrinoConfig) -> str:
        """Create a stable cache key for a Trino configuration."""

        return "|".join(
            [
                str(getattr(config, "host", "")),
                str(getattr(config, "port", "")),
                str(getattr(config, "http_scheme", "")),
                str(getattr(config, "catalog", "")),
                str(getattr(config, "schema", "")),
                str(getattr(config, "user", "")),
            ]
        )

    def _normalize_request(
        self,
        catalog_or_config: Union[str, TrinoConfig, Dict[str, Any], None],
    ) -> Tuple[str, TrinoConfig]:
        """Normalize inputs to a configuration cache key and TrinoConfig."""

        if catalog_or_config is None:
            config = _get_base_trino_config()
        elif isinstance(catalog_or_config, TrinoConfig):
            config = catalog_or_config
        elif isinstance(catalog_or_config, dict):
            allowed_keys = {field.name for field in fields(TrinoConfig)}
            filtered = {k: v for k, v in catalog_or_config.items() if k in allowed_keys}
            config = TrinoConfig(**filtered)
        elif isinstance(catalog_or_config, str):
            config = _catalog_trino_configs.get(catalog_or_config)
            if config is None:
                base = _get_base_trino_config()
                config = replace(base, catalog=catalog_or_config)
        elif all(hasattr(catalog_or_config, attr) for attr in ("host", "port", "user", "catalog", "schema", "http_scheme")):
            # Accept duck-typed config objects
            config = TrinoConfig(
                host=getattr(catalog_or_config, "host"),
                port=getattr(catalog_or_config, "port"),
                user=getattr(catalog_or_config, "user"),
                http_scheme=getattr(catalog_or_config, "http_scheme"),
                catalog=getattr(catalog_or_config, "catalog"),
                schema=getattr(catalog_or_config, "schema"),
                password=getattr(catalog_or_config, "password", None),
            )
        else:
            raise TypeError(
                "catalog_or_config must be a catalog string, TrinoConfig, dict, or object with TrinoConfig attributes"
            )

        key = self._build_config_key(config)
        return key, config

    def _ensure_simple_client(
        self,
        key: str,
        config: TrinoConfig,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ) -> SimpleTrinoClient:
        """Create or fetch a simplified Trino client for the given config."""

        with self._manager_lock:
            client = self._simple_clients.get(key)
            if client is None:
                client = create_trino_client(config, concurrency_config=concurrency_config)
                self._simple_clients[key] = client
                self._client_configs[key] = config
                self._client_settings[key] = copy.deepcopy(concurrency_config) if concurrency_config else None
            elif concurrency_config is not None and self._client_settings.get(key) != concurrency_config:
                # Preserve original configuration but surface a helpful log for mismatched reuse
                LOGGER.debug(
                    "Ignoring updated concurrency configuration for existing Trino client",
                    extra={
                        "client_key": key,
                        "requested_config": concurrency_config,
                        "existing_config": self._client_settings.get(key),
                    },
                )
        return client

    def _get_legacy_manager(self):
        """Get or create legacy Trino client manager."""

        if self._legacy_manager is None:
            from types import SimpleNamespace

            outer = self

            class LegacyManagerStub(SimpleNamespace):
                def __init__(self):
                    super().__init__()
                    self._clients: Dict[str, SimpleTrinoClient] = {}

                def get_client(
                    self,
                    catalog_or_config: Union[str, TrinoConfig, Dict[str, Any], None],
                    concurrency_config: Optional[Dict[str, Any]] = None,
                ):
                    key, config = outer._normalize_request(catalog_or_config)
                    client = self._clients.get(key)
                    if client is None:
                        client = create_trino_client(config, concurrency_config=concurrency_config)
                        self._clients[key] = client
                    return client

                async def close_all(self) -> None:
                    if not self._clients:
                        return
                    clients = list(self._clients.values())
                    self._clients.clear()
                    await asyncio.gather(*(client.close() for client in clients), return_exceptions=True)

            self._legacy_manager = LegacyManagerStub()
        return self._legacy_manager

    def get_client(
        self,
        catalog_or_config: Union[str, TrinoConfig, Dict[str, Any], None] = None,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ) -> SimpleTrinoClient:
        """Get a Trino client based on feature flags."""

        key, config = self._normalize_request(catalog_or_config)
        start_time = time.time()

        if self._use_simple or self._migration_phase in ("simplified", "hybrid"):
            client = self._ensure_simple_client(key, config, concurrency_config)
            client_type = "simplified"
        else:
            legacy_manager = self._get_legacy_manager()
            try:
                if concurrency_config is not None:
                    client = legacy_manager.get_client(config, concurrency_config=concurrency_config)
                else:
                    client = legacy_manager.get_client(config)
            except TypeError:
                client = legacy_manager.get_client(getattr(config, "catalog", config))
            client_type = "legacy"

        duration_ms = (time.time() - start_time) * 1000
        _db_migration_metrics.record_db_call(client_type, duration_ms)

        return client

    async def close_all(self) -> None:
        """Close all managed clients asynchronously."""

        with self._manager_lock:
            simple_clients = list(self._simple_clients.values())
            self._simple_clients.clear()
            self._client_configs.clear()
            self._client_settings.clear()

        if simple_clients:
            await asyncio.gather(*(client.close() for client in simple_clients), return_exceptions=True)

        if self._legacy_manager:
            maybe_coro = self._legacy_manager.close_all()
            if asyncio.iscoroutine(maybe_coro):
                await maybe_coro

    def close_all_sync(self) -> Optional[asyncio.Task]:
        """Close clients from synchronous code without double-running event loops.
        
        Returns a Task if called from async context, None if called from sync context.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No event loop running, safe to use asyncio.run
            asyncio.run(self.close_all())
            return None
        else:
            # We're in an async context, return a task that the caller can await
            return loop.create_task(self.close_all())

    def switch_to_simplified(self) -> bool:
        """Switch to simplified clients."""

        self._use_simple = True
        LOGGER.info("Switched to simplified Trino clients")
        return True

    def switch_to_legacy(self) -> bool:
        """Switch to legacy clients."""

        if self._legacy_manager is None:
            LOGGER.warning("No legacy manager initialized; staying on simplified clients")
            return False
        self._use_simple = False
        LOGGER.info("Switched to legacy Trino clients")
        return True

    def get_default_client(self) -> Optional[SimpleTrinoClient]:
        """Get the default Trino client if it has been initialized."""

        key, config = self._normalize_request(None)
        if self._use_simple or self._migration_phase in ("simplified", "hybrid"):
            with self._manager_lock:
                client = self._simple_clients.get(key)
            if client is not None:
                return client
            return self._ensure_simple_client(key, config)

        legacy_manager = self._get_legacy_manager()
        return legacy_manager.get_client(config)


# Global instance for backward compatibility


def get_trino_client(
    catalog: Union[str, TrinoConfig, Dict[str, Any], None] = "iceberg",
    concurrency_config: Optional[Dict[str, Any]] = None,
):
    """Get a Trino client for the specified catalog or configuration."""

    inferred_defaults: Dict[str, Any] = {}
    try:
        settings = get_settings()
    except Exception:
        settings = None
    else:
        inferred_defaults = _infer_concurrency_config_from_settings(settings)

    effective_config: Optional[Dict[str, Any]]
    if concurrency_config is None:
        effective_config = inferred_defaults or None
    else:
        # Preserve caller-provided overrides while layering in defaults from settings.
        if inferred_defaults:
            merged: Dict[str, Any] = dict(inferred_defaults)
            merged.update(concurrency_config)
            effective_config = merged
        else:
            effective_config = dict(concurrency_config)

    return _client_manager.get_client(catalog, concurrency_config=effective_config)


def get_default_trino_client():
    """Get the default Trino client."""
    return get_trino_client("iceberg")


def get_trino_client_by_catalog(catalog: str):
    """Get Trino client for specified catalog by catalog type."""
    return _client_manager.get_client(catalog)

def configure_trino_catalogs(
    catalog_configs: Sequence[TrinoCatalogConfig],
) -> Dict[str, TrinoCatalogConfig]:
    """Register available Trino catalog configurations."""

    global _catalog_configs, _catalog_trino_configs

    if not isinstance(catalog_configs, Sequence):
        raise TypeError("catalog_configs must be a sequence of TrinoCatalogConfig")

    updated_configs: Dict[str, TrinoCatalogConfig] = dict(_catalog_configs)
    updated_trino_configs: Dict[str, TrinoConfig] = dict(_catalog_trino_configs)

    for cfg in catalog_configs:
        if not isinstance(cfg, TrinoCatalogConfig):
            raise TypeError("catalog configuration entries must be TrinoCatalogConfig instances")
        updated_configs[cfg.catalog] = cfg
        updated_trino_configs[cfg.catalog] = TrinoConfig(
            host=cfg.host,
            port=cfg.port,
            user=cfg.user,
            http_scheme=cfg.http_scheme,
            catalog=cfg.catalog,
            schema=cfg.schema,
            password=cfg.password,
        )

    _catalog_configs = updated_configs
    _catalog_trino_configs = updated_trino_configs
    return dict(_catalog_configs)


def get_trino_catalog_config(
    catalog: Optional[str] = None,
) -> Union[TrinoCatalogConfig, Dict[str, TrinoCatalogConfig]]:
    """Fetch configured Trino catalog metadata.

    When no catalogs have been configured explicitly, defaults are inferred from
    application settings (raw + market catalogs).
    """

    if not _catalog_configs:
        base = _get_base_trino_config()
        defaults = [
            TrinoCatalogConfig(
                host=base.host,
                port=base.port,
                user=base.user,
                http_scheme=base.http_scheme,
                catalog=TrinoCatalogType.RAW.value,
                schema=base.schema,
                access_level=TrinoAccessLevel.READ_ONLY,
                password=base.password,
            ),
            TrinoCatalogConfig(
                host=base.host,
                port=base.port,
                user=base.user,
                http_scheme=base.http_scheme,
                catalog=TrinoCatalogType.MARKET.value,
                schema=base.schema,
                access_level=TrinoAccessLevel.READ_WRITE,
                password=base.password,
            ),
        ]
        configure_trino_catalogs(defaults)

    if catalog is None:
        return dict(_catalog_configs)

    try:
        return _catalog_configs[catalog]
    except KeyError as exc:
        raise KeyError(f"Unknown Trino catalog '{catalog}'") from exc


class TrinoClientManager(HybridTrinoClientManager):
    """Compatibility shim that exposes the hybrid manager under a legacy name."""

    @classmethod
    def get_instance(cls) -> HybridTrinoClientManager:
        return HybridTrinoClientManager.get_instance()


_client_manager = TrinoClientManager.get_instance()

# Migration management functions
def advance_db_migration_phase(phase: str = "hybrid") -> bool:
    """Advance database migration phase."""
    _set_db_flag_env(DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"], phase)
    _db_migration_metrics.set_migration_phase(phase)
    LOGGER.info(f"Advanced database migration to phase: {phase}")
    return True


def rollback_db_migration_phase() -> bool:
    """Rollback database migration phase."""
    current_phase = get_db_migration_phase()
    if current_phase == "simplified":
        advance_db_migration_phase("hybrid")
        LOGGER.warning("Rolled back database migration from simplified to hybrid")
        return True
    elif current_phase == "hybrid":
        advance_db_migration_phase("legacy")
        LOGGER.warning("Rolled back database migration from hybrid to legacy")
        return True
    return False


def get_db_migration_status() -> Dict[str, Any]:
    """Get database migration status."""
    return {
        "migration_phase": get_db_migration_phase(),
        "using_simple": _client_manager._use_simple,
        "has_simple_clients": len(_client_manager._simple_clients) > 0,
        "monitoring_enabled": _db_migration_metrics.is_monitoring_enabled(),
    }


# Add to __all__ for proper exports
__all__ = [
    "SimpleTrinoClient",
    "create_trino_client",
    "HybridTrinoClientManager",
    "get_trino_client",
    "get_default_trino_client",
    "advance_db_migration_phase",
    "rollback_db_migration_phase",
    "get_db_migration_status",
    "DB_FEATURE_FLAGS",
    "DatabaseMigrationMetrics",
]
