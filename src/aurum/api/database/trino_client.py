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
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Optional, Tuple, Union

try:
    import trino  # type: ignore
    import trino.dbapi  # type: ignore
except ImportError:
    # Trino is optional for import-time tooling (e.g., docs). Runtime paths
    # that require Trino will raise a clear error when invoked.
    trino = None  # type: ignore

from ..exceptions import ServiceUnavailableException
from ..performance import ConnectionPool
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
)

from .config import TrinoConfig

LOGGER = logging.getLogger(__name__)


def _create_lock() -> asyncio.Lock:
    """Create asyncio locks even when no loop is yet running."""

    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    return asyncio.Lock()

# Feature flags for database migration
DB_FEATURE_FLAGS = {
    "USE_SIMPLE_DB_CLIENT": "aurum_use_simple_db_client",
    "ENABLE_DB_MIGRATION_MONITORING": "aurum_enable_db_migration_monitoring",
    "DB_MIGRATION_PHASE": "aurum_db_migration_phase",  # "legacy", "hybrid", "simplified"
}


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
        self._prepared_statements: Dict[str, Any] = {}

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
        """Synchronous helper used by legacy service code paths."""

        async def _run() -> List[Dict[str, Any]]:
            return await self.execute_query(sql, params=params, tenant_id=tenant_id)

        return asyncio.run(_run())

    async def query(self, sql: str, params: Optional[Dict[str, Any]] = None, tenant_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Alias for execute_query to maintain backwards compatibility."""

        return await self.execute_query(sql, params=params, tenant_id=tenant_id)

    async def execute(self, sql: str, params: Optional[Dict[str, Any]] = None, tenant_id: Optional[str] = None) -> None:
        """Execute a statement without returning results."""

        await self._run_with_retries(sql, params, tenant_id or self._default_tenant, self.query_timeout)

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

        should_cache = False
        ttl = None
        if isinstance(params_copy, dict):
            should_cache, ttl = self._extract_cache_policy(sql, params_copy, cache_ttl)

        cache_key = None
        if should_cache:
            cache_key = self._cache_key(sql, params_copy)
            cached_result = await self._cache.get(cache_key)
            if cached_result is not None:
                try:
                    await increment_trino_queries(tenant_label, status="cache_hit")
                    await observe_trino_query_duration(tenant_label, self._classify_query(sql), 0.0)
                except NameError:
                    pass
                return cached_result

        result = await self._run_with_retries(sql, params_copy, tenant_label, self.query_timeout)

        if should_cache and cache_key:
            await self._cache.set(cache_key, result, ttl)

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
                except NameError:
                    pass
                raise ServiceUnavailableException("trino", detail="Circuit breaker open")

            try:
                start = time.perf_counter()
                rows = await self._execute_query_with_timeout(sql, params, query_timeout)
                duration = time.perf_counter() - start
                self.circuit_breaker.record_success()
                await self._sync_circuit_breaker_metric()
                try:
                    await increment_trino_queries(tenant_label, status="success")
                    await observe_trino_query_duration(tenant_label, self._classify_query(sql), duration)
                except NameError:
                    pass
                return rows
            except ServiceUnavailableException:
                self._record_breaker_failure()
                await self._sync_circuit_breaker_metric()
                try:
                    await increment_trino_queries(tenant_label, status="rejected")
                except NameError:
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
                except NameError:
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
    ) -> List[Dict[str, Any]]:
        conn = await self._acquire_connection()
        try:
            if query_timeout:
                return await asyncio.wait_for(self._run_query(conn, sql, params), timeout=query_timeout)
            return await self._run_query(conn, sql, params)
        finally:
            await self._release_connection(conn)

    async def _run_query(
        self,
        conn: Any,
        sql: str,
        params: Optional[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        def _do_query() -> List[Dict[str, Any]]:
            cursor = conn.cursor()
            try:
                try:
                    cursor.execute(sql, params or {})
                except Exception as exc:  # noqa: BLE001
                    if hasattr(conn, "rollback"):
                        try:
                            conn.rollback()
                        except Exception:  # noqa: BLE001
                            pass
                    raise

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
                if columns:
                    return [dict(zip(columns, row)) for row in rows]
                return rows
            finally:
                try:
                    cursor.close()
                except Exception:  # noqa: BLE001
                    pass

        return await asyncio.to_thread(_do_query)

    async def _acquire_connection(self) -> Any:
        attempt = 0
        delay = self.acquire_backoff

        while attempt < max(1, self.acquire_attempts):
            attempt += 1
            async with self._pending_lock:
                self._pending_acquires += 1
                try:
                    await set_trino_request_queue_depth(self._pending_acquires)
                except NameError:
                    pass

            started = time.perf_counter()
            try:
                connection = await asyncio.wait_for(self._pool.get_connection(), timeout=self.acquire_timeout)
                try:
                    await observe_trino_connection_acquire_time(time.perf_counter() - started)
                except NameError:
                    pass
                await self._record_pool_metrics()
                return connection
            except asyncio.TimeoutError:
                try:
                    await increment_trino_queue_rejections("acquire_timeout")
                except NameError:
                    pass
                if attempt >= self.acquire_attempts:
                    raise ServiceUnavailableException("trino", detail="Connection acquire timeout") from None
            except ServiceUnavailableException:
                try:
                    await increment_trino_queue_rejections("pool_exhausted")
                except NameError:
                    pass
                if attempt >= self.acquire_attempts:
                    raise
            finally:
                async with self._pending_lock:
                    self._pending_acquires = max(0, self._pending_acquires - 1)
                    try:
                        await set_trino_request_queue_depth(self._pending_acquires)
                    except NameError:
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
        except NameError:
            pass

    async def _sync_circuit_breaker_metric(self) -> None:
        try:
            await set_trino_circuit_breaker_state(self.circuit_breaker_state)
        except NameError:
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
        return os.getenv(DB_FEATURE_FLAGS["ENABLE_DB_MIGRATION_MONITORING"], "false").lower() in ("true", "1", "yes")

    def set_migration_phase(self, phase: str):
        """Set migration phase for database layer."""
        self._metrics["db_migration"]["migration_phase"] = phase
        self._save_metrics()


# Global database migration metrics
_db_migration_metrics = DatabaseMigrationMetrics()


def is_db_feature_enabled(flag: str) -> bool:
    """Check if a database feature flag is enabled."""
    return os.getenv(flag, "false").lower() in ("true", "1", "yes")


def get_db_migration_phase() -> str:
    """Get current database migration phase."""
    phase = os.getenv(DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"], "legacy")
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
            config = TrinoConfig()
        elif isinstance(catalog_or_config, TrinoConfig):
            config = catalog_or_config
        elif isinstance(catalog_or_config, dict):
            allowed_keys = {field.name for field in fields(TrinoConfig)}
            filtered = {k: v for k, v in catalog_or_config.items() if k in allowed_keys}
            config = TrinoConfig(**filtered)
        elif isinstance(catalog_or_config, str):
            config = TrinoConfig(catalog=catalog_or_config)
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
        """Close clients from synchronous code without double-running event loops."""

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(self.close_all())
            return None
        else:
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
_client_manager = HybridTrinoClientManager()


def get_trino_client(
    catalog: Union[str, TrinoConfig, Dict[str, Any], None] = "iceberg",
    concurrency_config: Optional[Dict[str, Any]] = None,
):
    """Get a Trino client for the specified catalog or configuration."""

    return _client_manager.get_client(catalog, concurrency_config=concurrency_config)


def get_default_trino_client():
    """Get the default Trino client."""
    return get_trino_client("iceberg")


def get_trino_client_by_catalog(catalog: str):
    """Get Trino client for specified catalog by catalog type."""
    return _client_manager.get_client(catalog)

def get_trino_catalog_config():
    """Get Trino catalog configuration."""
    # This is a placeholder implementation
    # In a real implementation, this would return catalog configuration
    return {}

def configure_trino_catalogs(catalog_configs):
    """Configure Trino catalogs."""
    # This is a placeholder implementation
    # In a real implementation, this would configure the catalogs
    pass


class TrinoClientManager:
    """Manager for Trino clients."""

    def __init__(self):
        """Initialize the TrinoClientManager."""
        pass

    def get_client(self, catalog: str):
        """Get a Trino client for the specified catalog."""
        return create_trino_client(TrinoConfig())

# Migration management functions
def advance_db_migration_phase(phase: str = "hybrid") -> bool:
    """Advance database migration phase."""
    os.environ[DB_FEATURE_FLAGS["DB_MIGRATION_PHASE"]] = phase
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
