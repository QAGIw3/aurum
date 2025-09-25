"""Enhanced Trino client with concurrency controls, timeouts, and resilience patterns.

Features:
- Connection pooling with async acquisition and health checks
- Circuit breaker with HALF_OPEN testing and retry backoff
- Query timeouts with threadpool execution of DB-API calls
- Normalized query stats + structured logging for observability

Configuration keys (via `concurrency_config`):
- `trino_max_retries`, `trino_retry_delay_seconds`, `trino_max_retry_delay_seconds`
- `trino_connection_timeout_seconds`, `trino_query_timeout_seconds`
- `trino_circuit_breaker_failure_threshold`, `trino_circuit_breaker_timeout_seconds`
- `trino_connection_pool_{min_size,max_size,max_idle,idle_timeout_seconds,wait_timeout_seconds}`

See docs:
- docs/structured_logging_guide.md
- docs/quotas_and_concurrency.md
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import time
import threading
from contextlib import asynccontextmanager
import threading
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, AsyncIterator
from concurrent.futures import ThreadPoolExecutor

try:
    import trino  # type: ignore
    import trino.dbapi  # type: ignore
except ImportError:
    # Trino is optional for import-time tooling (e.g., docs). Runtime paths
    # that require Trino will raise a clear error when invoked.
    trino = None  # type: ignore

from ..telemetry.context import log_structured
from ..telemetry.context import get_tenant_id
from ..observability.metrics import (
    increment_trino_queries,
    observe_trino_query_duration,
    set_trino_connection_pool_active,
    set_trino_connection_pool_idle,
    set_trino_connection_pool_utilization,
    observe_trino_connection_acquire_time,
    set_trino_circuit_breaker_state,
    set_trino_pool_saturation,
    set_trino_pool_saturation_status,
    set_trino_request_queue_depth,
    increment_trino_queue_rejections,
)
from .config import TrinoCatalogConfig, TrinoAccessLevel, TrinoCatalogType
from .config import TrinoConfig
from .exceptions import ServiceUnavailableException

LOGGER = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


from aurum.common.circuit_breaker import CircuitBreaker as _CommonCircuitBreaker


class CircuitBreaker(_CommonCircuitBreaker):
    """Adapter around the shared circuit breaker with Trino's enum state API."""

    def __init__(self, failure_threshold: int = 5, timeout_seconds: float = 60.0) -> None:
        super().__init__(failure_threshold=failure_threshold, recovery_timeout=timeout_seconds)

    def get_state(self) -> CircuitBreakerState:
        # Progress OPEN -> HALF_OPEN if recovery timeout elapsed
        _ = self.is_open()
        state = getattr(self, "_state", "CLOSED")
        if state == "OPEN":
            return CircuitBreakerState.OPEN
        if state == "HALF_OPEN":
            return CircuitBreakerState.HALF_OPEN
        return CircuitBreakerState.CLOSED


class ConnectionPool:
    """Simple connection pool for Trino connections with basic instrumentation."""

    def __init__(
        self,
        config: TrinoConfig,
        min_size: int = 2,
        max_size: int = 10,
        max_idle: int = 5,
        idle_timeout_seconds: float = 300.0,
        wait_timeout_seconds: float = 10.0,
    ):
        self.config = config
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle = max_idle
        self.idle_timeout_seconds = idle_timeout_seconds
        self.wait_timeout_seconds = wait_timeout_seconds

        self._pool: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._closed = False

        self._initialized = False
        self._active_connections = 0
        self._total_connections = 0

    async def _register_connection(self, *, active: bool) -> None:
        async with self._stats_lock:
            self._total_connections += 1
            if active:
                self._active_connections += 1

    async def _mark_active(self) -> None:
        async with self._stats_lock:
            self._active_connections += 1

    async def _mark_idle(self) -> None:
        async with self._stats_lock:
            self._active_connections = max(0, self._active_connections - 1)

    async def _decrement_total(self) -> None:
        async with self._stats_lock:
            self._total_connections = max(0, self._total_connections - 1)

    async def _ensure_initialized(self) -> None:
        """Ensure pool is initialized with minimum connections."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            for _ in range(self.min_size):
                try:
                    conn = await self._create_connection()
                    await self._pool.put(conn)
                    await self._register_connection(active=False)
                except Exception as exc:  # pragma: no cover - defensive
                    LOGGER.warning("Failed to create initial Trino connection: %s", exc)
                    break

            self._initialized = True

    async def _create_connection(self) -> Any:
        """Create a new Trino connection."""
        if trino is None:
            raise RuntimeError("trino package not available")

        def _connect():
            return trino.dbapi.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                catalog=self.config.catalog,
                schema=self.config.database_schema,
                http_scheme=self.config.http_scheme,
            )

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _connect)

    async def get_connection(self) -> Any:
        """Get a connection from the pool, creating one if capacity allows."""
        await self._ensure_initialized()

        try:
            conn = self._pool.get_nowait()
            await self._mark_active()
            return conn
        except asyncio.QueueEmpty:
            pass

        async with self._lock:
            if self._total_connections < self.max_size:
                try:
                    conn = await self._create_connection()
                    await self._register_connection(active=True)
                    return conn
                except Exception as exc:  # pragma: no cover - defensive
                    log_structured(
                        "error",
                        "trino_connection_creation_failed",
                        error=str(exc),
                        max_connections=self.max_size,
                    )
                    raise

        try:
            conn = await asyncio.wait_for(self._pool.get(), timeout=self.wait_timeout_seconds)
            await self._mark_active()
            return conn
        except asyncio.TimeoutError as exc:
            raise ServiceUnavailableException(
                "trino",
                "Connection pool timeout exceeded",
            ) from exc

    async def return_connection(self, conn: Any) -> None:
        """Return a connection to the pool, closing if the pool is shutting down."""
        await self._mark_idle()

        if self._closed:
            if hasattr(conn, "close"):
                conn.close()
            await self._decrement_total()
            return

        try:
            if hasattr(conn, "ping"):
                try:
                    conn.ping()
                except Exception:
                    if hasattr(conn, "close"):
                        conn.close()
                    await self._decrement_total()
                    return

            await self._pool.put(conn)
        except Exception:  # pragma: no cover - defensive
            if hasattr(conn, "close"):
                conn.close()
            await self._decrement_total()

    async def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True

        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                if hasattr(conn, "close"):
                    conn.close()
            except Exception:  # pragma: no cover - defensive
                pass

        async with self._stats_lock:
            self._active_connections = 0
            self._total_connections = 0

    async def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of pool utilisation."""
        async with self._stats_lock:
            active = self._active_connections
            total = self._total_connections
        idle = self._pool.qsize()
        total = max(total, active + idle)
        utilization = (active / total) if total else 0.0
        return {
            "active_connections": active,
            "idle_connections": idle,
            "total_connections": total,
            "max_connections": self.max_size,
            "pool_utilization": utilization,
        }


class RetryConfig:
    """Configuration for retry behavior."""
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 1.0,
        max_delay: float = 10.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        delay = min(
            self.initial_delay * (self.backoff_factor ** attempt),
            self.max_delay
        )

        if self.jitter:
            # Add random jitter up to 25% of delay
            jitter_amount = delay * 0.25 * random.random()
            delay += jitter_amount

        return delay


class TimeoutConfig:
    """Configuration for timeouts."""
    def __init__(
        self,
        connection_timeout: float = 5.0,
        query_timeout: float = 30.0,
    ):
        self.connection_timeout = connection_timeout
        self.query_timeout = query_timeout


class TrinoClient:
    """Enhanced Trino client with concurrency controls and resilience."""

    def __init__(
        self,
        config: TrinoConfig,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ):
        self.config = config
        self.concurrency_config = concurrency_config or {}

        # Initialize lineage tracking
        self._lineage_tags: List[str] = []

        # Initialize configurations
        self.retry_config = RetryConfig(
            max_retries=self.concurrency_config.get("trino_max_retries", 3),
            initial_delay=self.concurrency_config.get("trino_retry_delay_seconds", 1.0),
            max_delay=self.concurrency_config.get("trino_max_retry_delay_seconds", 10.0),
        )

        self.timeout_config = TimeoutConfig(
            connection_timeout=self.concurrency_config.get("trino_connection_timeout_seconds", 5.0),
            query_timeout=self.concurrency_config.get("trino_query_timeout_seconds", 30.0),
        )

        # Initialize circuit breaker
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=self.concurrency_config.get("trino_circuit_breaker_failure_threshold", 5),
            timeout_seconds=self.concurrency_config.get("trino_circuit_breaker_timeout_seconds", 60.0),
        )

        # Initialize connection pool
        self.connection_pool = ConnectionPool(
            config=config,
            min_size=self.concurrency_config.get("trino_connection_pool_min_size", 2),
            max_size=self.concurrency_config.get("trino_connection_pool_max_size", 10),
            max_idle=self.concurrency_config.get("trino_connection_pool_max_idle", 5),
            idle_timeout_seconds=self.concurrency_config.get("trino_connection_pool_idle_timeout_seconds", 300.0),
            wait_timeout_seconds=self.concurrency_config.get("trino_connection_pool_wait_timeout_seconds", 10.0),
        )

        self._max_concurrent = max(1, int(self.concurrency_config.get("max_concurrent_trino_connections", 10)))
        self._queue_max = max(0, int(self.concurrency_config.get("trino_queue_size", 0)))
        self._queue_timeout = float(
            self.concurrency_config.get(
                "trino_queue_timeout_seconds",
                self.concurrency_config.get("trino_connection_pool_wait_timeout_seconds", 10.0),
            )
        )
        self._saturation_warning = float(self.concurrency_config.get("trino_saturation_warning_threshold", 0.75))
        self._saturation_critical = float(self.concurrency_config.get("trino_saturation_critical_threshold", 0.9))

        self._semaphore = asyncio.Semaphore(self._max_concurrent)
        self._queue_lock = asyncio.Lock()
        self._active_lock = asyncio.Lock()
        self._queue_waiters = 0
        self._active_queries = 0

        self._start_time = time.time()
        self._total_queries = 0
        self._total_errors = 0

        # Initialize thread pool for sync operations
        self._executor = ThreadPoolExecutor(
            max_workers=self._max_concurrent,
            thread_name_prefix="trino-client"
        )

    async def execute_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        *,
        use_cache: bool = False,
        cache_ttl_seconds: Optional[int] = None,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """Execute a Trino query with retry logic, caching, and observability."""

        tenant_id = tenant_id or get_tenant_id()

        # Check circuit breaker
        if self.circuit_breaker.is_open():
            raise ServiceUnavailableException(
                "trino",
                "Circuit breaker is open - service temporarily unavailable"
            )

        last_exception: Optional[Exception] = None

        for attempt in range(self.retry_config.max_retries + 1):
            stats: Optional[Dict[str, Any]] = None
            acquired_slot = False

            try:
                await self._acquire_slot()
                acquired_slot = True
            except ServiceUnavailableException as exc:
                last_exception = exc
                log_structured(
                    "warning",
                    "trino_queue_rejected",
                    tenant_id=tenant_id,
                    attempt=attempt + 1,
                    queue_limit=self._queue_max,
                    error_message=str(exc),
                )
                break

            try:
                if use_cache:
                    result = await self._execute_with_cache(
                        query,
                        params=params,
                        tenant_id=tenant_id,
                        cache_ttl_seconds=cache_ttl_seconds,
                        force_refresh=force_refresh,
                    )
                else:
                    results, stats = await self._execute_query_with_timeout(query, params, tenant_id)
                    await self._record_query_observability(
                        query,
                        stats,
                        len(results),
                        tenant_id,
                        cached=False,
                    )
                    result = results

                self.circuit_breaker.record_success()
                self._total_queries += 1

                await increment_trino_queries(tenant_id, "success")
                if stats:
                    duration_seconds = self._duration_from_stats(stats)
                    await observe_trino_query_duration(
                        tenant_id,
                        self._classify_query_type(query),
                        duration_seconds,
                    )

                log_structured(
                    "info",
                    "trino_query_success",
                    tenant_id=tenant_id,
                    attempt=attempt + 1,
                    query_length=len(query),
                    query_timeout=self.timeout_config.query_timeout,
                )

                return result

            except Exception as exc:
                last_exception = exc
                self._total_errors += 1
                self.circuit_breaker.record_failure()
                await increment_trino_queries(tenant_id, "error")

                if attempt >= self.retry_config.max_retries:
                    break

                delay = self.retry_config.calculate_delay(attempt)
                log_structured(
                    "warning",
                    "trino_query_retry",
                    tenant_id=tenant_id,
                    attempt=attempt + 1,
                    error_type=exc.__class__.__name__,
                    error_message=str(exc),
                    next_retry_delay=delay,
                )

                await asyncio.sleep(delay)

            finally:
                if acquired_slot:
                    await self._release_slot()

        log_structured(
            "error",
            "trino_query_failed",
            tenant_id=tenant_id,
            total_attempts=self.retry_config.max_retries + 1,
            error_type=last_exception.__class__.__name__ if last_exception else "Unknown",
            error_message=str(last_exception) if last_exception else "Unknown",
        )

        raise last_exception

    # --- Synchronous convenience wrappers ---
    def execute_query_sync(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        *,
        use_cache: bool = False,
        cache_ttl_seconds: Optional[int] = None,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """Blocking wrapper around ``execute_query``.

        Avoids using asyncio.run in API paths. If already inside a running
        event loop, runs the coroutine in a dedicated thread-bound loop.
        """
        coro = self.execute_query(
            query,
            params=params,
            tenant_id=tenant_id,
            use_cache=use_cache,
            cache_ttl_seconds=cache_ttl_seconds,
            force_refresh=force_refresh,
        )
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None

        if running and running.is_running():
            result_box: Dict[str, Any] = {}
            error_box: Dict[str, BaseException] = {}

            def _runner():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    result_box["result"] = loop.run_until_complete(coro)
                except BaseException as exc:  # propagate after join
                    error_box["error"] = exc
                finally:
                    try:
                        loop.stop()
                    finally:
                        loop.close()
                        asyncio.set_event_loop(None)

            t = threading.Thread(target=_runner, daemon=True)
            t.start()
            t.join()
            if error_box:
                raise error_box["error"]
            return result_box.get("result", [])

        # No running loop in this thread; create a short-lived loop
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            try:
                loop.stop()
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    async def _execute_query_with_timeout(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Execute query with timeout controls."""

        async with self._get_connection() as conn:
            def _execute():
                with conn.cursor() as cur:
                    # Prefix query with trace context for observability in Trino
                    try:
                        from ..telemetry.context import get_request_id
                        rid = get_request_id() or "unknown"
                    except Exception:
                        rid = "unknown"
                    annotated_query = f"/* aurum tenant={tenant_id or 'unknown'} rid={rid} */\n{query}"
                    cur.execute(annotated_query, params or {})
                    columns = [col[0] for col in cur.description or []]
                    results = []
                    for row in cur.fetchall():
                        result_row = {}
                        for col, value in zip(columns, row):
                            result_row[col] = value
                        results.append(result_row)
                    stats = self._normalize_stats(getattr(cur, "stats", None), query, len(results))
                    return results, stats

            loop = asyncio.get_event_loop()
            results, stats = await asyncio.wait_for(
                loop.run_in_executor(self._executor, _execute),
                timeout=self.timeout_config.query_timeout
            )

        if stats:
            self._log_query_metrics(stats, tenant_id)

        return results, stats

    async def _acquire_slot(self) -> None:
        """Reserve a concurrency slot, applying queue backpressure when saturated."""
        queue_full = False
        async with self._queue_lock:
            if self._queue_max and self._queue_waiters >= self._queue_max:
                queue_full = True
            else:
                self._queue_waiters += 1

        await self._update_queue_metrics()

        if queue_full:
            await increment_trino_queue_rejections("queue_full")
            raise ServiceUnavailableException(
                "trino",
                "Trino request queue is saturated",
            )

        try:
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self._queue_timeout)
        except asyncio.TimeoutError as exc:
            async with self._queue_lock:
                self._queue_waiters = max(0, self._queue_waiters - 1)
            await increment_trino_queue_rejections("queue_timeout")
            await self._update_queue_metrics()
            raise ServiceUnavailableException(
                "trino",
                "Timed out waiting for available Trino worker",
            ) from exc

        async with self._queue_lock:
            self._queue_waiters = max(0, self._queue_waiters - 1)
        async with self._active_lock:
            self._active_queries += 1

        await self._update_queue_metrics()

    async def _release_slot(self) -> None:
        """Release a previously acquired concurrency slot."""
        self._semaphore.release()
        async with self._active_lock:
            self._active_queries = max(0, self._active_queries - 1)
        await self._update_queue_metrics()

    async def _update_queue_metrics(self) -> None:
        """Update queue depth and saturation gauges."""
        async with self._active_lock:
            active = self._active_queries
        async with self._queue_lock:
            queue_depth = self._queue_waiters

        utilization = active / self._max_concurrent if self._max_concurrent else 0.0

        await set_trino_request_queue_depth(queue_depth)
        await set_trino_pool_saturation(utilization)

        status = "healthy"
        if utilization >= self._saturation_critical or (
            self._queue_max and queue_depth >= self._queue_max
        ):
            status = "critical"
        elif utilization >= self._saturation_warning or (
            self._queue_max and queue_depth >= max(1, int(self._queue_max * 0.5))
        ):
            status = "warning"

        await set_trino_pool_saturation_status(status)

    async def _update_pool_metrics(self) -> None:
        """Push connection pool utilisation metrics."""
        try:
            stats = await self.connection_pool.get_stats()
        except Exception:  # pragma: no cover - defensive
            return

        await set_trino_connection_pool_active(stats["active_connections"])
        await set_trino_connection_pool_idle(stats["idle_connections"])
        await set_trino_connection_pool_utilization(stats["pool_utilization"])

    async def _execute_with_cache(
        self,
        query: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        cache_ttl_seconds: Optional[int] = None,
        force_refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        try:
            from .golden_query_cache import get_golden_query_cache
        except RuntimeError:
            # Cache not initialised; execute normally
            results, stats = await self._execute_query_with_timeout(query, params, tenant_id)
            await self._record_query_observability(query, stats, len(results), tenant_id, cached=False)
            return results

        cache = get_golden_query_cache()
        cache_state = {"hit": True}
        stats_holder: Dict[str, Any] = {}

        async def compute() -> List[Dict[str, Any]]:
            cache_state["hit"] = False
            results, stats = await self._execute_query_with_timeout(query, params, tenant_id)
            stats_holder["stats"] = stats
            await self._record_query_observability(query, stats, len(results), tenant_id, cached=False)
            return results

        result = await cache.get_or_compute(
            query=query,
            compute_func=compute,
            ttl_seconds=cache_ttl_seconds,
            force_refresh=force_refresh,
        )

        if cache_state["hit"]:
            await self._record_query_observability(
                query,
                stats_holder.get("stats"),
                len(result) if isinstance(result, list) else 0,
                tenant_id,
                cached=True,
            )

        return result

    async def _record_query_observability(
        self,
        query: str,
        stats: Optional[Dict[str, Any]],
        result_count: int,
        tenant_id: Optional[str],
        *,
        cached: bool,
    ) -> None:
        fingerprint = self._fingerprint_query(query)
        wall_time_ms = None
        if stats:
            wall_time_ms = stats.get("wall_time_ms")
        execution_seconds = None
        if wall_time_ms is not None:
            try:
                execution_seconds = float(wall_time_ms) / 1000.0
            except (TypeError, ValueError):
                execution_seconds = None

        payload = {
            "query_fingerprint": fingerprint,
            "cached": cached,
            "result_count": result_count,
        }
        if execution_seconds is not None:
            payload["execution_seconds"] = execution_seconds
        if tenant_id:
            payload["tenant_id"] = tenant_id

        log_structured(
            "info" if not cached else "debug",
            "trino_query_profile",
            **payload,
        )

        try:
            from .performance import get_performance_monitor

            monitor = get_performance_monitor()
            await monitor.record_query(
                fingerprint,
                execution_time=execution_seconds or 0.0,
                result_count=result_count,
                cached=cached,
                metadata={
                    "tenant_id": tenant_id,
                },
            )
        except Exception:
            # Observability should not interfere with query results
            pass

    async def stream_query(
        self,
        query: str,
        params: Optional[Dict[str, Any]] = None,
        tenant_id: Optional[str] = None,
        *,
        chunk_size: int = 1000,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Stream a Trino query in chunks of dictionaries."""

        tenant_id = tenant_id or get_tenant_id()
        conn_context = self._get_connection()
        loop = asyncio.get_event_loop()
        cursor_holder: Dict[str, Any] = {}
        columns_holder: Dict[str, List[str]] = {}
        row_count = 0
        start_time = time.perf_counter()
        acquired_slot = False

        try:
            await self._acquire_slot()
            acquired_slot = True
            conn = await conn_context.__aenter__()

            def _prepare_cursor() -> None:
                cursor = conn.cursor()
                cursor.execute(query, params or {})
                columns = [col[0] for col in cursor.description or []]
                cursor_holder["cursor"] = cursor
                columns_holder["columns"] = columns

            await loop.run_in_executor(self._executor, _prepare_cursor)

            while True:
                cursor = cursor_holder["cursor"]
                rows = await loop.run_in_executor(
                    self._executor,
                    cursor.fetchmany,
                    chunk_size,
                )
                if not rows:
                    break

                columns = columns_holder["columns"]
                chunk = [dict(zip(columns, row)) for row in rows]
                row_count += len(chunk)
                yield chunk
        finally:
            cursor = cursor_holder.get("cursor")
            if cursor is not None:
                await loop.run_in_executor(self._executor, cursor.close)
            await conn_context.__aexit__(None, None, None)
            if acquired_slot:
                await self._release_slot()

        duration = time.perf_counter() - start_time
        pseudo_stats = {
            "wall_time_ms": duration * 1000,
            "result_row_count": row_count,
        }
        await self._record_query_observability(
            query,
            pseudo_stats,
            row_count,
            tenant_id,
            cached=False,
        )

    @asynccontextmanager
    async def _get_connection(self):
        """Get a connection from the pool."""
        start = time.perf_counter()
        conn = await self.connection_pool.get_connection()
        await observe_trino_connection_acquire_time(time.perf_counter() - start)
        await self._update_pool_metrics()
        try:
            yield conn
        finally:
            await self.connection_pool.return_connection(conn)
            await self._update_pool_metrics()

    def _normalize_stats(
        self,
        raw_stats: Optional[Dict[str, Any]],
        query: str,
        result_row_count: int,
    ) -> Dict[str, Any]:
        if not raw_stats:
            return {
                "query_fingerprint": self._fingerprint_query(query),
                "result_row_count": result_row_count,
            }

        stats = raw_stats
        query_id = None
        if isinstance(raw_stats, dict):
            raw_response = raw_stats.get("rawResponse")
            if isinstance(raw_response, dict):
                query_id = raw_response.get("id") or raw_response.get("queryId")
                stats = raw_response.get("stats", raw_stats)
            else:
                query_id = raw_stats.get("queryId") or raw_stats.get("id")

        def _as_float(value: Any) -> Optional[float]:
            if value is None:
                return None
            try:
                number = float(value)
                if math.isnan(number):
                    return None
                return number
            except (TypeError, ValueError):
                return None

        processed_bytes = _as_float(stats.get("processedBytes"))
        scan_input_bytes = _as_float(stats.get("scanInputBytes"))
        cache_hit_rate = _as_float(
            stats.get("columnarCacheHitRate")
            or stats.get("cacheHitRate")
            or stats.get("resultCacheHitRate")
        )

        if cache_hit_rate is None and scan_input_bytes and processed_bytes is not None and scan_input_bytes > 0:
            cached_bytes = max(scan_input_bytes - processed_bytes, 0.0)
            cache_hit_rate = max(0.0, min(1.0, cached_bytes / scan_input_bytes))

        normalized = {
            "query_id": query_id,
            "query_fingerprint": self._fingerprint_query(query),
            "wall_time_ms": _as_float(stats.get("wallTimeMillis")),
            "cpu_time_ms": _as_float(stats.get("cpuTimeMillis")),
            "processed_bytes": processed_bytes,
            "scan_input_bytes": scan_input_bytes,
            "rows_processed": _as_float(stats.get("processedRows")),
            "peak_memory_bytes": _as_float(stats.get("peakMemoryBytes")),
            "cache_hit_rate": cache_hit_rate,
            "result_row_count": result_row_count,
        }

        return {key: value for key, value in normalized.items() if value is not None}

    def _log_query_metrics(self, stats: Dict[str, Any], tenant_id: Optional[str]) -> None:
        payload = {
            key: value
            for key, value in stats.items()
            if value is not None and (not isinstance(value, float) or math.isfinite(value))
        }
        payload.update(
            {
                "catalog": self.config.catalog,
                "schema": self.config.database_schema,
            }
        )

        log_structured(
            "info",
            "trino_query_metrics",
            tenant_id=tenant_id,
            **payload,
        )

    @staticmethod
    def _fingerprint_query(query: str) -> str:
        digest = hashlib.sha1(query.encode("utf-8")).hexdigest()
        return digest[:16]

    @staticmethod
    def _classify_query_type(query: str) -> str:
        """Classify query type for metrics."""
        query_upper = query.strip().upper()
        if query_upper.startswith("SELECT"):
            return "select"
        elif query_upper.startswith("INSERT"):
            return "insert"
        elif query_upper.startswith("UPDATE"):
            return "update"
        elif query_upper.startswith("DELETE"):
            return "delete"
        elif query_upper.startswith("CREATE"):
            return "create"
        elif query_upper.startswith("DROP"):
            return "drop"
        elif query_upper.startswith("ALTER"):
            return "alter"
        elif query_upper.startswith("SHOW"):
            return "show"
        elif query_upper.startswith("DESCRIBE"):
            return "describe"
        else:
            return "other"

    @staticmethod
    def _duration_from_stats(stats: Dict[str, Any]) -> float:
        """Extract execution duration in seconds from Trino stats payload."""
        wall_ms = stats.get("wall_time_ms") or stats.get("wallTimeMillis")
        if wall_ms is not None:
            try:
                return float(wall_ms) / 1000.0
            except (TypeError, ValueError):  # pragma: no cover - defensive
                pass
        try:
            return float(stats.get("execution_time", 0.0))
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return 0.0

    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the Trino client."""
        await set_trino_circuit_breaker_state(self.circuit_breaker.get_state().value)
        await self._update_queue_metrics()
        await self._update_pool_metrics()

        pool_metrics = await self.get_pool_metrics()

        async with self._active_lock:
            active_queries = self._active_queries

        status = {
            "circuit_breaker_state": self.circuit_breaker.get_state().value,
            "uptime_seconds": time.time() - self._start_time,
            "total_queries": getattr(self, "_total_queries", 0),
            "total_errors": getattr(self, "_total_errors", 0),
            "active_queries": active_queries,
            **pool_metrics,
        }

        try:
            await self.execute_query("SELECT 1 as health_check", tenant_id="health-check")
            status["healthy"] = True
        except Exception as exc:  # pragma: no cover - health endpoint defensive
            status["healthy"] = False
            status["last_error"] = str(exc)

        return status

    async def get_pool_metrics(self) -> Dict[str, Any]:
        """Get comprehensive connection pool metrics."""
        stats = await self.connection_pool.get_stats()
        async with self._queue_lock:
            queue_depth = self._queue_waiters

        metrics = dict(stats)
        metrics.update(
            {
                "request_queue_depth": queue_depth,
                "queue_capacity": self._queue_max,
                "queue_utilization": (queue_depth / self._queue_max) if self._queue_max else 0.0,
                "max_concurrent_queries": self._max_concurrent,
            }
        )
        return metrics

    async def get_query_stats(self) -> Dict[str, Any]:
        """Get query performance statistics."""
        # These would typically come from Prometheus metrics or internal counters
        # For now, return mock data based on recent activity
        return {
            "running_queries": getattr(self, "_running_queries", 0),
            "queued_queries": getattr(self, "_queued_queries", 0),
            "failed_queries": getattr(self, "_failed_queries", 0),
            "total_queries": getattr(self, "_total_queries", 0),
            "avg_query_time_seconds": getattr(self, "_avg_query_time", 0.5),
            "p95_query_time_seconds": getattr(self, "_p95_query_time", 1.0),
            "p99_query_time_seconds": getattr(self, "_p99_query_time", 2.0),
        }

    async def get_connection_creation_rate(self) -> float:
        """Get rate of connection creation per second."""
        return getattr(self, "_connection_creation_rate", 0.0)

    async def get_connection_destroy_rate(self) -> float:
        """Get rate of connection destruction per second."""
        return getattr(self, "_connection_destroy_rate", 0.0)

    async def get_pool_efficiency(self) -> float:
        """Get pool efficiency score (0-1)."""
        return getattr(self, "_pool_efficiency", 0.8)

    async def reset_circuit_breaker(self) -> None:
        """Reset the circuit breaker to recover from failures."""
        self.circuit_breaker.reset()

    def set_lineage_tags(self, tags: List[str]) -> None:
        """Set lineage tags for this client."""
        self._lineage_tags = tags

    def get_lineage_tags(self) -> List[str]:
        """Get lineage tags for this client."""
        return self._lineage_tags.copy()

    async def validate_access(self, operation: str = "read") -> None:
        """Validate access permissions for the current catalog."""
        # In a real implementation, this would check ACLs
        # For now, we'll log the operation for lineage
        if hasattr(self, "_lineage_tags") and self._lineage_tags:
            log_structured(
                "info",
                "trino_access_validation",
                catalog=self.config.catalog,
                operation=operation,
                lineage_tags=self._lineage_tags,
                user=self.config.user,
            )

    async def close(self) -> None:
        """Close the client and clean up resources."""
        await self.connection_pool.close()
        self._executor.shutdown(wait=True)


class TrinoClientManager:
    """Singleton manager for Trino clients with multi-catalog support."""

    _instance: Optional[TrinoClientManager] = None
    _lock = threading.Lock()

    def __init__(self):
        self._clients: Dict[str, TrinoClient] = {}
        self._catalog_configs: Dict[str, TrinoCatalogConfig] = {}
        self._default_client: Optional[TrinoClient] = None
        self._default_config: Optional[TrinoConfig] = None

    @classmethod
    def get_instance(cls) -> TrinoClientManager:
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def configure(self, config: TrinoConfig) -> None:
        """Configure the Trino client manager with legacy single catalog."""
        self._default_config = config
        self._clients = {}
        self._catalog_configs = {}
        self._default_client = None

    def configure_catalogs(self, catalog_configs: List[TrinoCatalogConfig]) -> None:
        """Configure multiple Trino catalogs with different access levels."""
        self._catalog_configs = {config.catalog: config for config in catalog_configs}
        self._clients = {}
        self._default_client = None

    def get_client(
        self,
        config: TrinoConfig,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ) -> TrinoClient:
        """Get or create a Trino client (legacy method)."""
        serialized_cfg = json.dumps(concurrency_config or {}, sort_keys=True)
        cfg_hash = hashlib.sha1(serialized_cfg.encode("utf-8")).hexdigest()[:8]
        key = f"{config.host}:{config.port}:{config.user}:{cfg_hash}"

        if key not in self._clients:
            self._clients[key] = TrinoClient(config, concurrency_config)
            if self._default_client is None:
                self._default_client = self._clients[key]

        return self._clients[key]

    def get_client_by_catalog(self, catalog_type: str) -> TrinoClient:
        """Get a Trino client for the specified catalog type."""
        if catalog_type not in self._clients:
            if catalog_type == "default" and self._default_config is not None:
                # Backward compatibility with single catalog
                self._clients[catalog_type] = TrinoClient(self._default_config)
            elif catalog_type in self._catalog_configs:
                # Use multi-catalog configuration
                config = self._catalog_configs[catalog_type]
                client = TrinoClient.from_catalog_config(config)
                self._clients[catalog_type] = client
                if self._default_client is None:
                    self._default_client = client
            else:
                raise ValueError(f"Unknown catalog type: {catalog_type}")

        return self._clients[catalog_type]

    def get_raw_catalog_client(self) -> TrinoClient:
        """Get client for raw data catalog (read-only)."""
        return self.get_client_by_catalog(TrinoCatalogType.RAW.value)

    def get_market_catalog_client(self) -> TrinoClient:
        """Get client for market data catalog (read-write)."""
        return self.get_client_by_catalog(TrinoCatalogType.MARKET.value)

    def get_catalog_config(self, catalog_type: str) -> TrinoCatalogConfig:
        """Get configuration for a specific catalog."""
        if catalog_type in self._catalog_configs:
            return self._catalog_configs[catalog_type]
        elif catalog_type == "default" and self._default_config is not None:
            # Create a default catalog config for backward compatibility
            return TrinoCatalogConfig(
                host=self._default_config.host,
                port=self._default_config.port,
                user=self._default_config.user,
                http_scheme=self._default_config.http_scheme,
                catalog=self._default_config.catalog,
                schema=self._default_config.schema,
                access_level=TrinoAccessLevel.READ_WRITE,
                password=self._default_config.password,
            )
        else:
            raise ValueError(f"Unknown catalog type: {catalog_type}")

    def get_default_client(self) -> Optional[TrinoClient]:
        """Get the default Trino client."""
        return self._default_client

    async def close_all(self) -> None:
        """Close all clients."""
        for client in self._clients.values():
            await client.close()
        self._clients.clear()
        self._default_client = None


# Global client manager instance
_client_manager = TrinoClientManager.get_instance()


def get_trino_client(
    config: Optional[TrinoConfig] = None,
    concurrency_config: Optional[Dict[str, Any]] = None,
) -> TrinoClient:
    """Get a Trino client instance."""
    settings = None

    if config is None or concurrency_config is None:
        from .state import get_settings

        settings = get_settings()

    if config is None:
        assert settings is not None
        config = TrinoConfig(
            host=settings.trino.host,
            port=settings.trino.port,
            user=settings.trino.user,
            catalog=settings.trino.catalog,
            schema=settings.trino.database_schema,
            http_scheme=settings.trino.http_scheme,
        )

    if concurrency_config is None:
        assert settings is not None
        concurrency_config = settings.api.concurrency.model_dump()
    else:
        if settings is None:
            from .state import get_settings

            settings = get_settings()
        merged_config = settings.api.concurrency.model_dump()
        merged_config.update(concurrency_config)
        concurrency_config = merged_config

    return _client_manager.get_client(config, concurrency_config)


async def get_default_trino_client() -> Optional[TrinoClient]:
    """Get the default Trino client."""
    return _client_manager.get_default_client()


def get_trino_client_by_catalog(catalog_type: str) -> TrinoClient:
    """Get a Trino client for the specified catalog type."""
    return _client_manager.get_client_by_catalog(catalog_type)


def get_trino_raw_catalog_client() -> TrinoClient:
    """Get client for raw data catalog (read-only)."""
    return _client_manager.get_raw_catalog_client()


def get_trino_market_catalog_client() -> TrinoClient:
    """Get client for market data catalog (read-write)."""
    return _client_manager.get_market_catalog_client()


def get_trino_catalog_config(catalog_type: str) -> TrinoCatalogConfig:
    """Get configuration for a specific catalog."""
    return _client_manager.get_catalog_config(catalog_type)


def configure_trino_catalogs(catalog_configs: List[TrinoCatalogConfig]) -> None:
    """Configure multiple Trino catalogs with different access levels."""
    _client_manager.configure_catalogs(catalog_configs)
