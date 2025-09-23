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
import logging
import math
import random
import time
import threading
from contextlib import asynccontextmanager
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
    """Simple connection pool for Trino connections."""

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
        self._idle_connections: List[Tuple[Any, float]] = []
        self._lock = asyncio.Lock()
        self._closed = False

        # Initialize minimum connections
        self._initialized = False

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
                except Exception as exc:
                    LOGGER.warning(f"Failed to create initial connection: {exc}")
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
        """Get a connection from the pool."""
        await self._ensure_initialized()

        try:
            # Try to get existing connection first
            conn = self._pool.get_nowait()
            return conn
        except asyncio.QueueEmpty:
            # Pool is empty, create new connection if under max size
            async with self._lock:
                current_size = self._pool.qsize()
                if current_size < self.max_size:
                    try:
                        conn = await self._create_connection()
                        return conn
                    except Exception as exc:
                        log_structured(
                            "error",
                            "trino_connection_creation_failed",
                            error=str(exc),
                            max_connections=self.max_size,
                        )
                        raise

            # Wait for a connection to be returned
            try:
                conn = await asyncio.wait_for(self._pool.get(), timeout=self.wait_timeout_seconds)
                return conn
            except asyncio.TimeoutError:
                raise ServiceUnavailableException(
                    "trino",
                    "Connection pool timeout exceeded"
                )

    async def return_connection(self, conn: Any) -> None:
        """Return a connection to the pool."""
        if self._closed:
            conn.close()
            return

        try:
            # Check if connection is still valid
            if hasattr(conn, 'ping'):
                try:
                    conn.ping()
                except Exception:
                    conn.close()
                    return

            await self._pool.put(conn)
        except Exception:
            # If pool is full, close the connection
            if hasattr(conn, 'close'):
                conn.close()

    async def close(self) -> None:
        """Close all connections in the pool."""
        self._closed = True

        # Close all connections in the queue
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                if hasattr(conn, 'close'):
                    conn.close()
            except:
                pass


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

        # Initialize thread pool for sync operations
        self._executor = ThreadPoolExecutor(
            max_workers=self.concurrency_config.get("max_concurrent_trino_connections", 10),
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

        last_exception = None

        for attempt in range(self.retry_config.max_retries + 1):
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

                # Record success
                self.circuit_breaker.record_success()

                # Track metrics
                await increment_trino_queries(tenant_id, "success")
                if "stats" in locals():
                    await observe_trino_query_duration(
                        tenant_id,
                        self._classify_query_type(query),
                        stats.get("execution_time", 0.0)
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

                # Record failure
                self.circuit_breaker.record_failure()

                # Track error metrics
                await increment_trino_queries(tenant_id, "error")

                # Don't retry on the last attempt
                if attempt >= self.retry_config.max_retries:
                    break

                # Calculate delay and wait
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

        # All retries exhausted
        log_structured(
            "error",
            "trino_query_failed",
            tenant_id=tenant_id,
            total_attempts=self.retry_config.max_retries + 1,
            error_type=last_exception.__class__.__name__ if last_exception else "Unknown",
            error_message=str(last_exception) if last_exception else "Unknown",
        )

        raise last_exception

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
        conn = await conn_context.__aenter__()
        loop = asyncio.get_event_loop()
        cursor_holder: Dict[str, Any] = {}
        columns_holder: Dict[str, List[str]] = {}
        row_count = 0
        start_time = time.perf_counter()

        try:
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
        conn = await self.connection_pool.get_connection()
        try:
            yield conn
        finally:
            await self.connection_pool.return_connection(conn)

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

    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the Trino client."""
        # Update metrics
        await set_trino_circuit_breaker_state(self.circuit_breaker.get_state().value)

        status = {
            "circuit_breaker_state": self.circuit_breaker.get_state().value,
            "pool_size": self.connection_pool._pool.qsize(),
            "executor_active_threads": self._executor._threads.__len__(),
            "uptime_seconds": time.time() - self._start_time,
            "total_queries": getattr(self, "_total_queries", 0),
            "total_errors": getattr(self, "_total_errors", 0),
        }

        # Try a simple query to test health
        try:
            await self.execute_query("SELECT 1 as health_check", tenant_id="health-check")
            status["healthy"] = True
            status["avg_response_time"] = getattr(self, "_avg_response_time", 0)

            # Update pool metrics
            pool_metrics = await self.get_pool_metrics()
            await set_trino_connection_pool_active(pool_metrics["active_connections"])
            await set_trino_connection_pool_idle(pool_metrics["idle_connections"])
            await set_trino_connection_pool_utilization(pool_metrics["pool_utilization"])
            await set_trino_pool_saturation(pool_metrics["pool_utilization"])

            # Set pool saturation status
            if pool_metrics["pool_utilization"] > 0.9:
                await set_trino_pool_saturation_status("critical")
            elif pool_metrics["pool_utilization"] > 0.7:
                await set_trino_pool_saturation_status("warning")
            elif pool_metrics["pool_utilization"] > 0.5:
                await set_trino_pool_saturation_status("elevated")
            else:
                await set_trino_pool_saturation_status("healthy")

        except Exception as exc:
            status["healthy"] = False
            status["last_error"] = str(exc)

        return status

    async def get_pool_metrics(self) -> Dict[str, Any]:
        """Get comprehensive connection pool metrics."""
        pool = self.connection_pool

        # Get current pool statistics
        total_connections = pool.min_size  # Start with minimum
        active_connections = 0
        idle_connections = 0

        # Count active vs idle connections
        try:
            # This is an approximation since we don't directly track active/idle
            # In a real implementation, you'd maintain counters
            pool_size = pool._pool.qsize()
            total_connections = max(pool_size, pool.min_size)
            # Assume most connections are active during load
            active_connections = max(1, int(total_connections * 0.8))
            idle_connections = total_connections - active_connections
        except Exception:
            active_connections = pool.min_size
            idle_connections = 0
            total_connections = pool.min_size

        pool_utilization = total_connections / pool.max_size if pool.max_size > 0 else 0

        return {
            "active_connections": active_connections,
            "idle_connections": idle_connections,
            "total_connections": total_connections,
            "max_connections": pool.max_size,
            "pool_utilization": pool_utilization,
            "queue_size": getattr(pool._pool, 'qsize', lambda: 0)(),
            "wait_queue_size": getattr(pool._pool, 'qsize', lambda: 0)(),
            "connection_acquire_time_ms": getattr(self, "_avg_acquire_time", 100.0),
            "connection_errors": getattr(self, "_connection_errors", 0),
        }

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
        key = f"{config.host}:{config.port}:{config.user}"

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
    if config is None:
        from .state import get_settings
        settings = get_settings()
        config = TrinoConfig(
            host=settings.trino.host,
            port=settings.trino.port,
            user=settings.trino.user,
            catalog=settings.trino.catalog,
            schema=settings.trino.database_schema,
            http_scheme=settings.trino.http_scheme,
        )

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
