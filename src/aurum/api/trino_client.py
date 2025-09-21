"""Enhanced Trino client with concurrency controls, timeouts, and resilience patterns."""

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
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor

try:
    import trino
    import trino.dbapi
except ImportError:
    trino = None
    trino.dbapi = None

from ..telemetry.context import log_structured
from .config import TrinoConfig
from .exceptions import ServiceUnavailableException

LOGGER = logging.getLogger(__name__)


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, rejecting requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreaker:
    """Circuit breaker for Trino connections."""
    failure_threshold: int = 5
    timeout_seconds: float = 60.0
    _failures: int = 0
    _last_failure_time: Optional[float] = None
    _state: CircuitBreakerState = CircuitBreakerState.CLOSED
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt a reset."""
        if self._last_failure_time is None:
            return False
        return time.time() - self._last_failure_time >= self.timeout_seconds

    def record_success(self) -> None:
        """Record a successful operation."""
        with self._lock:
            self._failures = 0
            self._state = CircuitBreakerState.CLOSED

    def record_failure(self) -> None:
        """Record a failed operation."""
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()

            if self._failures >= self.failure_threshold:
                self._state = CircuitBreakerState.OPEN

    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        with self._lock:
            if self._state == CircuitBreakerState.OPEN:
                if self._should_attempt_reset():
                    self._state = CircuitBreakerState.HALF_OPEN
                    return False
                return True
            return False

    def get_state(self) -> CircuitBreakerState:
        """Get current circuit breaker state."""
        with self._lock:
            return self._state


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
                schema=self.config.schema,
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
    ) -> List[Dict[str, Any]]:
        """Execute a Trino query with retry logic and circuit breaker."""

        # Check circuit breaker
        if self.circuit_breaker.is_open():
            raise ServiceUnavailableException(
                "trino",
                "Circuit breaker is open - service temporarily unavailable"
            )

        last_exception = None

        for attempt in range(self.retry_config.max_retries + 1):
            try:
                # Execute the query
        result = await self._execute_query_with_timeout(query, params, tenant_id)

                # Record success
                self.circuit_breaker.record_success()

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
    ) -> List[Dict[str, Any]]:
        """Execute query with timeout controls."""

        async with self._get_connection() as conn:
            def _execute():
                with conn.cursor() as cur:
                    cur.execute(query, params or {})
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

        return results

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
                "schema": self.config.schema,
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

    async def get_health_status(self) -> Dict[str, Any]:
        """Get health status of the Trino client."""
        status = {
            "circuit_breaker_state": self.circuit_breaker.get_state().value,
            "pool_size": self.connection_pool._pool.qsize(),
            "executor_active_threads": self._executor._threads.__len__(),
        }

        # Try a simple query to test health
        try:
            await self.execute_query("SELECT 1 as health_check", tenant_id="health-check")
            status["healthy"] = True
        except Exception as exc:
            status["healthy"] = False
            status["last_error"] = str(exc)

        return status

    async def close(self) -> None:
        """Close the client and clean up resources."""
        await self.connection_pool.close()
        self._executor.shutdown(wait=True)


class TrinoClientManager:
    """Singleton manager for Trino clients."""

    _instance: Optional[TrinoClientManager] = None
    _lock = threading.Lock()

    def __init__(self):
        self._clients: Dict[str, TrinoClient] = {}
        self._default_client: Optional[TrinoClient] = None

    @classmethod
    def get_instance(cls) -> TrinoClientManager:
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def get_client(
        self,
        config: TrinoConfig,
        concurrency_config: Optional[Dict[str, Any]] = None,
    ) -> TrinoClient:
        """Get or create a Trino client."""
        key = f"{config.host}:{config.port}:{config.user}"

        if key not in self._clients:
            self._clients[key] = TrinoClient(config, concurrency_config)
            if self._default_client is None:
                self._default_client = self._clients[key]

        return self._clients[key]

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
            schema=settings.trino.schema,
            http_scheme=settings.trino.http_scheme,
        )

    return _client_manager.get_client(config, concurrency_config)


async def get_default_trino_client() -> Optional[TrinoClient]:
    """Get the default Trino client."""
    return _client_manager.get_default_client()
