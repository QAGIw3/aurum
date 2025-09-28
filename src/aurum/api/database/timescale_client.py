"""Async Timescale client with connection pooling, retries, and monitoring.

Provides a singleton client accessed via ``get_timescale_client()`` that uses
psycopg (v3) ``AsyncConnectionPool`` under the hood. Queries are fully async
and recorded to the database monitor for observability.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

try:  # pragma: no cover - optional runtime dependency
    import psycopg  # type: ignore
    from psycopg.rows import dict_row  # type: ignore
except Exception:  # pragma: no cover - allow import in docs/tests
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]

from .database_monitor import get_database_monitor, QueryMetrics
from ..exceptions import ServiceUnavailableException
from ...core.settings import get_settings


LOGGER = logging.getLogger(__name__)


@dataclass
class TimescalePoolConfig:
    min_size: int = 1
    max_size: int = 5
    acquire_timeout_seconds: float = 5.0
    query_timeout_seconds: float = 30.0
    max_retries: int = 2
    initial_backoff_seconds: float = 0.2
    max_backoff_seconds: float = 2.0


class AsyncTimescaleClient:
    """Async Timescale client with pooled connections and basic retries."""

    def __init__(self, dsn: str, pool_config: Optional[TimescalePoolConfig] = None) -> None:
        if not dsn:
            raise ValueError("Timescale DSN must be provided")
        self._dsn = dsn
        self._cfg = pool_config or TimescalePoolConfig()
        self._pool: Optional["psycopg.AsyncConnectionPool"] = None
        self._active_connections: int = 0
        self._lock = asyncio.Lock()
        self._closed = False

    async def _ensure_pool(self) -> None:
        if self._pool is not None:
            return
        if psycopg is None:
            raise RuntimeError("psycopg is required for AsyncTimescaleClient")
        async with self._lock:
            if self._pool is not None:
                return
            # Create pool lazily
            self._pool = psycopg.AsyncConnectionPool(  # type: ignore[attr-defined]
                conninfo=self._dsn,
                min_size=max(1, int(self._cfg.min_size)),
                max_size=max(1, int(self._cfg.max_size)),
                name="aurum_timescale",
                check=psycopg.AsyncConnectionPool.check_connection,  # type: ignore[attr-defined]
                timeout=float(self._cfg.acquire_timeout_seconds) or None,
            )

    async def close(self) -> None:
        if self._pool is None or self._closed:
            return
        async with self._lock:
            if self._pool is None or self._closed:
                return
            try:
                await self._pool.close()  # type: ignore[func-returns-value]
            finally:
                self._pool = None
                self._closed = True

    async def get_pool_metrics(self) -> Dict[str, Any]:
        pool = self._pool
        max_size = getattr(pool, "max_size", self._cfg.max_size) if pool else self._cfg.max_size
        active = max(0, int(self._active_connections))
        idle = max(0, int(max_size) - active)
        total = active  # psycopg doesn't expose total created; report active
        utilization = (active / float(max(max_size, 1))) if max_size else 0.0
        return {
            "active_connections": active,
            "idle_connections": idle,
            "total_connections": total,
            "max_connections": int(max_size),
            "pool_utilization": utilization,
        }

    async def execute_query(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Execute a SQL statement and return rows as list[dict]."""

        await self._ensure_pool()
        assert self._pool is not None  # for type-checkers

        attempt = 0
        backoff = float(self._cfg.initial_backoff_seconds)
        max_retries = int(max(0, self._cfg.max_retries))
        q_timeout = float(timeout or self._cfg.query_timeout_seconds)
        last_error: Optional[Exception] = None

        while attempt <= max_retries:
            attempt += 1
            start = time.perf_counter()
            try:
                self._active_connections += 1
                async with self._pool.connection() as conn:  # type: ignore[attr-defined]
                    async with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
                        if q_timeout:
                            rows = await asyncio.wait_for(cur.execute(sql, params or {}), timeout=q_timeout)
                        else:
                            rows = await cur.execute(sql, params or {})
                        # psycopg returns the cursor; fetch rows explicitly
                        fetched = await cur.fetchall()
                        duration = time.perf_counter() - start
                        await self._record_monitoring(sql, params, duration, result_count=len(fetched))
                        return [dict(row) for row in fetched]
            except asyncio.TimeoutError as exc:
                last_error = exc
                await self._record_monitoring(sql, params, float(self._cfg.query_timeout_seconds), error=str(exc))
                if attempt > max_retries:
                    raise ServiceUnavailableException("timescale", detail="Query timed out") from exc
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt > max_retries:
                    raise
            finally:
                self._active_connections = max(0, self._active_connections - 1)

            await asyncio.sleep(min(backoff, self._cfg.max_backoff_seconds))
            backoff = min(backoff * 2.0, self._cfg.max_backoff_seconds)

        if last_error:
            raise last_error
        return []

    async def execute(
        self,
        sql: str,
        params: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[float] = None,
    ) -> None:
        """Execute a statement that doesn't return rows."""

        await self._ensure_pool()
        assert self._pool is not None
        q_timeout = float(timeout or self._cfg.query_timeout_seconds)

        async with self._pool.connection() as conn:  # type: ignore[attr-defined]
            async with conn.cursor(row_factory=dict_row) as cur:  # type: ignore[arg-type]
                if q_timeout:
                    await asyncio.wait_for(cur.execute(sql, params or {}), timeout=q_timeout)
                else:
                    await cur.execute(sql, params or {})
                try:
                    await conn.commit()
                except Exception:  # noqa: BLE001 - best effort commit
                    pass

    async def _record_monitoring(
        self,
        sql: str,
        params: Optional[Dict[str, Any]],
        duration_seconds: float,
        *,
        result_count: int = 0,
        error: Optional[str] = None,
    ) -> None:
        try:
            monitor = get_database_monitor()
            metrics = QueryMetrics(
                query_hash=monitor._generate_query_hash(sql),
                query_text=sql,
                execution_time=duration_seconds,
                timestamp=datetime.utcnow(),
                endpoint="",
                parameters={"param_keys": list((params or {}).keys())},
                result_count=result_count,
                error_message=error,
                database_engine="timescale",
            )
            await monitor.record_query(metrics)
        except Exception:  # pragma: no cover - monitoring must be best-effort
            pass


_client: Optional[AsyncTimescaleClient] = None
_client_lock = asyncio.Lock()


def _resolve_dsn() -> str:
    try:
        settings = get_settings()
        return str(getattr(settings.database, "timescale_dsn", ""))
    except Exception:
        return ""


def get_timescale_client(
    dsn: Optional[str] = None,
    pool_config: Optional[TimescalePoolConfig] = None,
) -> AsyncTimescaleClient:
    """Return a singleton AsyncTimescaleClient instance."""
    global _client
    if _client is not None:
        return _client
    resolved_dsn = (dsn or _resolve_dsn()).strip()
    _client = AsyncTimescaleClient(resolved_dsn, pool_config=pool_config)
    return _client


__all__ = [
    "AsyncTimescaleClient",
    "TimescalePoolConfig",
    "get_timescale_client",
]


