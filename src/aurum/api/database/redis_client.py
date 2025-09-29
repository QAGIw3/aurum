from __future__ import annotations

"""Centralized Redis client manager with pooling, pre-warm, and cleanup.

Provides a small manager keyed by Redis URL that returns a shared
`redis.asyncio.Redis` client using a connection pool. The manager also exposes
`initialize()` for pre-warming and `close_all()` for graceful shutdown.
"""

import asyncio
import logging
from typing import Dict, Optional


LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - optional dependency
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover - allow import in docs/tests
    redis = None  # type: ignore[assignment]


class RedisClientManager:
    """Manages shared Redis clients keyed by URL with pooling and cleanup."""

    _instance: Optional["RedisClientManager"] = None
    _lock = asyncio.Lock()

    def __init__(self) -> None:
        self._clients: Dict[str, "redis.Redis"] = {}
        self._clients_lock = asyncio.Lock()

    @classmethod
    def get_instance(cls) -> "RedisClientManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    async def initialize(self, url: Optional[str]) -> None:
        """Initialize and pre-warm client for the provided URL."""
        if not url or redis is None:
            return
        client = await self.get_client(url)
        try:
            await client.ping()
        except Exception:
            # Best-effort prewarm
            LOGGER.debug("redis_prewarm_failed", exc_info=True)

    async def get_client(self, url: str, *, decode_responses: bool = True) -> "redis.Redis":
        """Get or create a shared Redis client for the given URL."""
        if redis is None:
            raise RuntimeError("redis.asyncio is not installed; install redis>=4.2")
        if not url:
            raise ValueError("Redis URL must be provided")

        async with self._clients_lock:
            client = self._clients.get(url)
            if client is not None:
                return client

            # Create client with connection pool; max_connections can be tuned via URL params
            client = redis.from_url(url, decode_responses=decode_responses)
            self._clients[url] = client
            # Try to expose basic pool metrics if available
            try:
                pool = getattr(client, "connection_pool", None)
                active = getattr(pool, "_in_use_connections", None)
                total = getattr(pool, "_created_connections", None)
                # Metrics will be updated by callers that know the workload
                _ = (active, total)
            except Exception:
                pass
            return client

    async def close_all(self) -> None:
        """Close all managed Redis clients and their pools."""
        async with self._clients_lock:
            clients = list(self._clients.values())
            self._clients.clear()

        async def _close(c: "redis.Redis") -> None:
            try:
                # Prefer aclose() when available
                close = getattr(c, "aclose", None) or getattr(c, "close", None)
                if close is None:
                    # Disconnect underlying pool if exposed
                    pool = getattr(c, "connection_pool", None)
                    if pool is not None and hasattr(pool, "disconnect"):
                        await pool.disconnect()  # type: ignore[func-returns-value]
                    return
                res = close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                LOGGER.debug("redis_close_failed", exc_info=True)

        if clients:
            await asyncio.gather(*(_close(c) for c in clients), return_exceptions=True)


def get_redis_manager() -> RedisClientManager:
    return RedisClientManager.get_instance()


__all__ = [
    "RedisClientManager",
    "get_redis_manager",
]


