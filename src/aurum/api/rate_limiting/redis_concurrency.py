"""Redis-backed helpers for distributed API concurrency controls.

The in-process ``ConcurrencyController`` provides rich fairness semantics for
single-instance deployments. This module introduces a lightweight Redis-backed
coordinator that shares global counters and queue ordering across multiple API
replicas while preserving the existing middleware contract.

Design goals:

* Deterministic FIFO ordering across replicas via a Redis sorted set queue.
* Atomic slot acquisition/release implemented in Lua to avoid race conditions.
* Minimal per-request state persisted in Redis (active counters + queue maps).
* Graceful degradation when Redis is unavailable – callers can fall back to the
  in-memory controller by not instantiating this backend.
* Metrics hooks mirroring the in-process controller so existing observers and
  Prometheus gauges continue to work.

The backend keeps the queue metadata small by lazily expiring stale entries on
slot acquisition attempts. Callers are expected to enforce their own timeout at
call-site and invoke ``cancel`` to evict abandoned queue entries.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Request

try:  # pragma: no cover - optional dependency
    import redis.asyncio as redis  # type: ignore
except ImportError:  # pragma: no cover
    redis = None  # type: ignore

MetricCallback = Callable[..., None]


@dataclass
class RedisConcurrencyConfig:
    """Configuration knobs for Redis-backed concurrency coordination."""

    url: str
    namespace: str = "aurum:api:concurrency"
    poll_interval_seconds: float = 0.05
    queue_stale_after_seconds: float = 5.0
    queue_ttl_seconds: float = 120.0

    def validate(self) -> None:
        """Validate configuration values to guard against misconfiguration."""

        if not self.url:
            raise ValueError("Redis URL must be provided for distributed concurrency")
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.queue_stale_after_seconds <= 0:
            raise ValueError("queue_stale_after_seconds must be positive")
        if self.queue_ttl_seconds <= 0:
            raise ValueError("queue_ttl_seconds must be positive")


class RedisUnavailable(RuntimeError):
    """Raised when redis-based coordination is requested but redis is missing."""


class RedisConcurrencyBackend:
    """Coordinate concurrency slots across replicas using Redis primitives."""

    ACTIVE_KEY_SUFFIX = "active"
    TENANT_ACTIVE_SUFFIX = "tenant_active"
    QUEUE_ZSET_SUFFIX = "queue"
    REQUEST_OWNER_SUFFIX = "request_owner"
    TENANT_QUEUE_SUFFIX = "tenant_queue"

    def __init__(
        self,
        redis_client: "redis.Redis",
        *,
        config: RedisConcurrencyConfig,
    ) -> None:
        if redis is None:  # pragma: no cover - guarded at import time
            raise RedisUnavailable(
                "redis.asyncio is not installed; install redis>=4.2 to enable distributed concurrency"
            )

        self._redis = redis_client
        self._config = config
        self._namespace = config.namespace.rstrip(":")
        self._poll_interval = max(0.01, float(config.poll_interval_seconds))
        self._queue_stale_ms = int(max(1.0, config.queue_stale_after_seconds) * 1000)
        self._queue_ttl_ms = int(max(config.queue_ttl_seconds, config.queue_stale_after_seconds) * 1000)
        self._script_lock = asyncio.Lock()
        self._acquire_sha: Optional[str] = None
        self._release_sha: Optional[str] = None
        self._cancel_sha: Optional[str] = None
        self._stats_sha: Optional[str] = None

        # Metric observers (set by controller)
        self._queue_observer: Optional[MetricCallback] = None
        self._active_observer: Optional[MetricCallback] = None
        self._tenant_active_observer: Optional[MetricCallback] = None
        self._global_queue_observer: Optional[MetricCallback] = None

    # ------------------------------------------------------------------
    # Public observer wiring
    # ------------------------------------------------------------------
    def register_observers(
        self,
        *,
        queue_depth: Optional[MetricCallback] = None,
        active: Optional[MetricCallback] = None,
        tenant_active: Optional[MetricCallback] = None,
        global_queue: Optional[MetricCallback] = None,
    ) -> None:
        """Register callbacks mirroring the in-process controller observers."""

        self._queue_observer = queue_depth
        self._active_observer = active
        self._tenant_active_observer = tenant_active
        self._global_queue_observer = global_queue

    async def ping(self) -> bool:
        """Check if the Redis connection is healthy."""

        try:
            result = await self._redis.ping()  # type: ignore[call-arg]
        except Exception:
            return False
        return bool(result)

    # ------------------------------------------------------------------
    # High-level coordination API
    # ------------------------------------------------------------------
    async def acquire_slot(
        self,
        *,
        tenant_key: str,
        tenant_id: Optional[str],
        request_id: Optional[str],
        max_global: int,
        tenant_limit: int,
        tenant_queue_limit: int,
        queue_timeout_seconds: float,
    ) -> Dict[str, Any]:
        """Try to acquire a concurrency slot, blocking until granted or timeout.

        Returns a payload describing the allocation when successful.
        Raises ``TimeoutError`` or ``OverflowError`` when the queue limit is hit.
        """

        token = f"{tenant_key}:{uuid.uuid4().hex}"
        queued_perf = time.perf_counter()
        deadline = queued_perf + max(0.0, queue_timeout_seconds)

        while True:
            now_ms = int(time.time() * 1000)
            result = await self._eval_acquire(
                tenant_key=tenant_key,
                token=token,
                max_global=max_global,
                tenant_limit=tenant_limit,
                tenant_queue_limit=tenant_queue_limit,
                now_ms=now_ms,
            )

            status = int(result[0])
            if status == 1:  # granted
                active_total = int(result[1])
                tenant_active = int(result[2])
                tenant_queue_remaining = max(0, int(result[3]))
                global_queue_depth = max(0, int(result[4]))
                self._emit_active(active_total)
                self._emit_tenant_active(tenant_key, tenant_active)
                self._emit_queue_depth(tenant_key, tenant_queue_remaining)
                self._emit_global_queue(global_queue_depth)
                granted_perf = time.perf_counter()
                return {
                    "token": token,
                    "acquire_perf": granted_perf,
                    "queued_perf": queued_perf,
                    "tenant_queue_depth": tenant_queue_remaining,
                    "global_queue_depth": global_queue_depth,
                    "active_total": active_total,
                    "tenant_active": tenant_active,
                }

            if status == 2:  # queue full
                queue_depth = int(result[1]) if len(result) > 1 else tenant_queue_limit
                raise OverflowError(queue_depth)

            # Still waiting — update observers with the most recent queue depth
            position = int(result[1]) if len(result) > 1 else 1
            global_queue_depth = int(result[2]) if len(result) > 2 else 0
            tenant_queue_depth = int(result[3]) if len(result) > 3 else position
            self._emit_queue_depth(tenant_key, tenant_queue_depth)
            self._emit_global_queue(global_queue_depth)

            if time.perf_counter() >= deadline:
                await self.cancel(tenant_key=tenant_key, token=token)
                raise TimeoutError(tenant_queue_depth)

            await asyncio.sleep(self._poll_interval)

    async def release(self, *, tenant_key: str) -> Dict[str, int]:
        """Release a previously acquired slot."""
        result = await self._eval_release(tenant_key=tenant_key)
        active_total = int(result[0]) if result else 0
        tenant_active = int(result[1]) if len(result) > 1 else 0
        global_queue = int(result[2]) if len(result) > 2 else 0
        self._emit_active(active_total)
        self._emit_tenant_active(tenant_key, tenant_active)
        self._emit_global_queue(global_queue)
        return {
            "active_total": active_total,
            "tenant_active": tenant_active,
            "global_queue_depth": global_queue,
        }

    async def cancel(self, *, tenant_key: str, token: str) -> Dict[str, int]:
        """Remove a queued request after timeout/cancellation."""
        result = await self._eval_cancel(tenant_key=tenant_key, token=token)
        tenant_queue = int(result[0]) if result else 0
        global_queue = int(result[1]) if len(result) > 1 else 0
        self._emit_queue_depth(tenant_key, tenant_queue)
        self._emit_global_queue(global_queue)
        return {
            "tenant_queue_depth": tenant_queue,
            "global_queue_depth": global_queue,
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Return a snapshot of global/tenant active counts and queues."""
        result = await self._eval_stats()
        active_total = int(result[0]) if result else 0
        global_queue = int(result[1]) if len(result) > 1 else 0
        tenant_active_flat = result[2] if len(result) > 2 else []
        tenant_queue_flat = result[3] if len(result) > 3 else []
        tenant_active: Dict[str, int] = {}
        tenant_queue: Dict[str, int] = {}

        for idx in range(0, len(tenant_active_flat), 2):
            tenant_active[tenant_active_flat[idx]] = int(tenant_active_flat[idx + 1])
        for idx in range(0, len(tenant_queue_flat), 2):
            tenant_queue[tenant_queue_flat[idx]] = int(tenant_queue_flat[idx + 1])

        return {
            "active_total": active_total,
            "global_queue_depth": global_queue,
            "tenant_active": tenant_active,
            "tenant_queue": tenant_queue,
        }


diagnostics_router = APIRouter(prefix="/v1/admin/concurrency", tags=["Concurrency"])


def _require_backend(request: Request) -> RedisConcurrencyBackend:
    backend = getattr(getattr(request.app, "state", None), "concurrency_backend", None)
    if not isinstance(backend, RedisConcurrencyBackend):
        raise HTTPException(
            status_code=503,
            detail="Distributed concurrency backend is disabled",
        )
    return backend


@diagnostics_router.get("/redis/stats", summary="Redis concurrency statistics")
async def redis_concurrency_stats(
    backend: RedisConcurrencyBackend = Depends(_require_backend),
) -> Dict[str, Any]:
    """Expose current distributed concurrency statistics."""

    return await backend.get_stats()


@diagnostics_router.get("/redis/health", summary="Redis concurrency health")
async def redis_concurrency_health(
    backend: RedisConcurrencyBackend = Depends(_require_backend),
) -> Dict[str, str]:
    """Simple readiness check for the Redis-backed concurrency coordinator."""

    healthy = await backend.ping()
    if not healthy:
        raise HTTPException(status_code=503, detail="Redis concurrency backend unavailable")
    return {"status": "ok"}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _key(self, suffix: str) -> str:
        return f"{self._namespace}:{suffix}"

    async def _ensure_scripts(self) -> None:
        async with self._script_lock:
            if self._acquire_sha is not None:
                return
            self._acquire_sha = await self._redis.script_load(_ACQUIRE_LUA)
            self._release_sha = await self._redis.script_load(_RELEASE_LUA)
            self._cancel_sha = await self._redis.script_load(_CANCEL_LUA)
            self._stats_sha = await self._redis.script_load(_STATS_LUA)

    async def _eval_acquire(
        self,
        *,
        tenant_key: str,
        token: str,
        max_global: int,
        tenant_limit: int,
        tenant_queue_limit: int,
        now_ms: int,
    ) -> Sequence[Any]:
        await self._ensure_scripts()
        keys = [
            self._key(self.ACTIVE_KEY_SUFFIX),
            self._key(self.TENANT_ACTIVE_SUFFIX),
            self._key(self.QUEUE_ZSET_SUFFIX),
            self._key(self.REQUEST_OWNER_SUFFIX),
            self._key(self.TENANT_QUEUE_SUFFIX),
        ]
        args = [
            tenant_key,
            token,
            str(max_global),
            str(max(1, tenant_limit)),
            str(now_ms),
            str(self._queue_stale_ms),
            str(max(0, tenant_queue_limit)),
            str(self._queue_ttl_ms),
        ]
        return await self._redis.evalsha(self._acquire_sha, len(keys), *keys, *args)

    async def _eval_release(self, *, tenant_key: str) -> Sequence[Any]:
        await self._ensure_scripts()
        keys = [
            self._key(self.ACTIVE_KEY_SUFFIX),
            self._key(self.TENANT_ACTIVE_SUFFIX),
            self._key(self.QUEUE_ZSET_SUFFIX),
        ]
        args = [tenant_key]
        return await self._redis.evalsha(self._release_sha, len(keys), *keys, *args)

    async def _eval_cancel(self, *, tenant_key: str, token: str) -> Sequence[Any]:
        await self._ensure_scripts()
        keys = [
            self._key(self.QUEUE_ZSET_SUFFIX),
            self._key(self.REQUEST_OWNER_SUFFIX),
            self._key(self.TENANT_QUEUE_SUFFIX),
        ]
        args = [tenant_key, token]
        return await self._redis.evalsha(self._cancel_sha, len(keys), *keys, *args)

    async def _eval_stats(self) -> Sequence[Any]:
        await self._ensure_scripts()
        keys = [
            self._key(self.ACTIVE_KEY_SUFFIX),
            self._key(self.TENANT_ACTIVE_SUFFIX),
            self._key(self.QUEUE_ZSET_SUFFIX),
            self._key(self.TENANT_QUEUE_SUFFIX),
        ]
        return await self._redis.evalsha(self._stats_sha, len(keys), *keys)

    # Observer helpers -------------------------------------------------
    def _emit_queue_depth(self, tenant_key: str, depth: int) -> None:
        if self._queue_observer:
            try:
                self._queue_observer(tenant_key, depth)
            except Exception:  # pragma: no cover - metrics must never raise
                pass

    def _emit_active(self, active_total: int) -> None:
        if self._active_observer:
            try:
                self._active_observer(active_total)
            except Exception:  # pragma: no cover - metrics best effort
                pass

    def _emit_tenant_active(self, tenant_key: str, value: int) -> None:
        if self._tenant_active_observer:
            try:
                self._tenant_active_observer(tenant_key, value)
            except Exception:  # pragma: no cover - metrics best effort
                pass

    def _emit_global_queue(self, depth: int) -> None:
        if self._global_queue_observer:
            try:
                self._global_queue_observer(depth)
            except Exception:  # pragma: no cover
                pass


_ACQUIRE_LUA = """
-- Redis Lua script for distributed concurrency acquisition.
-- KEYS:
--   1: global active counter key
--   2: tenant active hash key
--   3: pending request queue (sorted set, score = enqueue ms)
--   4: request owner hash (token -> tenant)
--   5: tenant queue depth hash (tenant -> queued count)
-- ARGV:
--   1: tenant key
--   2: request token
--   3: max global concurrent requests
--   4: max active per tenant
--   5: now (milliseconds)
--   6: stale threshold milliseconds
--   7: tenant queue limit
--   8: queue ttl milliseconds (kept for future use)
local tenant = ARGV[1]
local token = ARGV[2]
local max_global = tonumber(ARGV[3])
local max_tenant = tonumber(ARGV[4])
local now_ms = tonumber(ARGV[5])
local stale_before = now_ms - tonumber(ARGV[6])
local queue_limit = tonumber(ARGV[7])

-- Clean up stale queue entries
local stale = redis.call('zrangebyscore', KEYS[3], '-inf', stale_before)
for _, stale_token in ipairs(stale) do
    local stale_tenant = redis.call('hget', KEYS[4], stale_token)
    if stale_tenant then
        local remaining = redis.call('hincrby', KEYS[5], stale_tenant, -1)
        if remaining <= 0 then
            redis.call('hdel', KEYS[5], stale_tenant)
        end
    end
    redis.call('zrem', KEYS[3], stale_token)
    redis.call('hdel', KEYS[4], stale_token)
end

local already_enqueued = redis.call('zscore', KEYS[3], token) ~= false

if not already_enqueued then
    if queue_limit > 0 then
        local queue_depth = tonumber(redis.call('hget', KEYS[5], tenant) or '0')
        if queue_depth >= queue_limit then
            return {2, queue_depth}
        end
    end
    redis.call('zadd', KEYS[3], now_ms, token)
    redis.call('hset', KEYS[4], token, tenant)
    redis.call('hincrby', KEYS[5], tenant, 1)
    already_enqueued = true
end

local head = redis.call('zrange', KEYS[3], 0, 0)[1]
if head ~= token then
    local position = tonumber(redis.call('zrank', KEYS[3], token) or 0) + 1
    local total_queue = tonumber(redis.call('zcard', KEYS[3]))
    local tenant_queue = tonumber(redis.call('hincrby', KEYS[5], tenant, 0) or 0)
    return {0, position, total_queue, tenant_queue}
end

local active_total = tonumber(redis.call('get', KEYS[1]) or '0')
if active_total >= max_global then
    local total_queue = tonumber(redis.call('zcard', KEYS[3]))
    local tenant_queue = tonumber(redis.call('hincrby', KEYS[5], tenant, 0) or 0)
    return {0, 1, total_queue, tenant_queue}
end

local tenant_active = tonumber(redis.call('hget', KEYS[2], tenant) or '0')
if tenant_active >= max_tenant then
    local total_queue = tonumber(redis.call('zcard', KEYS[3]))
    local tenant_queue = tonumber(redis.call('hincrby', KEYS[5], tenant, 0) or 0)
    return {0, 1, total_queue, tenant_queue}
end

local new_active_total = redis.call('incr', KEYS[1])
local new_tenant_active = redis.call('hincrby', KEYS[2], tenant, 1)
redis.call('zrem', KEYS[3], token)
redis.call('hdel', KEYS[4], token)
local queue_remaining = redis.call('hincrby', KEYS[5], tenant, -1)
if queue_remaining <= 0 then
    redis.call('hdel', KEYS[5], tenant)
    queue_remaining = 0
end
local total_queue = tonumber(redis.call('zcard', KEYS[3]))
return {1, new_active_total, new_tenant_active, queue_remaining, total_queue}
"""

_RELEASE_LUA = """
-- Release a slot: decrement global and tenant counters.
-- KEYS:
--   1: global active counter key
--   2: tenant active hash key
--   3: queue zset (for global depth insight)
-- ARGV:
--   1: tenant key
local tenant = ARGV[1]
local active_total = tonumber(redis.call('get', KEYS[1]) or '0')
if active_total > 0 then
    active_total = redis.call('decr', KEYS[1])
else
    redis.call('set', KEYS[1], 0)
    active_total = 0
end

local tenant_active = tonumber(redis.call('hget', KEYS[2], tenant) or '0')
if tenant_active > 1 then
    tenant_active = redis.call('hincrby', KEYS[2], tenant, -1)
elseif tenant_active == 1 then
    redis.call('hdel', KEYS[2], tenant)
    tenant_active = 0
else
    tenant_active = 0
end

local global_queue = tonumber(redis.call('zcard', KEYS[3]))
return {active_total, tenant_active, global_queue}
"""

_CANCEL_LUA = """
-- Cancel a queued request.
-- KEYS:
--   1: queue zset
--   2: request owner hash
--   3: tenant queue hash
-- ARGV:
--   1: tenant key
--   2: request token
local tenant = ARGV[1]
local token = ARGV[2]
if redis.call('zrem', KEYS[1], token) == 1 then
    redis.call('hdel', KEYS[2], token)
    local remaining = redis.call('hincrby', KEYS[3], tenant, -1)
    if remaining <= 0 then
        redis.call('hdel', KEYS[3], tenant)
        remaining = 0
    end
    local total_queue = tonumber(redis.call('zcard', KEYS[1]))
    return {remaining, total_queue}
end

redis.call('hdel', KEYS[2], token)
local tenant_queue = tonumber(redis.call('hincrby', KEYS[3], tenant, 0) or 0)
local total_queue = tonumber(redis.call('zcard', KEYS[1]))
return {tenant_queue, total_queue}
"""

_STATS_LUA = """
-- Return active counters and queue depths for diagnostics.
-- KEYS:
--   1: global active counter key
--   2: tenant active hash key
--   3: queue zset key
--   4: tenant queue hash key
local active_total = tonumber(redis.call('get', KEYS[1]) or '0')
local tenant_active = redis.call('hgetall', KEYS[2])
local global_queue = tonumber(redis.call('zcard', KEYS[3]))
local tenant_queue = redis.call('hgetall', KEYS[4])
return {active_total, global_queue, tenant_active, tenant_queue}
"""
