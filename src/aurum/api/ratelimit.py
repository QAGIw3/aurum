from __future__ import annotations

"""Simple rate limiting middleware using Redis when available (fallback in-memory)."""

import time
from dataclasses import dataclass
from typing import Optional

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from .config import CacheConfig


@dataclass(frozen=True)
class RateLimitConfig:
    rps: int = 10
    burst: int = 20

    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        import os

        rps = int(os.getenv("AURUM_API_RATE_LIMIT_RPS", "10") or 10)
        burst = int(os.getenv("AURUM_API_RATE_LIMIT_BURST", "20") or 20)
        return cls(rps=rps, burst=burst)


def _redis_client(cache_cfg: CacheConfig):
    if not cache_cfg.redis_url:
        return None
    try:
        import redis  # type: ignore

        return redis.Redis.from_url(cache_cfg.redis_url)
    except ModuleNotFoundError:
        return None


class RateLimitMiddleware:
    def __init__(self, app: ASGIApp, cache_cfg: CacheConfig, rl_cfg: RateLimitConfig) -> None:
        self.app = app
        self.cache_cfg = cache_cfg
        self.rl_cfg = rl_cfg
        self._redis = _redis_client(cache_cfg)
        self._window_counts: dict[str, tuple[int, int]] = {}

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        # Skip rate limiting for health and metrics endpoints
        if request.url.path in {"/health", "/metrics"}:
            await self.app(scope, receive, send)
            return
        client_ip = self._client_ip(request)
        allowed = await self._allow(client_ip)
        if not allowed:
            await self._reject(send)
            return
        await self.app(scope, receive, send)

    async def _allow(self, key: str) -> bool:
        now = int(time.time())
        window = f"ratelimit:{key}:{now}"
        if self._redis is not None:
            try:
                pipe = self._redis.pipeline()
                pipe.incr(window, 1)
                pipe.expire(window, 2)
                current, _ = pipe.execute()
                return int(current) <= (self.rl_cfg.rps + self.rl_cfg.burst)
            except Exception:
                # fall back to memory on Redis error
                pass
        # in-memory fixed window
        count, ts = self._window_counts.get(key, (0, now))
        if ts != now:
            count, ts = 0, now
        count += 1
        self._window_counts[key] = (count, ts)
        return count <= (self.rl_cfg.rps + self.rl_cfg.burst)

    def _client_ip(self, request: Request) -> str:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        client = request.client
        return client.host if client else "unknown"

    async def _reject(self, send: Send) -> None:
        response = JSONResponse({"error": "rate_limited", "message": "Too Many Requests"}, status_code=429)
        await response(send)


__all__ = ["RateLimitConfig", "RateLimitMiddleware"]
