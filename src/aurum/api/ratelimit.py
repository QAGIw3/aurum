from __future__ import annotations

"""Sliding-window rate limiting middleware with optional Redis backend."""

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple, Any

from aurum.core import AurumSettings

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from .config import CacheConfig

try:  # pragma: no cover - optional metric dependency
    from prometheus_client import Counter  # type: ignore
except Exception:  # pragma: no cover
    Counter = None  # type: ignore

if Counter is not None:  # pragma: no cover - metrics optional in tests
    RATE_LIMIT_EVENTS = Counter(
        "aurum_api_ratelimit_total",
        "Rate limit decisions",
        ["result"],
    )
else:  # pragma: no cover
    RATE_LIMIT_EVENTS = None


def _record_metric(result: str) -> None:
    if RATE_LIMIT_EVENTS is None:
        return
    try:
        RATE_LIMIT_EVENTS.labels(result=result).inc()
    except Exception:
        pass


@dataclass(frozen=True)
class RateLimitConfig:
    rps: int = 10
    burst: int = 20
    overrides: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    identifier_header: str | None = None

    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        import os

        rps = int(os.getenv("AURUM_API_RATE_LIMIT_RPS", "10") or 10)
        burst = int(os.getenv("AURUM_API_RATE_LIMIT_BURST", "20") or 20)
        overrides_env = os.getenv("AURUM_API_RATE_LIMIT_OVERRIDES", "")
        overrides: Dict[str, Tuple[int, int]] = {}
        if overrides_env:
            for item in overrides_env.split(","):
                if not item.strip():
                    continue
                try:
                    path_part, limits_part = item.split("=", 1)
                    path_prefix = path_part.strip()
                    limit_tokens = limits_part.split(":")
                    if len(limit_tokens) != 2:
                        continue
                    override_rps = int(limit_tokens[0])
                    override_burst = int(limit_tokens[1])
                    overrides[path_prefix] = (override_rps, override_burst)
                except ValueError:
                    continue
        identifier_header = os.getenv("AURUM_API_RATE_LIMIT_HEADER")
        return cls(rps=rps, burst=burst, overrides=overrides, identifier_header=identifier_header)

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "RateLimitConfig":
        rl = settings.api.rate_limit
        overrides = dict(rl.overrides)
        return cls(
            rps=rl.requests_per_second,
            burst=rl.burst,
            overrides=overrides,
            identifier_header=rl.identifier_header,
        )

    def limits_for_path(self, path: str) -> Tuple[int, int]:
        match_prefix = ""
        selected = (self.rps, self.burst)
        for prefix, values in self.overrides.items():
            if path.startswith(prefix) and len(prefix) > len(match_prefix):
                match_prefix = prefix
                selected = values
        return selected


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
        self._memory_windows: dict[str, Deque[float]] = {}
        self._window_seconds = 1.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        # Skip rate limiting for health and metrics endpoints
        if request.url.path in {"/health", "/metrics", "/ready"}:
            await self.app(scope, receive, send)
            return
        path = request.url.path
        rps, burst = self.rl_cfg.limits_for_path(path)
        key = self._rate_key(request, path)
        allowed, remaining, reset = await self._allow(key, rps=rps, burst=burst)
        if not allowed:
            await self._reject(scope, receive, send, remaining=0, reset=reset)
            return
        # Inject rate limit headers on successful responses
        limit_total = rps + burst

        async def send_with_headers(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                headers.append((b"x-ratelimit-limit", str(limit_total).encode("ascii")))
                headers.append((b"x-ratelimit-remaining", str(max(0, remaining)).encode("ascii")))
                headers.append((b"x-ratelimit-reset", str(reset).encode("ascii")))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)

    async def _allow(self, key: str, *, rps: int, burst: int) -> Tuple[bool, int, int]:
        limit_total = rps + burst
        now = time.time()
        reset = 1

        if self._redis is not None:
            now_ms = int(now * 1000)
            window_ms = int(self._window_seconds * 1000)
            window_start = now_ms - window_ms
            member = f"{now_ms}:{uuid.uuid4().hex}"
            try:
                pipe = self._redis.pipeline()
                redis_key = f"ratelimit:{key}"
                pipe.zremrangebyscore(redis_key, 0, window_start)
                pipe.zcard(redis_key)
                pipe.zadd(redis_key, {member: now_ms})
                pipe.pexpire(redis_key, window_ms)
                _, current_count, _, _ = pipe.execute()
                current = int(current_count) + 1
                remaining = max(0, limit_total - current)
                allowed = current <= limit_total
                _record_metric("allowed" if allowed else "blocked")
                return allowed, remaining, reset
            except Exception:
                _record_metric("error")
                self._redis = None

        bucket = self._memory_windows.setdefault(key, deque())
        window_start_time = now - self._window_seconds
        while bucket and bucket[0] <= window_start_time:
            bucket.popleft()
        bucket.append(now)
        current = len(bucket)
        remaining = max(0, limit_total - current)
        allowed = current <= limit_total
        _record_metric("allowed" if allowed else "blocked")
        return allowed, remaining, reset

    def _client_ip(self, request: Request) -> str:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        client = request.client
        return client.host if client else "unknown"

    def _rate_key(self, request: Request, path: str) -> str:
        principal = getattr(request.state, "principal", {}) or {}
        tenant = principal.get("tenant")
        header_identifier = None
        if self.rl_cfg.identifier_header:
            header_identifier = request.headers.get(self.rl_cfg.identifier_header)
        identifier = tenant or header_identifier or self._client_ip(request)
        return f"{path}:{identifier}"

    async def _reject(self, scope: Scope, receive: Receive, send: Send, *, remaining: int, reset: int) -> None:
        headers = {"Retry-After": str(reset), "X-RateLimit-Limit": str(self.rl_cfg.rps + self.rl_cfg.burst), "X-RateLimit-Remaining": str(remaining), "X-RateLimit-Reset": str(reset)}
        response = JSONResponse({"error": "rate_limited", "message": "Too Many Requests"}, status_code=429, headers=headers)
        await response(scope, receive, send)


__all__ = ["RateLimitConfig", "RateLimitMiddleware"]
