from __future__ import annotations

"""Sliding-window rate limiting middleware with optional Redis backend.

Features:
- Per-path and per-tenant overrides (longest-prefix match)
- Optional Redis-backed sliding window; falls back to in-memory windows
- Standard headers: `X-RateLimit-{Limit,Remaining,Reset}` and 429 with `Retry-After`
- Metrics (Prometheus) when `prometheus_client` is available

Environment variables:
- `AURUM_API_RATE_LIMIT_RPS` / `AURUM_API_RATE_LIMIT_BURST`
- `AURUM_API_RATE_LIMIT_OVERRIDES` (e.g., "/v1/curves=20:40,/v1/metadata=50:100")
- `AURUM_API_RATE_LIMIT_HEADER` (custom identifier header)
- `AURUM_API_RATE_LIMIT_WHITELIST` (comma‑separated identifiers)

See also:
- docs/README.md → Runtime config for admin endpoints to update limits at runtime.
"""

import math
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, Dict, Optional, Tuple, Any
from datetime import datetime, timezone, timedelta

from aurum.core import AurumSettings

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send
from fastapi import APIRouter, Query, HTTPException

from .config import CacheConfig
from .quota_manager import TenantQuotaManager
from ..telemetry.context import (
    TenantIdValidationError,
    extract_tenant_id_from_headers,
    get_request_id,
)
from ...observability.metric_helpers import get_counter, get_gauge, get_histogram


RATE_LIMIT_EVENTS = get_counter(
    "aurum_api_ratelimit_total",
    "Rate limit decisions",
    ["result", "path", "tenant_id"],
)

RATE_LIMIT_ACTIVE_WINDOWS = get_gauge(
    "aurum_api_ratelimit_active_windows",
    "Number of active rate limit windows",
    ["path", "tenant_id"],
)

RATE_LIMIT_REQUESTS_PER_WINDOW = get_histogram(
    "aurum_api_ratelimit_requests_per_window",
    "Number of requests in current window",
    ["path", "tenant_id"],
    buckets=[0, 1, 2, 5, 10, 20, 50, 100, 200, 500],
)

RATE_LIMIT_BLOCK_DURATION = get_histogram(
    "aurum_api_ratelimit_block_duration_seconds",
    "Duration of rate limit blocks",
    buckets=[0.1, 1, 5, 10, 30, 60, 300, 600],
)


def _record_metric(result: str, path: str = "", tenant_id: str = "", request_count: int = 0) -> None:
    if RATE_LIMIT_EVENTS is None:
        return
    try:
        RATE_LIMIT_EVENTS.labels(result=result, path=path, tenant_id=tenant_id).inc()

        # Update gauge for active windows
        if RATE_LIMIT_ACTIVE_WINDOWS is not None:
            window_key = f"{path}:{tenant_id}"
            # This is a simplified approach - in production would track actual window state
            # For now, we just record when we have rate limit activity
            if result == "blocked":
                RATE_LIMIT_ACTIVE_WINDOWS.labels(path=path, tenant_id=tenant_id).set(1)

        # Record request count per window
        if RATE_LIMIT_REQUESTS_PER_WINDOW is not None and request_count > 0:
            RATE_LIMIT_REQUESTS_PER_WINDOW.labels(path=path, tenant_id=tenant_id).observe(request_count)

        # Record block duration
        if result == "blocked" and RATE_LIMIT_BLOCK_DURATION is not None:
            # This would need to be called when the block is lifted
            # For now, we'll observe a default duration
            RATE_LIMIT_BLOCK_DURATION.observe(1.0)  # 1 second default

    except Exception:
        pass


@dataclass(frozen=True)
class RateLimitConfig:
    rps: int = 10
    burst: int = 20
    daily_cap: int = 100_000  # default daily cap per tenant; set 0 to disable
    overrides: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    tenant_overrides: Dict[str, Dict[str, Tuple[int, int]]] = field(default_factory=dict)
    identifier_header: str | None = None
    whitelist: Tuple[str, ...] = field(default_factory=tuple)

    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        import os

        rps = int(os.getenv("AURUM_API_RATE_LIMIT_RPS", "10") or 10)
        burst = int(os.getenv("AURUM_API_RATE_LIMIT_BURST", "20") or 20)
        daily_env = os.getenv("AURUM_API_DAILY_CAP") or os.getenv("AURUM_API_RATE_LIMIT_DAILY_CAP")
        daily_cap = int(daily_env) if daily_env not in (None, "") else 100_000
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
        whitelist_env = os.getenv("AURUM_API_RATE_LIMIT_WHITELIST", "")
        whitelist: Tuple[str, ...]
        if whitelist_env:
            whitelist = tuple(item.strip() for item in whitelist_env.split(",") if item.strip())
        else:
            whitelist = ()

        return cls(
            rps=rps,
            burst=burst,
            daily_cap=daily_cap,
            overrides=overrides,
            identifier_header=identifier_header,
            whitelist=whitelist,
        )

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "RateLimitConfig":
        rl = settings.api.rate_limit
        overrides = dict(rl.overrides)
        tenant_overrides = dict(rl.tenant_overrides)
        return cls(
            rps=rl.requests_per_second,
            burst=rl.burst,
            daily_cap=100_000,  # default daily limit; can be overridden via Redis config
            overrides=overrides,
            tenant_overrides=tenant_overrides,
            identifier_header=rl.identifier_header,
            whitelist=tuple(rl.whitelist),
        )

    async def limits_for_path(self, path: str, tenant: Optional[str] = None) -> Tuple[int, int]:
        # First check tenant-specific overrides from database
        if tenant:
            try:
                from aurum.scenarios.storage import get_scenario_store
                from aurum.telemetry.context import set_tenant_id, reset_tenant_id

                store = get_scenario_store()
                token = set_tenant_id(tenant)
                try:
                    overrides = await store.list_rate_limit_overrides(tenant)
                    match_prefix = ""
                    selected = (self.rps, self.burst)
                    for override in overrides:
                        if override["enabled"] and path.startswith(override["path_prefix"]):
                            if len(override["path_prefix"]) > len(match_prefix):
                                match_prefix = override["path_prefix"]
                                selected = (override["requests_per_second"], override["burst_capacity"])
                    if match_prefix:
                        return selected
                finally:
                    reset_tenant_id(token)
            except Exception:
                # Fall back to in-memory overrides if database query fails
                pass

            # Fall back to in-memory tenant overrides
            if tenant in self.tenant_overrides:
                tenant_overrides = self.tenant_overrides[tenant]
                match_prefix = ""
                selected = (self.rps, self.burst)
                for prefix, values in tenant_overrides.items():
                    if path.startswith(prefix) and len(prefix) > len(match_prefix):
                        match_prefix = prefix
                        selected = values
                return selected

        # Fall back to path-based overrides
        match_prefix = ""
        selected = (self.rps, self.burst)
        for prefix, values in self.overrides.items():
            if path.startswith(prefix) and len(prefix) > len(match_prefix):
                match_prefix = prefix
                selected = values
        return selected

    def is_whitelisted(self, *identifiers: Optional[str]) -> bool:
        if not self.whitelist:
            return False
        candidates = {identifier for identifier in identifiers if identifier}
        if not candidates:
            return False
        whitelist_set = set(self.whitelist)
        return any(candidate in whitelist_set for candidate in candidates)


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
        self._tenant_quota = TenantQuotaManager(self._redis)
        self._memory_windows: dict[str, Deque[float]] = {}
        self._window_seconds = 1.0

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        metrics_path = "/metrics"
        settings = getattr(request.app.state, "settings", None)
        if isinstance(settings, AurumSettings):
            configured_path = settings.api.metrics.path
            metrics_path = configured_path if configured_path.startswith("/") else f"/{configured_path}"
        # Skip rate limiting for health and metrics endpoints
        if request.url.path in {"/health", metrics_path, "/ready"}:
            await self.app(scope, receive, send)
            return
        path = request.url.path
        identifier, tenant_id, header_identifier, client_ip = self._resolve_identity(request)
        rps, burst = await self.rl_cfg.limits_for_path(path, tenant_id)

        if self.rl_cfg.is_whitelisted(identifier, tenant_id, header_identifier, client_ip):
            await self.app(scope, receive, send)
            return

        limit_total = rps + burst
        daily_remaining_header: Optional[int] = None
        daily_limit_header: Optional[int] = None

        # Prefer per‑tenant quotas when tenant_id is known
        if tenant_id is not None and self._tenant_quota is not None:
            # Allow per-tenant overrides (rps, burst, daily) via Redis config hash
            cfg = self._tenant_quota.get_tenant_config(
                tenant_id,
                default_rps=rps,
                default_burst=burst,
                default_daily=self.rl_cfg.daily_cap,
            )
            rps = int(cfg.get("rps", rps))
            burst = int(cfg.get("burst", burst))
            daily_cap = int(cfg.get("daily_cap", self.rl_cfg.daily_cap))
            limit_total = rps + burst
            result = self._tenant_quota.check_and_consume(tenant_id, rps=rps, burst=burst, daily_cap=daily_cap)
            if not result.get("allowed", False):
                reset_seconds = int(result.get("daily_reset") if result.get("reason") == "daily_exceeded" else result.get("rps_reset", 1))
                await self._reject(scope, receive, send, remaining=0, reset=reset_seconds)
                return
            remaining = int(result.get("rps_remaining", 0))
            reset = int(result.get("rps_reset", 1))
            daily_remaining_header = int(result.get("daily_remaining", 0))
            daily_limit_header = daily_cap
        else:
            key = self._rate_key(path, identifier)
            allowed, remaining, reset = await self._allow(key, rps=rps, burst=burst)
            if not allowed:
                await self._reject(scope, receive, send, remaining=0, reset=reset)
                return

        async def send_with_headers(message: dict[str, Any]) -> None:
            if message.get("type") == "http.response.start":
                headers = list(message.get("headers") or [])
                headers.append((b"X-RateLimit-Limit", str(limit_total).encode("ascii")))
                headers.append((b"X-RateLimit-Remaining", str(max(0, remaining)).encode("ascii")))
                headers.append((b"X-RateLimit-Reset", str(reset).encode("ascii")))
                if daily_remaining_header is not None:
                    headers.append((b"X-RateLimit-Daily-Remaining", str(max(0, daily_remaining_header)).encode("ascii")))
                if daily_limit_header is not None:
                    headers.append((b"X-RateLimit-Daily-Limit", str(max(0, daily_limit_header)).encode("ascii")))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)

    async def _allow(self, key: str, *, rps: int, burst: int) -> Tuple[bool, int, int]:
        limit_total = rps + burst
        now = time.time()

        if self._redis is not None:
            now_ms = int(now * 1000)
            window_ms = int(self._window_seconds * 1000)
            window_start = now_ms - window_ms
            member = f"{now_ms}:{uuid.uuid4().hex}"
            try:
                redis_key = f"ratelimit:{key}"
                pipe = self._redis.pipeline()
                pipe.zremrangebyscore(redis_key, 0, window_start)
                pipe.zadd(redis_key, {member: now_ms})
                pipe.zcard(redis_key)
                pipe.zrange(redis_key, 0, 0, withscores=True)
                pipe.pexpire(redis_key, window_ms)
                _, _, current_count, earliest, _ = pipe.execute()
                current = int(current_count)
                remaining = max(0, limit_total - current)
                allowed = current <= limit_total
                oldest_score = None
                if earliest:
                    # zrange returns list of (member, score)
                    try:
                        oldest_score = float(earliest[0][1])
                    except (ValueError, TypeError):
                        oldest_score = None
                reset_seconds = 1
                if oldest_score is not None:
                    reset_window = ((oldest_score + window_ms) - now_ms) / 1000.0
                    reset_seconds = max(0, math.ceil(reset_window))
                _record_metric("allowed" if allowed else "blocked")
                return allowed, remaining, reset_seconds
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
        reset_seconds = 1
        if bucket:
            reset_window = self._window_seconds - (now - bucket[0])
            reset_seconds = max(0, math.ceil(reset_window))
        return allowed, remaining, reset_seconds

    async def _reject(self, scope: Scope, receive: Receive, send: Send, *, remaining: int, reset: int) -> None:
        headers = {
            "Retry-After": str(reset),
            "X-RateLimit-Limit": str(self.rl_cfg.rps + self.rl_cfg.burst),
            "X-RateLimit-Remaining": str(remaining),
            "X-RateLimit-Reset": str(reset),
        }
        response = JSONResponse({"error": "rate_limited", "message": "Too Many Requests"}, status_code=429, headers=headers)
        await response(scope, receive, send)

    def _client_ip(self, request: Request) -> str:
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        client = request.client
        return client.host if client else "unknown"

    def _rate_key(self, path: str, identifier: str) -> str:
        return f"{path}:{identifier}"

    def _resolve_identity(
        self, request: Request
    ) -> tuple[str, Optional[str], Optional[str], str]:
        # First priority: check request.state.tenant (set by auth middleware)
        tenant = getattr(request.state, "tenant", None)

        # Second priority: check principal from auth middleware
        principal = getattr(request.state, "principal", {}) or {}
        if tenant is None:
            tenant = principal.get("tenant")

        # Third priority: check X-Aurum-Tenant header
        if tenant is None:
            try:
                tenant = extract_tenant_id_from_headers(request.headers)
            except TenantIdValidationError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        header_identifier: Optional[str] = None
        if self.rl_cfg.identifier_header:
            raw_header = request.headers.get(self.rl_cfg.identifier_header)
            if raw_header:
                stripped = raw_header.strip()
                header_identifier = stripped or None

        client_ip = self._client_ip(request)

        # Construct identifier with priority: tenant+header -> tenant -> header -> client_ip
        if tenant and header_identifier:
            identifier = f"tenant:{tenant}:header:{header_identifier}"
        elif tenant:
            identifier = f"tenant:{tenant}"
        elif header_identifier:
            identifier = f"header:{header_identifier}"
        else:
            identifier = f"ip:{client_ip}"

        return identifier, tenant, header_identifier, client_ip


# Admin endpoints for rate limit inspection
ratelimit_admin_router = APIRouter()


@ratelimit_admin_router.get("/v1/admin/ratelimit/config")
async def get_ratelimit_config() -> Dict[str, Any]:
    """Get current rate limiting configuration."""
    from ..core.settings import AurumSettings

    settings = AurumSettings.from_env()
    rl = settings.api.rate_limit
    window_seconds = getattr(rl, "window_seconds", 1)

    return {
        "meta": {
            "request_id": get_request_id(),
        },
        "data": {
            "enabled": rl.enabled,
            "requests_per_second": rl.requests_per_second,
            "burst": rl.burst,
            "daily_cap": RateLimitConfig.from_settings(settings).daily_cap,
            "overrides": rl.overrides,
            "tenant_overrides": rl.tenant_overrides,
            "window_seconds": window_seconds,
        }
    }


@ratelimit_admin_router.get("/v1/admin/ratelimit/status")
async def get_ratelimit_status(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    path: Optional[str] = Query(None, description="Filter by path prefix"),
) -> Dict[str, Any]:
    """Get current rate limiting status and active windows."""
    # This would integrate with actual rate limit state tracking
    # For now, return basic information
    return {
        "meta": {
            "request_id": get_request_id(),
        },
        "data": {
            "active_windows": 0,  # Would be populated from actual state
            "blocked_requests": 0,  # Would be populated from metrics
            "allowed_requests": 0,  # Would be populated from metrics
            "filters": {
                "tenant_id": tenant_id,
                "path": path,
            }
        }
    }


@ratelimit_admin_router.get("/v1/admin/ratelimit/metrics")
async def get_ratelimit_metrics(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
) -> Dict[str, Any]:
    """Get rate limiting metrics."""
    # In a real implementation, this would expose Prometheus metrics
    # or provide a structured view of rate limiting statistics
    return {
        "meta": {
            "request_id": get_request_id(),
        },
        "data": {
            "tenant_id": tenant_id,
            "metrics": {
                "requests_blocked": 0,
                "requests_allowed": 0,
                "active_rate_limits": 0,
                "average_window_utilization": 0.0,
            }
        }
    }


def _admin_redis_client() -> Optional["redis.Redis"]:
    try:
        from .config import CacheConfig  # local import to avoid import cycles
        from aurum.core import AurumSettings
        settings = AurumSettings.from_env()
        cache_cfg = CacheConfig.from_settings(settings)
        return _redis_client(cache_cfg)
    except Exception:
        return None


@ratelimit_admin_router.get("/v1/admin/ratelimit/tenants")
async def list_tenant_quotas(
    prefix: Optional[str] = Query(None, description="Filter tenants by prefix"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum tenants to return"),
) -> Dict[str, Any]:
    """List per‑tenant daily usage and effective caps for the current UTC day."""
    now = datetime.utcnow()
    day_str = now.strftime("%Y%m%d")
    client = _admin_redis_client()
    tqm = TenantQuotaManager(client)
    rl_cfg = RateLimitConfig.from_settings(AurumSettings.from_env())
    items: List[Dict[str, Any]] = []
    scanned = 0
    if client is not None:
        try:
            pattern = f"quota:tenant:*:day:{day_str}"
            for key in client.scan_iter(match=pattern, count=1000):
                key_str = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key)
                # Extract tenant id between 'tenant:' and ':day:'
                try:
                    tenant_id = key_str.split(":tenant:", 1)[1].split(":day:", 1)[0]
                except Exception:
                    continue
                if prefix and not tenant_id.startswith(prefix):
                    continue
                try:
                    used = int(client.get(key) or 0)
                except Exception:
                    used = 0
                cfg = tqm.get_tenant_config(tenant_id, default_rps=rl_cfg.rps, default_burst=rl_cfg.burst, default_daily=rl_cfg.daily_cap)
                items.append({
                    "tenant_id": tenant_id,
                    "used_today": used,
                    "daily_cap": cfg["daily_cap"],
                    "remaining_today": max(0, int(cfg["daily_cap"]) - used),
                    "rps": cfg["rps"],
                    "burst": cfg["burst"],
                })
                scanned += 1
                if scanned >= limit:
                    break
        except Exception:
            pass
    return {
        "meta": {"request_id": get_request_id(), "day": day_str, "count": len(items)},
        "data": items,
    }


@ratelimit_admin_router.get("/v1/admin/ratelimit/tenants/{tenant_id}")
async def get_tenant_quota(tenant_id: str) -> Dict[str, Any]:
    """Get quota status and usage for a specific tenant."""
    client = _admin_redis_client()
    tqm = TenantQuotaManager(client)
    rl_cfg = RateLimitConfig.from_settings(AurumSettings.from_env())
    cfg = tqm.get_tenant_config(tenant_id, default_rps=rl_cfg.rps, default_burst=rl_cfg.burst, default_daily=rl_cfg.daily_cap)
    used_today, day_reset = tqm.get_day_usage(tenant_id)
    current_window = tqm.get_rps_usage(tenant_id)
    return {
        "meta": {"request_id": get_request_id()},
        "data": {
            "tenant_id": tenant_id,
            "config": cfg,
            "usage": {
                "rps_window_count": current_window,
                "used_today": used_today,
                "remaining_today": max(0, int(cfg["daily_cap"]) - int(used_today)),
                "reset_today_seconds": day_reset,
            },
        },
    }


@ratelimit_admin_router.post("/v1/admin/ratelimit/test")
async def test_ratelimit(
    path: str = Query(..., description="Path to test rate limiting for"),
    tenant_id: str = Query(..., description="Tenant ID to test"),
    requests_per_second: int = Query(10, description="RPS to test with"),
) -> Dict[str, Any]:
    """Test rate limiting for a specific path and tenant."""
    # This would simulate rate limit behavior for testing
    return {
        "meta": {
            "request_id": get_request_id(),
        },
        "data": {
            "test_config": {
                "path": path,
                "tenant_id": tenant_id,
                "requests_per_second": requests_per_second,
            },
            "simulated_result": "allowed",  # Would be actual simulation result
            "remaining_requests": requests_per_second - 1,
            "reset_seconds": 1
        }
    }


@ratelimit_admin_router.post("/v1/admin/ratelimit/tenants/{tenant_id}/config")
async def set_tenant_quota_config(
    tenant_id: str,
    body: Dict[str, int],
) -> Dict[str, Any]:
    """Set per-tenant quota configuration in Redis.

    Body fields (optional):
    - rps: int
    - burst: int
    - daily_cap: int
    """
    client = _admin_redis_client()
    if client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    fields: Dict[str, int] = {}
    for key in ("rps", "burst", "daily_cap"):
        if key in body:
            try:
                val = int(body[key])
                if val < 0:
                    raise ValueError
                fields[key] = val
            except Exception:
                raise HTTPException(status_code=400, detail=f"Invalid integer for {key}")
    if not fields:
        raise HTTPException(status_code=400, detail="No valid fields provided")
    tqm = TenantQuotaManager(client)
    client.hset(tqm._key_cfg(tenant_id), mapping=fields)
    return {"meta": {"request_id": get_request_id()}, "data": {"tenant_id": tenant_id, "updated": fields}}


@ratelimit_admin_router.post("/v1/admin/ratelimit/tenants/{tenant_id}/reset")
async def reset_tenant_daily_usage(tenant_id: str) -> Dict[str, Any]:
    """Reset the tenant's usage for the current UTC day."""
    client = _admin_redis_client()
    if client is None:
        raise HTTPException(status_code=503, detail="Redis unavailable")
    now = datetime.utcnow()
    day_str = now.strftime("%Y%m%d")
    key = f"quota:tenant:{tenant_id}:day:{day_str}"
    with client.pipeline() as pipe:
        pipe.delete(key)
        pipe.execute()
    return {"meta": {"request_id": get_request_id()}, "data": {"tenant_id": tenant_id, "reset_day": day_str}}


__all__ = ["RateLimitConfig", "RateLimitMiddleware", "ratelimit_admin_router"]
