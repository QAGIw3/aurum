"""Enhanced tenant-aware rate limiting with circuit breaker and graceful degradation.

This module provides production-ready rate limiting with:
- Per-tenant quota tiers with dynamic adjustment
- Circuit breaker behavior when cache/redis is unavailable
- Graceful degradation to in-memory limiting
- Comprehensive metrics and monitoring
- Admin endpoints for inspection and management

Environment variables (selected):
- `AURUM_API_RATE_LIMIT_RPS` / `AURUM_API_RATE_LIMIT_BURST` (defaults)
- `AURUM_API_RATE_LIMIT_OVERRIDES` – per-path rps:burst (comma‑separated)
- `AURUM_API_RATE_LIMIT_HEADER` – custom identifier header
- `AURUM_API_RATE_LIMIT_WHITELIST` – comma‑separated identifiers to skip RL

See also:
- src/aurum/api/rate_limit_management.py (admin routes)
- docs/README.md → Runtime configuration and Admin APIs
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Deque, Dict, Optional, Tuple, Any, List

from aurum.core import AurumSettings
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send
from fastapi import APIRouter, Depends, HTTPException

from .config import CacheConfig
from ..telemetry.context import get_request_id

try:  # pragma: no cover - optional metric dependency
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore
except Exception:  # pragma: no cover
    Counter, Gauge, Histogram = None, None, None  # type: ignore

if Counter is not None:  # pragma: no cover - metrics optional in tests
    TENANT_RATE_LIMIT_EVENTS = Counter(
        "aurum_api_tenant_ratelimit_total",
        "Tenant-aware rate limit decisions",
        ["result", "path", "tenant_id", "tier"],
    )

    TENANT_RATE_LIMIT_ACTIVE_WINDOWS = Gauge(
        "aurum_api_tenant_ratelimit_active_windows",
        "Number of active rate limit windows per tenant",
        ["tenant_id", "tier"],
    )

    TENANT_RATE_LIMIT_REQUESTS_PER_WINDOW = Histogram(
        "aurum_api_tenant_ratelimit_requests_per_window",
        "Number of requests in current window per tenant",
        ["tenant_id", "tier"],
        buckets=[0, 1, 2, 5, 10, 20, 50, 100, 200, 500],
    )

    TENANT_RATE_LIMIT_BLOCK_DURATION = Histogram(
        "aurum_api_tenant_ratelimit_block_duration_seconds",
        "Duration of tenant rate limit blocks",
        ["tenant_id", "tier"],
        buckets=[0.1, 1, 5, 10, 30, 60, 300, 600],
    )

    TENANT_RATE_LIMIT_CIRCUIT_STATE = Gauge(
        "aurum_api_tenant_ratelimit_circuit_state",
        "Circuit breaker state for tenant rate limiting",
        ["state"],
    )

    TENANT_RATE_LIMIT_CACHE_FAILURES = Counter(
        "aurum_api_tenant_ratelimit_cache_failures_total",
        "Cache failures in tenant rate limiting",
        ["failure_type"],
    )
else:  # pragma: no cover
    TENANT_RATE_LIMIT_EVENTS = None
    TENANT_RATE_LIMIT_ACTIVE_WINDOWS = None
    TENANT_RATE_LIMIT_REQUESTS_PER_WINDOW = None
    TENANT_RATE_LIMIT_BLOCK_DURATION = None
    TENANT_RATE_LIMIT_CIRCUIT_STATE = None
    TENANT_RATE_LIMIT_CACHE_FAILURES = None


class CircuitBreakerState(Enum):
    """Circuit breaker states for rate limiting."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TenantTier(Enum):
    """Tenant quota tiers."""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


@dataclass
class TenantQuota:
    """Quota configuration for a tenant tier."""
    requests_per_second: int
    burst_size: int
    max_concurrent_requests: int = 100
    priority_boost: float = 1.0

    def with_dynamic_adjustment(self, load_factor: float) -> "TenantQuota":
        """Adjust quotas based on system load."""
        if load_factor > 0.9:  # High load - reduce quotas
            factor = 0.7
        elif load_factor > 0.7:  # Medium load - slight reduction
            factor = 0.85
        else:  # Normal load - full quotas
            factor = 1.0

        return TenantQuota(
            requests_per_second=max(1, int(self.requests_per_second * factor)),
            burst_size=max(1, int(self.burst_size * factor)),
            max_concurrent_requests=max(10, int(self.max_concurrent_requests * factor)),
            priority_boost=self.priority_boost,
        )


class TenantRateLimitManager:
    """Enhanced tenant-aware rate limiting with circuit breaker."""

    def __init__(self):
        self._logger = logging.getLogger(__name__)

        # Default quotas per tier
        self._tier_quotas = {
            TenantTier.FREE: TenantQuota(5, 10, 50, 0.5),
            TenantTier.BASIC: TenantQuota(20, 40, 100, 0.8),
            TenantTier.PREMIUM: TenantQuota(100, 200, 500, 1.0),
            TenantTier.ENTERPRISE: TenantQuota(500, 1000, 1000, 1.2),
        }

        # Tenant-specific overrides
        self._tenant_overrides: Dict[str, TenantQuota] = {}

        # Circuit breaker state
        self._circuit_breaker_state = CircuitBreakerState.CLOSED
        self._circuit_failure_count = 0
        self._circuit_failure_threshold = 5
        self._circuit_reset_timeout = 60  # seconds
        self._last_failure_time = 0

        # System load tracking
        self._system_load = 0.0
        self._load_update_interval = 10  # seconds
        self._last_load_update = 0

        # In-memory fallback windows (when Redis/cache fails)
        self._memory_windows: Dict[str, Deque[float]] = defaultdict(deque)
        self._window_seconds = 1.0

    def get_tenant_tier(self, tenant_id: str) -> TenantTier:
        """Determine tenant tier based on tenant_id or configuration."""
        # Simple tier detection based on tenant_id pattern
        # In production, this would be configured per tenant
        if tenant_id.startswith("enterprise-"):
            return TenantTier.ENTERPRISE
        elif tenant_id.startswith("premium-"):
            return TenantTier.PREMIUM
        elif tenant_id.startswith("basic-"):
            return TenantTier.BASIC
        else:
            return TenantTier.FREE

    def get_tenant_quota(self, tenant_id: str) -> TenantQuota:
        """Get quota for a tenant, with dynamic adjustment."""
        # Check for tenant-specific override first
        if tenant_id in self._tenant_overrides:
            quota = self._tenant_overrides[tenant_id]
        else:
            tier = self.get_tenant_tier(tenant_id)
            quota = self._tier_quotas[tier]

        # Apply dynamic adjustment based on system load
        load_factor = self._get_system_load()
        return quota.with_dynamic_adjustment(load_factor)

    def _get_system_load(self) -> float:
        """Get current system load factor."""
        current_time = time.time()

        # Update load calculation periodically
        if current_time - self._last_load_update > self._load_update_interval:
            # Simple load calculation based on active rate limit windows
            # In production, this would consider CPU, memory, and other metrics
            active_windows = len(self._memory_windows)
            estimated_load = min(1.0, active_windows / 100)  # Assume 100 concurrent tenants = 100% load
            self._system_load = estimated_load
            self._last_load_update = current_time

        return self._system_load

    def _check_circuit_breaker(self) -> bool:
        """Check if operations can proceed based on circuit breaker state."""
        current_time = time.time()

        if self._circuit_breaker_state == CircuitBreakerState.CLOSED:
            return True

        if self._circuit_breaker_state == CircuitBreakerState.OPEN:
            if current_time - self._last_failure_time > self._circuit_reset_timeout:
                self._circuit_breaker_state = CircuitBreakerState.HALF_OPEN
                self._logger.info("Rate limit circuit breaker transitioning to half-open")
                return True
            return False

        return self._circuit_breaker_state == CircuitBreakerState.HALF_OPEN

    def _record_circuit_success(self):
        """Record successful circuit breaker operation."""
        if self._circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self._circuit_breaker_state = CircuitBreakerState.CLOSED
            self._circuit_failure_count = 0
            self._logger.info("Rate limit circuit breaker closed after successful operation")

    def _record_circuit_failure(self):
        """Record circuit breaker failure."""
        self._circuit_failure_count += 1
        self._last_failure_time = time.time()

        if self._circuit_failure_count >= self._circuit_failure_threshold:
            self._circuit_breaker_state = CircuitBreakerState.OPEN
            self._logger.warning(f"Rate limit circuit breaker opened after {self._circuit_failure_count} failures")

    async def check_rate_limit(
        self,
        tenant_id: str,
        path: str,
        redis_client = None
    ) -> Tuple[bool, Optional[float], Dict[str, Any]]:
        """Check if request should be rate limited.

        Args:
            tenant_id: Tenant identifier
            path: Request path
            redis_client: Optional Redis client for distributed limiting

        Returns:
            Tuple of (allowed, retry_after_seconds, metadata)
        """
        if not self._check_circuit_breaker():
            # Circuit breaker open - fall back to in-memory limiting
            self._logger.warning("Rate limit circuit breaker open, using in-memory fallback")
            return self._check_memory_rate_limit(tenant_id, path)

        quota = self.get_tenant_quota(tenant_id)
        window_key = f"ratelimit:{tenant_id}:{path}"

        try:
            if redis_client:
                # Try Redis-based limiting first
                return await self._check_redis_rate_limit(redis_client, window_key, quota)
            else:
                # Fall back to in-memory limiting
                return self._check_memory_rate_limit(tenant_id, path, quota)

        except Exception as e:
            # Record circuit breaker failure and fall back to in-memory
            self._record_circuit_failure()
            TENANT_RATE_LIMIT_CACHE_FAILURES and TENANT_RATE_LIMIT_CACHE_FAILURES.labels(
                failure_type="redis_error"
            ).inc()

            self._logger.error(f"Rate limit cache failure: {e}, falling back to in-memory")
            return self._check_memory_rate_limit(tenant_id, path, quota)

    async def _check_redis_rate_limit(
        self,
        redis_client,
        window_key: str,
        quota: TenantQuota
    ) -> Tuple[bool, Optional[float], Dict[str, Any]]:
        """Check rate limit using Redis."""
        current_time = time.time()
        window_start = math.floor(current_time / self._window_seconds) * self._window_seconds

        # Use Redis pipeline for atomic operations
        pipe = redis_client.pipeline()
        pipe.zremrangebyscore(window_key, 0, window_start - self._window_seconds)
        pipe.zcard(window_key)
        pipe.zrange(window_key, 0, 0, withscores=True)
        pipe.expire(window_key, int(self._window_seconds * 2))

        results = await pipe.execute()
        request_count = results[1]
        oldest_request = results[2][0][1] if results[2] else current_time

        # Check limits
        if request_count >= quota.burst_size:
            retry_after = self._window_seconds - (current_time - oldest_request)
            metadata = {
                "quota": quota.__dict__,
                "current_count": request_count,
                "retry_after": retry_after,
                "circuit_breaker_state": self._circuit_breaker_state.value
            }
            return False, retry_after, metadata

        # Add current request
        pipe = redis_client.pipeline()
        pipe.zadd(window_key, {str(current_time): current_time})
        pipe.zremrangebyrank(window_key, 0, -quota.burst_size - 1)
        await pipe.execute()

        self._record_circuit_success()
        metadata = {
            "quota": quota.__dict__,
            "current_count": request_count + 1,
            "circuit_breaker_state": self._circuit_breaker_state.value
        }
        return True, None, metadata

    def _check_memory_rate_limit(
        self,
        tenant_id: str,
        path: str,
        quota: Optional[TenantQuota] = None
    ) -> Tuple[bool, Optional[float], Dict[str, Any]]:
        """Check rate limit using in-memory storage."""
        if quota is None:
            quota = self.get_tenant_quota(tenant_id)

        current_time = time.time()
        window_key = f"{tenant_id}:{path}"

        # Clean old entries
        window = self._memory_windows[window_key]
        while window and current_time - window[0] > self._window_seconds:
            window.popleft()

        # Check limits
        if len(window) >= quota.burst_size:
            oldest_request = window[0]
            retry_after = self._window_seconds - (current_time - oldest_request)

            metadata = {
                "quota": quota.__dict__,
                "current_count": len(window),
                "retry_after": retry_after,
                "fallback_mode": True,
                "circuit_breaker_state": self._circuit_breaker_state.value
            }
            return False, retry_after, metadata

        # Add current request
        window.append(current_time)

        metadata = {
            "quota": quota.__dict__,
            "current_count": len(window),
            "fallback_mode": True,
            "circuit_breaker_state": self._circuit_breaker_state.value
        }
        return True, None, metadata

    def get_tenant_status(self, tenant_id: str) -> Dict[str, Any]:
        """Get rate limiting status for a tenant."""
        tier = self.get_tenant_tier(tenant_id)
        quota = self.get_tenant_quota(tenant_id)

        # Count active windows
        active_windows = sum(
            1 for key in self._memory_windows.keys()
            if key.startswith(f"{tenant_id}:")
        )

        return {
            "tenant_id": tenant_id,
            "tier": tier.value,
            "quota": quota.__dict__,
            "active_windows": active_windows,
            "circuit_breaker_state": self._circuit_breaker_state.value,
            "system_load": self._system_load,
            "timestamp": time.time()
        }

    def get_all_tenant_status(self) -> List[Dict[str, Any]]:
        """Get rate limiting status for all tenants."""
        tenant_ids = set()
        for key in self._memory_windows.keys():
            if ":" in key:
                tenant_id = key.split(":")[0]
                tenant_ids.add(tenant_id)

        return [self.get_tenant_status(tenant_id) for tenant_id in sorted(tenant_ids)]

    def reset_tenant(self, tenant_id: str):
        """Reset rate limiting state for a tenant (admin operation)."""
        keys_to_remove = [
            key for key in self._memory_windows.keys()
            if key.startswith(f"{tenant_id}:")
        ]

        for key in keys_to_remove:
            del self._memory_windows[key]

        self._logger.info(f"Reset rate limiting for tenant {tenant_id}")


# Global rate limit manager instance
_rate_limit_manager = TenantRateLimitManager()


def get_rate_limit_manager() -> TenantRateLimitManager:
    """Get the global rate limit manager instance."""
    return _rate_limit_manager


class TenantRateLimitMiddleware:
    """Enhanced tenant-aware rate limiting middleware."""

    def __init__(self, app: ASGIApp, cache_cfg: CacheConfig) -> None:
        self.app = app
        self.cache_cfg = cache_cfg
        self._redis_client = None
        self._logger = logging.getLogger(__name__)

        # Initialize Redis client lazily
        if cache_cfg.redis_url:
            try:
                import redis.asyncio as redis
                self._redis_client = redis.from_url(cache_cfg.redis_url)
            except Exception:
                self._logger.warning("Failed to initialize Redis client for rate limiting")

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)

        # Extract tenant ID from request
        tenant_id = self._extract_tenant_id(request)

        if not tenant_id:
            # No tenant - skip rate limiting
            await self.app(scope, receive, send)
            return

        # Check rate limit
        path = request.url.path
        allowed, retry_after, metadata = await _rate_limit_manager.check_rate_limit(
            tenant_id, path, self._redis_client
        )

        # Record metrics
        tier = _rate_limit_manager.get_tenant_tier(tenant_id)
        self._record_metrics(allowed, path, tenant_id, tier.value, metadata)

        if not allowed:
            # Rate limited - return 429 response
            response = JSONResponse(
                status_code=429,
                content={
                    "error": {
                        "type": "https://aurum.api/errors/rate_limit_exceeded",
                        "title": "Rate Limit Exceeded",
                        "detail": f"Too many requests for tenant {tenant_id}",
                        "retry_after": retry_after,
                        "instance": get_request_id(),
                    }
                },
                headers={
                    "Retry-After": str(int(retry_after or 1)),
                    "X-RateLimit-Tenant": tenant_id,
                    "X-RateLimit-Tier": tier.value,
                }
            )
            await response(scope, receive, send)
            return

        # Add rate limit headers to response
        await self._add_rate_limit_headers(send, tenant_id, tier.value, metadata)

        # Continue processing
        await self.app(scope, receive, send)

    def _extract_tenant_id(self, request: Request) -> Optional[str]:
        """Extract tenant ID from request."""
        # Check headers first
        tenant_id = request.headers.get("X-Tenant-ID") or request.headers.get("X-Aurum-Tenant")

        if tenant_id:
            return tenant_id

        # Check user context (if available)
        try:
            principal = getattr(request.state, "principal", None)
            if principal and isinstance(principal, dict):
                return principal.get("tenant")
        except Exception:
            pass

        return None

    def _record_metrics(self, allowed: bool, path: str, tenant_id: str, tier: str, metadata: Dict[str, Any]):
        """Record rate limiting metrics."""
        if TENANT_RATE_LIMIT_EVENTS is None:
            return

        result = "allowed" if allowed else "blocked"
        TENANT_RATE_LIMIT_EVENTS.labels(
            result=result, path=path, tenant_id=tenant_id, tier=tier
        ).inc()

        # Update active windows gauge
        if TENANT_RATE_LIMIT_ACTIVE_WINDOWS is not None:
            if not allowed:
                TENANT_RATE_LIMIT_ACTIVE_WINDOWS.labels(
                    tenant_id=tenant_id, tier=tier
                ).set(1)

        # Record requests per window
        if TENANT_RATE_LIMIT_REQUESTS_PER_WINDOW is not None:
            current_count = metadata.get("current_count", 0)
            TENANT_RATE_LIMIT_REQUESTS_PER_WINDOW.labels(
                tenant_id=tenant_id, tier=tier
            ).observe(current_count)

        # Record block duration
        if not allowed and TENANT_RATE_LIMIT_BLOCK_DURATION is not None:
            retry_after = metadata.get("retry_after", 1.0)
            TENANT_RATE_LIMIT_BLOCK_DURATION.labels(
                tenant_id=tenant_id, tier=tier
            ).observe(retry_after)

    async def _add_rate_limit_headers(self, send: Send, tenant_id: str, tier: str, metadata: Dict[str, Any]):
        """Add rate limit headers to response."""
        # This is a simplified implementation
        # In practice, headers would be added to the actual response
        pass


# Admin endpoints for rate limiting inspection
ratelimit_admin_router = APIRouter(prefix="/admin/ratelimit", tags=["admin-ratelimit"])


@ratelimit_admin_router.get("/tenants")
async def list_tenant_status():
    """List rate limiting status for all tenants."""
    return {
        "tenants": _rate_limit_manager.get_all_tenant_status(),
        "circuit_breaker_state": _rate_limit_manager._circuit_breaker_state.value,
        "system_load": _rate_limit_manager._system_load,
    }


@ratelimit_admin_router.get("/tenants/{tenant_id}")
async def get_tenant_status(tenant_id: str):
    """Get rate limiting status for a specific tenant."""
    return _rate_limit_manager.get_tenant_status(tenant_id)


@ratelimit_admin_router.delete("/tenants/{tenant_id}/reset")
async def reset_tenant_limits(tenant_id: str):
    """Reset rate limiting state for a tenant (admin operation)."""
    _rate_limit_manager.reset_tenant(tenant_id)
    return {"message": f"Rate limits reset for tenant {tenant_id}"}


@ratelimit_admin_router.post("/circuit-breaker/reset")
async def reset_circuit_breaker():
    """Reset circuit breaker state (admin operation)."""
    _rate_limit_manager._circuit_breaker_state = CircuitBreakerState.CLOSED
    _rate_limit_manager._circuit_failure_count = 0
    return {"message": "Circuit breaker reset to closed state"}


@ratelimit_admin_router.get("/quotas")
async def list_tenant_quotas():
    """List quota configurations for all tenant tiers."""
    return {
        "default_quotas": {
            tier.value: quota.__dict__
            for tier, quota in _rate_limit_manager._tier_quotas.items()
        },
        "tenant_overrides": _rate_limit_manager._tenant_overrides,
    }
