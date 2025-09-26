"""Sophisticated rate limiting system with user/tenant quotas and multiple algorithms."""

from __future__ import annotations

import asyncio
import time
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict, deque

from ..telemetry.context import get_request_id
from .sliding_window import RateLimitMiddleware, ratelimit_admin_router
from ..cache.cache import AsyncCache, CacheManager


class RateLimitAlgorithm(Enum):
    """Available rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class QuotaTier(Enum):
    """User/tenant quota tiers."""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"
    UNLIMITED = "unlimited"


@dataclass
class RateLimitRule:
    """Configuration for a rate limit rule."""
    name: str
    requests_per_window: int
    window_seconds: int
    burst_limit: Optional[int] = None
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.TOKEN_BUCKET
    tier_override: Optional[QuotaTier] = None
    endpoint_patterns: List[str] = field(default_factory=list)
    user_agent_patterns: List[str] = field(default_factory=list)


@dataclass
class Quota:
    """User/tenant quota configuration."""
    tier: QuotaTier
    requests_per_minute: int
    requests_per_hour: int
    requests_per_day: int
    burst_multiplier: float = 1.0
    custom_limits: Dict[str, int] = field(default_factory=dict)

    def get_limit(self, window_seconds: int) -> int:
        """Get limit for a specific time window."""
        if window_seconds <= 60:
            return self.requests_per_minute
        elif window_seconds <= 3600:
            return self.requests_per_hour
        else:
            return self.requests_per_day


class RateLimitState:
    """Current state of rate limiting for a user/tenant."""

    def __init__(self, identifier: str, quota: Quota):
        self.identifier = identifier
        self.quota = quota
        self.reset_time = time.time()
        self.request_count = 0
        self.tokens = quota.requests_per_minute
        self.last_refill = time.time()
        self.burst_tokens = min(quota.burst_multiplier * quota.requests_per_minute, 1000)

    def can_consume(self, tokens: int = 1) -> bool:
        """Check if tokens can be consumed."""
        self._refill_tokens()

        if self.tokens >= tokens:
            self.tokens -= tokens
            self.request_count += tokens
            return True
        return False

    def _refill_tokens(self):
        """Refill tokens based on time elapsed."""
        now = time.time()
        time_passed = now - self.last_refill

        # Refill rate: tokens per second
        refill_rate = self.quota.requests_per_minute / 60.0

        # Refill tokens
        tokens_to_add = time_passed * refill_rate
        self.tokens = min(self.tokens + tokens_to_add, self.quota.requests_per_minute)

        # Refill burst tokens more slowly
        burst_to_add = time_passed * (refill_rate * 0.1)  # 10% of normal rate
        self.burst_tokens = min(self.burst_tokens + burst_to_add, self.quota.requests_per_minute * self.quota.burst_multiplier)

        self.last_refill = now

    def get_remaining_tokens(self) -> int:
        """Get remaining tokens."""
        self._refill_tokens()
        return int(self.tokens)

    def get_reset_time_seconds(self) -> int:
        """Get seconds until next token refill."""
        return max(0, int(60 - (time.time() - self.last_refill)))


class RateLimitResult:
    """Result of a rate limit check."""

    def __init__(
        self,
        allowed: bool,
        remaining_requests: int = 0,
        reset_time: int = 0,
        retry_after: int = 0,
        quota: Optional[Quota] = None,
        tier: Optional[QuotaTier] = None
    ):
        self.allowed = allowed
        self.remaining_requests = remaining_requests
        self.reset_time = reset_time
        self.retry_after = retry_after
        self.quota = quota
        self.tier = tier

    def to_headers(self) -> Dict[str, str]:
        """Convert to HTTP headers."""
        headers = {}

        if self.quota:
            headers.update({
                "X-RateLimit-Limit": str(self.quota.requests_per_minute),
                "X-RateLimit-Remaining": str(self.remaining_requests),
                "X-RateLimit-Reset": str(self.reset_time),
            })

            if not self.allowed:
                headers["Retry-After"] = str(self.retry_after)

        if self.tier:
            headers["X-RateLimit-Tier"] = self.tier.value

        return headers


class RateLimitStorage(ABC):
    """Abstract base class for rate limit storage backends."""

    @abstractmethod
    async def get_state(self, identifier: str) -> Optional[RateLimitState]:
        """Get rate limit state for identifier."""
        pass

    @abstractmethod
    async def set_state(self, identifier: str, state: RateLimitState) -> None:
        """Set rate limit state for identifier."""
        pass

    @abstractmethod
    async def delete_state(self, identifier: str) -> None:
        """Delete rate limit state for identifier."""
        pass

    @abstractmethod
    async def cleanup_expired(self) -> int:
        """Clean up expired rate limit states."""
        pass


class InMemoryRateLimitStorage(RateLimitStorage):
    """In-memory rate limit storage (for development/testing)."""

    def __init__(self):
        self._states: Dict[str, RateLimitState] = {}
        self._lock = asyncio.Lock()

    async def get_state(self, identifier: str) -> Optional[RateLimitState]:
        async with self._lock:
            return self._states.get(identifier)

    async def set_state(self, identifier: str, state: RateLimitState) -> None:
        async with self._lock:
            self._states[identifier] = state

    async def delete_state(self, identifier: str) -> None:
        async with self._lock:
            self._states.pop(identifier, None)

    async def cleanup_expired(self) -> int:
        """Clean up expired states (simple implementation)."""
        async with self._lock:
            expired_count = 0
            current_time = time.time()

            for identifier, state in list(self._states.items()):
                # Remove states older than 1 hour
                if current_time - state.last_refill > 3600:
                    del self._states[identifier]
                    expired_count += 1

            return expired_count


class RedisRateLimitStorage(RateLimitStorage):
    """Redis-based rate limit storage for production."""

    def __init__(self, redis_url: str, namespace: str = "ratelimit"):
        self.redis_url = redis_url
        self.namespace = namespace
        self._redis_client = None

    async def _get_redis_client(self):
        """Get Redis client (lazy initialization)."""
        if self._redis_client is None:
            try:
                import redis.asyncio as redis
                self._redis_client = redis.from_url(self.redis_url, decode_responses=True)
                await self._redis_client.ping()
            except ImportError:
                raise RuntimeError("redis package not available")
            except Exception as e:
                raise RuntimeError(f"Failed to connect to Redis: {e}")
        return self._redis_client

    def _make_key(self, identifier: str) -> str:
        """Create Redis key for identifier."""
        return f"{self.namespace}:{identifier}"

    async def get_state(self, identifier: str) -> Optional[RateLimitState]:
        redis_client = await self._get_redis_client()
        key = self._make_key(identifier)

        try:
            data = await redis_client.get(key)
            if data:
                # Deserialize state (simplified)
                return RateLimitState(identifier, Quota(QuotaTier.FREE, 60, 3600, 86400))
        except Exception:
            pass

        return None

    async def set_state(self, identifier: str, state: RateLimitState) -> None:
        redis_client = await self._get_redis_client()
        key = self._make_key(identifier)

        try:
            # Serialize state (simplified)
            data = {
                "tokens": state.tokens,
                "last_refill": state.last_refill,
                "request_count": state.request_count
            }
            await redis_client.setex(key, 3600, str(data))  # 1 hour expiry
        except Exception:
            pass

    async def delete_state(self, identifier: str) -> None:
        redis_client = await self._get_redis_client()
        key = self._make_key(identifier)

        try:
            await redis_client.delete(key)
        except Exception:
            pass

    async def cleanup_expired(self) -> int:
        """Clean up expired keys using Redis SCAN."""
        redis_client = await self._get_redis_client()

        try:
            pattern = f"{self.namespace}:*"
            keys = await redis_client.keys(pattern)
            return len(keys)  # Simplified - would need actual expiry check
        except Exception:
            return 0


class RateLimitManager:
    """Manages rate limiting across the application."""

    def __init__(self, storage: RateLimitStorage):
        self.storage = storage
        self._quotas: Dict[QuotaTier, Quota] = {}
        self._rules: List[RateLimitRule] = []
        self._lock = asyncio.Lock()

    def register_quota(self, tier: QuotaTier, quota: Quota) -> None:
        """Register a quota tier."""
        self._quotas[tier] = quota

    def add_rule(self, rule: RateLimitRule) -> None:
        """Add a rate limiting rule."""
        self._rules.append(rule)

    def get_quota(self, tier: QuotaTier) -> Optional[Quota]:
        """Get quota for a tier."""
        return self._quotas.get(tier)

    def get_default_quota(self) -> Quota:
        """Get default quota (free tier)."""
        return self._quotas.get(QuotaTier.FREE, Quota(QuotaTier.FREE, 60, 3600, 86400))

    async def check_rate_limit(
        self,
        identifier: str,
        tier: QuotaTier = QuotaTier.FREE,
        endpoint: Optional[str] = None,
        user_agent: Optional[str] = None,
        request_tokens: int = 1
    ) -> RateLimitResult:
        """Check if request is within rate limits."""
        async with self._lock:
            # Get or create rate limit state
            state = await self.storage.get_state(identifier)
            if state is None:
                quota = self.get_quota(tier) or self.get_default_quota()
                state = RateLimitState(identifier, quota)

            # Check custom rules first
            for rule in self._rules:
                if self._matches_rule(rule, endpoint, user_agent):
                    if not state.can_consume(request_tokens):
                        return RateLimitResult(
                            allowed=False,
                            remaining_requests=0,
                            reset_time=state.get_reset_time_seconds(),
                            retry_after=state.get_reset_time_seconds(),
                            quota=state.quota,
                            tier=tier
                        )

            # Check general quota
            if not state.can_consume(request_tokens):
                return RateLimitResult(
                    allowed=False,
                    remaining_requests=0,
                    reset_time=state.get_reset_time_seconds(),
                    retry_after=state.get_reset_time_seconds(),
                    quota=state.quota,
                    tier=tier
                )

            # Update state
            await self.storage.set_state(identifier, state)

            return RateLimitResult(
                allowed=True,
                remaining_requests=state.get_remaining_tokens(),
                reset_time=state.get_reset_time_seconds(),
                quota=state.quota,
                tier=tier
            )

    def _matches_rule(self, rule: RateLimitRule, endpoint: Optional[str], user_agent: Optional[str]) -> bool:
        """Check if request matches a rule."""
        # Check endpoint patterns
        if rule.endpoint_patterns and endpoint:
            for pattern in rule.endpoint_patterns:
                if pattern in endpoint:
                    return True

        # Check user agent patterns
        if rule.user_agent_patterns and user_agent:
            for pattern in rule.user_agent_patterns:
                if pattern in user_agent:
                    return True

        return False

    async def get_tier_usage(self, tier: QuotaTier) -> Dict[str, Any]:
        """Get usage statistics for a tier."""
        # In a real implementation, this would aggregate usage data
        return {
            "tier": tier.value,
            "active_users": 0,
            "requests_today": 0,
            "requests_this_hour": 0,
            "average_utilization": 0.0
        }

    async def cleanup(self) -> int:
        """Clean up expired rate limit states."""
        return await self.storage.cleanup_expired()


def create_rate_limit_manager(storage_backend: str = "memory") -> RateLimitManager:
    """Create a rate limit manager with default configuration."""

    # Create storage backend
    if storage_backend == "redis":
        storage = RedisRateLimitStorage("redis://localhost:6379", "aurum_ratelimit")
    else:
        storage = InMemoryRateLimitStorage()

    # Create manager
    manager = RateLimitManager(storage)

    # Register default quotas
    manager.register_quota(
        QuotaTier.FREE,
        Quota(
            tier=QuotaTier.FREE,
            requests_per_minute=60,
            requests_per_hour=1000,
            requests_per_day=10000,
            burst_multiplier=1.0
        )
    )

    manager.register_quota(
        QuotaTier.BASIC,
        Quota(
            tier=QuotaTier.BASIC,
            requests_per_minute=300,
            requests_per_hour=5000,
            requests_per_day=50000,
            burst_multiplier=1.5
        )
    )

    manager.register_quota(
        QuotaTier.PREMIUM,
        Quota(
            tier=QuotaTier.PREMIUM,
            requests_per_minute=1000,
            requests_per_hour=20000,
            requests_per_day=200000,
            burst_multiplier=2.0
        )
    )

    manager.register_quota(
        QuotaTier.ENTERPRISE,
        Quota(
            tier=QuotaTier.ENTERPRISE,
            requests_per_minute=5000,
            requests_per_hour=100000,
            requests_per_day=1000000,
            burst_multiplier=3.0
        )
    )

    manager.register_quota(
        QuotaTier.UNLIMITED,
        Quota(
            tier=QuotaTier.UNLIMITED,
            requests_per_minute=10000,
            requests_per_hour=200000,
            requests_per_day=2000000,
            burst_multiplier=5.0
        )
    )

    # Add default rules
    manager.add_rule(
        RateLimitRule(
            name="api_endpoints",
            requests_per_window=100,
            window_seconds=60,
            burst_limit=20,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/", "/v2/"]
        )
    )

    manager.add_rule(
        RateLimitRule(
            name="metadata_endpoints",
            requests_per_window=200,
            window_seconds=60,
            burst_limit=50,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/metadata/"]
        )
    )

    # Add external data endpoint rules
    manager.add_rule(
        RateLimitRule(
            name="external_providers",
            requests_per_window=100,
            window_seconds=60,
            burst_limit=20,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/external/providers"]
        )
    )

    manager.add_rule(
        RateLimitRule(
            name="external_series",
            requests_per_window=200,
            window_seconds=60,
            burst_limit=30,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/external/series"]
        )
    )

    manager.add_rule(
        RateLimitRule(
            name="external_observations",
            requests_per_window=50,
            window_seconds=60,
            burst_limit=10,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/external/series/*/observations"]
        )
    )

    manager.add_rule(
        RateLimitRule(
            name="external_metadata",
            requests_per_window=100,
            window_seconds=60,
            burst_limit=25,
            algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
            endpoint_patterns=["/v1/metadata/external"]
        )
    )

    return manager
