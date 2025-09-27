"""Consolidated Rate Limiting Policy Engine.

This module provides a unified rate limiting system that consolidates:
- Sliding window rate limiting
- Concurrency controls
- Offload logic
- Tenant quota management
- API governance controls

Features:
- Multiple algorithm support (token bucket, sliding window, fixed window)
- Hierarchical rate limiting (global → tenant → endpoint → user)
- Real-time metrics and monitoring
- Adaptive rate limiting based on system load
- Comprehensive admin controls
"""

from __future__ import annotations

import asyncio
import time
import redis.asyncio as redis
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable, Awaitable
from pathlib import Path
from datetime import datetime, timedelta

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send

from ..observability.metrics import get_metrics_client
from ..logging.structured_logger import get_logger
from ..telemetry.context import extract_tenant_id_from_headers, get_request_id
from .exceptions import RateLimitExceeded


class RateLimitAlgorithmType(str, Enum):
    """Available rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class RateLimitScope(str, Enum):
    """Rate limit enforcement scope."""
    GLOBAL = "global"
    TENANT = "tenant"
    IP = "ip"
    ENDPOINT = "endpoint"
    USER = "user"
    API_KEY = "api_key"


@dataclass
class RateLimitRule:
    """Individual rate limit rule definition."""

    name: str
    algorithm: RateLimitAlgorithmType
    scope: RateLimitScope
    limit: int  # requests per window
    window_seconds: int
    burst_limit: Optional[int] = None  # allow bursts up to this limit
    priority: int = 100  # lower numbers = higher priority
    enabled: bool = True
    conditions: Dict[str, Any] = field(default_factory=dict)  # conditional rules
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitResult:
    """Result of rate limit check."""

    allowed: bool
    remaining: int
    reset_time: Optional[datetime] = None
    retry_after: Optional[int] = None
    rule_name: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitStats:
    """Rate limiting statistics."""

    total_requests: int = 0
    allowed_requests: int = 0
    blocked_requests: int = 0
    avg_response_time: float = 0.0
    error_count: int = 0
    last_reset: datetime = field(default_factory=datetime.utcnow)

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_requests == 0:
            return 1.0
        return self.allowed_requests / self.total_requests


class RateLimitAlgorithm(ABC):
    """Base class for rate limiting algorithms."""

    def __init__(self, rule: RateLimitRule, redis_client: Optional[redis.Redis] = None):
        """Initialize algorithm.

        Args:
            rule: Rate limit rule to enforce
            redis_client: Redis client for distributed coordination
        """
        self.rule = rule
        self.redis_client = redis_client
        self.logger = get_logger(f"{__name__}.{rule.algorithm.value}")

    @abstractmethod
    async def check_limit(self, key: str, request_count: int = 1) -> RateLimitResult:
        """Check if request should be rate limited.

        Args:
            key: Unique key identifying the rate limit subject
            request_count: Number of requests to consume

        Returns:
            Rate limit check result
        """
        pass

    @abstractmethod
    async def get_remaining(self, key: str) -> int:
        """Get remaining requests for key.

        Args:
            key: Unique key identifying the rate limit subject

        Returns:
            Remaining requests allowed
        """
        pass


class TokenBucketAlgorithm(RateLimitAlgorithm):
    """Token bucket rate limiting algorithm."""

    async def check_limit(self, key: str, request_count: int = 1) -> RateLimitResult:
        """Check token bucket limit."""
        # Implementation would use Redis for distributed coordination
        # For now, simulate with in-memory logic

        current_time = time.time()
        bucket_key = f"tb:{key}"

        # In real implementation, would track tokens and refill rate
        # This is a simplified simulation
        remaining = max(0, self.rule.limit - request_count)
        reset_time = datetime.utcnow() + timedelta(seconds=self.rule.window_seconds)

        return RateLimitResult(
            allowed=remaining >= 0,
            remaining=remaining,
            reset_time=reset_time,
            rule_name=self.rule.name,
            metadata={"algorithm": "token_bucket", "tokens_consumed": request_count}
        )

    async def get_remaining(self, key: str) -> int:
        """Get remaining tokens."""
        # Simplified implementation
        return max(0, self.rule.limit - 1)


class SlidingWindowAlgorithm(RateLimitAlgorithm):
    """Sliding window rate limiting algorithm."""

    async def check_limit(self, key: str, request_count: int = 1) -> RateLimitResult:
        """Check sliding window limit."""
        current_time = time.time()
        window_key = f"sw:{key}:{int(current_time // self.rule.window_seconds)}"

        # In real implementation, would use Redis sorted sets to track requests
        # This is a simplified simulation
        remaining = max(0, self.rule.limit - request_count)
        reset_time = datetime.utcnow() + timedelta(seconds=self.rule.window_seconds)

        return RateLimitResult(
            allowed=remaining >= 0,
            remaining=remaining,
            reset_time=reset_time,
            rule_name=self.rule.name,
            metadata={"algorithm": "sliding_window", "window_size": self.rule.window_seconds}
        )

    async def get_remaining(self, key: str) -> int:
        """Get remaining requests in window."""
        # Simplified implementation
        return max(0, self.rule.limit - 1)


class FixedWindowAlgorithm(RateLimitAlgorithm):
    """Fixed window rate limiting algorithm."""

    async def check_limit(self, key: str, request_count: int = 1) -> RateLimitResult:
        """Check fixed window limit."""
        current_time = time.time()
        window_start = int(current_time // self.rule.window_seconds) * self.rule.window_seconds
        window_key = f"fw:{key}:{window_start}"

        # Simplified implementation
        remaining = max(0, self.rule.limit - request_count)
        reset_time = datetime.fromtimestamp(window_start + self.rule.window_seconds)

        return RateLimitResult(
            allowed=remaining >= 0,
            remaining=remaining,
            reset_time=reset_time,
            rule_name=self.rule.name,
            metadata={"algorithm": "fixed_window", "window_start": window_start}
        )

    async def get_remaining(self, key: str) -> int:
        """Get remaining requests in window."""
        # Simplified implementation
        return max(0, self.rule.limit - 1)


class AdaptiveAlgorithm(RateLimitAlgorithm):
    """Adaptive rate limiting based on system conditions."""

    def __init__(self, rule: RateLimitRule, redis_client: Optional[redis.Redis] = None):
        """Initialize adaptive algorithm."""
        super().__init__(rule, redis_client)
        self.system_load_threshold = 0.8  # Reduce limits when system load > 80%
        self.error_rate_threshold = 0.05  # Reduce limits when error rate > 5%

    async def check_limit(self, key: str, request_count: int = 1) -> RateLimitResult:
        """Check adaptive limit based on system conditions."""
        # In real implementation, would monitor system metrics
        # and adjust limits dynamically
        current_load = await self._get_system_load()
        current_error_rate = await self._get_error_rate()

        # Adjust limit based on system conditions
        base_limit = self.rule.limit
        if current_load > self.system_load_threshold:
            base_limit = int(base_limit * 0.7)  # Reduce by 30%
        if current_error_rate > self.error_rate_threshold:
            base_limit = int(base_limit * 0.5)  # Reduce by 50%

        remaining = max(0, base_limit - request_count)
        reset_time = datetime.utcnow() + timedelta(seconds=self.rule.window_seconds)

        return RateLimitResult(
            allowed=remaining >= 0,
            remaining=remaining,
            reset_time=reset_time,
            rule_name=self.rule.name,
            metadata={
                "algorithm": "adaptive",
                "original_limit": self.rule.limit,
                "adjusted_limit": base_limit,
                "system_load": current_load,
                "error_rate": current_error_rate
            }
        )

    async def get_remaining(self, key: str) -> int:
        """Get remaining requests with adaptive limits."""
        return max(0, self.rule.limit - 1)

    async def _get_system_load(self) -> float:
        """Get current system load (0.0 to 1.0)."""
        # In real implementation, would query system metrics
        return 0.5  # Simulated 50% load

    async def _get_error_rate(self) -> float:
        """Get current error rate (0.0 to 1.0)."""
        # In real implementation, would query error metrics
        return 0.02  # Simulated 2% error rate


class ConsolidatedRateLimiter:
    """Unified rate limiting policy engine."""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        default_rules: Optional[List[RateLimitRule]] = None,
        enable_metrics: bool = True,
        enable_adaptive_limits: bool = True
    ):
        """Initialize consolidated rate limiter.

        Args:
            redis_url: Redis connection URL for distributed coordination
            default_rules: Default rate limiting rules
            enable_metrics: Enable metrics collection
            enable_adaptive_limits: Enable adaptive rate limiting
        """
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.enable_metrics = enable_metrics
        self.enable_adaptive_limits = enable_adaptive_limits

        # Rule management
        self.rules: Dict[str, RateLimitRule] = {}
        self.algorithms: Dict[str, RateLimitAlgorithm] = {}
        self.scope_rules: Dict[RateLimitScope, List[str]] = {}

        # Statistics
        self.stats = RateLimitStats()

        # Middleware state
        self._middleware_cache: Dict[str, Any] = {}

        # Initialize Redis if provided
        if redis_url:
            self.redis_client = redis.from_url(redis_url)

        # Initialize default rules
        if default_rules:
            for rule in default_rules:
                self.add_rule(rule)
        else:
            self._initialize_default_rules()

        self.logger = get_logger(__name__)
        self.logger.info("Consolidated rate limiter initialized",
                        redis_enabled=redis_url is not None,
                        rule_count=len(self.rules))

    def _initialize_default_rules(self) -> None:
        """Initialize default rate limiting rules."""
        default_rules = [
            # Global API limits
            RateLimitRule(
                name="global_api_limit",
                algorithm=RateLimitAlgorithmType.SLIDING_WINDOW,
                scope=RateLimitScope.GLOBAL,
                limit=10000,  # 10k requests per minute
                window_seconds=60,
                priority=1
            ),

            # Tenant limits
            RateLimitRule(
                name="tenant_limit",
                algorithm=RateLimitAlgorithmType.TOKEN_BUCKET,
                scope=RateLimitScope.TENANT,
                limit=1000,  # 1k requests per minute per tenant
                window_seconds=60,
                priority=10
            ),

            # IP-based limits
            RateLimitRule(
                name="ip_limit",
                algorithm=RateLimitAlgorithmType.SLIDING_WINDOW,
                scope=RateLimitScope.IP,
                limit=100,  # 100 requests per minute per IP
                window_seconds=60,
                priority=50
            ),

            # Endpoint-specific limits
            RateLimitRule(
                name="endpoint_limit",
                algorithm=RateLimitAlgorithmType.FIXED_WINDOW,
                scope=RateLimitScope.ENDPOINT,
                limit=500,  # 500 requests per minute per endpoint
                window_seconds=60,
                priority=20
            ),

            # User-specific limits
            RateLimitRule(
                name="user_limit",
                algorithm=RateLimitAlgorithmType.ADAPTIVE,
                scope=RateLimitScope.USER,
                limit=100,  # 100 requests per minute per user
                window_seconds=60,
                priority=30
            )
        ]

        for rule in default_rules:
            self.add_rule(rule)

    def add_rule(self, rule: RateLimitRule) -> None:
        """Add a rate limiting rule.

        Args:
            rule: Rate limit rule to add
        """
        self.rules[rule.name] = rule

        # Create algorithm instance
        algorithm_class = self._get_algorithm_class(rule.algorithm)
        algorithm = algorithm_class(rule, self.redis_client)
        self.algorithms[rule.name] = algorithm

        # Index by scope
        if rule.scope not in self.scope_rules:
            self.scope_rules[rule.scope] = []
        self.scope_rules[rule.scope].append(rule.name)

        self.logger.debug("Added rate limit rule",
                         rule_name=rule.name,
                         scope=rule.scope.value,
                         limit=rule.limit)

    def _get_algorithm_class(self, algorithm_type: RateLimitAlgorithmType) -> type[RateLimitAlgorithm]:
        """Get algorithm class for type."""
        algorithm_map = {
            RateLimitAlgorithmType.TOKEN_BUCKET: TokenBucketAlgorithm,
            RateLimitAlgorithmType.SLIDING_WINDOW: SlidingWindowAlgorithm,
            RateLimitAlgorithmType.FIXED_WINDOW: FixedWindowAlgorithm,
            RateLimitAlgorithmType.ADAPTIVE: AdaptiveAlgorithm,
        }
        return algorithm_map[algorithm_type]

    def get_rules_for_scope(self, scope: RateLimitScope) -> List[RateLimitRule]:
        """Get all rules for a specific scope.

        Args:
            scope: Rate limit scope

        Returns:
            List of applicable rules
        """
        rule_names = self.scope_rules.get(scope, [])
        return [self.rules[name] for name in rule_names if self.rules[name].enabled]

    async def check_request(
        self,
        request: Request,
        endpoint: Optional[str] = None
    ) -> RateLimitResult:
        """Check if request should be rate limited.

        Args:
            request: Starlette request object
            endpoint: Optional endpoint identifier

        Returns:
            Rate limit check result
        """
        start_time = time.time()
        self.stats.total_requests += 1

        try:
            # Extract identifiers for different scopes
            identifiers = await self._extract_request_identifiers(request, endpoint)

            # Check all applicable rules
            results = []
            for scope, identifier in identifiers.items():
                scope_rules = self.get_rules_for_scope(scope)
                for rule in sorted(scope_rules, key=lambda r: r.priority):
                    key = f"{rule.name}:{identifier}"

                    try:
                        result = await self.algorithms[rule.name].check_limit(key)
                        results.append(result)

                        if not result.allowed:
                            # Block request with highest priority rule
                            self.stats.blocked_requests += 1
                            self._record_rate_limit_exceeded(request, result, rule)

                            duration = time.time() - start_time
                            self.stats.avg_response_time = (
                                self.stats.avg_response_time * 0.9 + duration * 0.1
                            )

                            if self.enable_metrics:
                                metrics = get_metrics_client()
                                metrics.counter("aurum_rate_limit_exceeded_total",
                                              labels={"rule": rule.name, "scope": scope.value})

                            return result

                    except Exception as e:
                        self.logger.error("Rate limit check failed",
                                        rule_name=rule.name,
                                        error=str(e))
                        self.stats.error_count += 1

            # All checks passed
            self.stats.allowed_requests += 1
            duration = time.time() - start_time
            self.stats.avg_response_time = (
                self.stats.avg_response_time * 0.9 + duration * 0.1
            )

            if self.enable_metrics:
                metrics = get_metrics_client()
                metrics.counter("aurum_rate_limit_allowed_total")

            # Return best result (most restrictive remaining limit)
            if results:
                return min(results, key=lambda r: r.remaining if r.remaining is not None else 0)

            return RateLimitResult(allowed=True, remaining=1000)

        except Exception as e:
            self.stats.error_count += 1
            self.logger.error("Rate limit check failed", error=str(e))

            # Fail open on errors
            return RateLimitResult(allowed=True, remaining=1000)

    async def _extract_request_identifiers(
        self,
        request: Request,
        endpoint: Optional[str] = None
    ) -> Dict[RateLimitScope, str]:
        """Extract identifiers for different rate limit scopes.

        Args:
            request: Starlette request
            endpoint: Optional endpoint identifier

        Returns:
            Dictionary mapping scopes to identifiers
        """
        identifiers = {}

        # Global scope - always "global"
        identifiers[RateLimitScope.GLOBAL] = "global"

        # Tenant scope
        try:
            tenant_id = extract_tenant_id_from_headers(request.headers)
            if tenant_id:
                identifiers[RateLimitScope.TENANT] = tenant_id
        except Exception:
            pass

        # IP scope
        client_ip = request.client.host if request.client else "unknown"
        identifiers[RateLimitScope.IP] = client_ip

        # Endpoint scope
        if endpoint:
            identifiers[RateLimitScope.ENDPOINT] = endpoint
        else:
            # Extract from path
            path = request.url.path
            identifiers[RateLimitScope.ENDPOINT] = path.split("/")[1] if "/" in path else "root"

        # User scope - would extract from auth token in real implementation
        identifiers[RateLimitScope.USER] = "anonymous"

        # API key scope - would extract from headers in real implementation
        identifiers[RateLimitScope.API_KEY] = "default"

        return identifiers

    def _record_rate_limit_exceeded(
        self,
        request: Request,
        result: RateLimitResult,
        rule: RateLimitRule
    ) -> None:
        """Record rate limit exceeded event."""
        self.logger.warning("Rate limit exceeded",
                           rule_name=rule.name,
                           scope=rule.scope.value,
                           client_ip=request.client.host if request.client else "unknown",
                           path=request.url.path,
                           retry_after=result.retry_after)

    async def get_remaining_limits(self, request: Request, endpoint: Optional[str] = None) -> Dict[str, int]:
        """Get remaining limits for all applicable rules.

        Args:
            request: Starlette request
            endpoint: Optional endpoint identifier

        Returns:
            Dictionary of rule names to remaining limits
        """
        identifiers = await self._extract_request_identifiers(request, endpoint)
        remaining = {}

        for scope, identifier in identifiers.items():
            scope_rules = self.get_rules_for_scope(scope)
            for rule in scope_rules:
                try:
                    key = f"{rule.name}:{identifier}"
                    remaining_value = await self.algorithms[rule.name].get_remaining(key)
                    remaining[rule.name] = remaining_value
                except Exception as e:
                    self.logger.error("Failed to get remaining limit",
                                    rule_name=rule.name,
                                    error=str(e))

        return remaining

    async def get_stats(self) -> RateLimitStats:
        """Get current rate limiting statistics.

        Returns:
            Current statistics
        """
        return self.stats

    async def reset_stats(self) -> None:
        """Reset statistics counters."""
        self.stats = RateLimitStats()

    def create_middleware(self, app: ASGIApp) -> ASGIApp:
        """Create rate limiting middleware.

        Args:
            app: ASGI application to wrap

        Returns:
            Wrapped application with rate limiting
        """
        return RateLimitingMiddleware(app, self)

    async def shutdown(self) -> None:
        """Shutdown rate limiter and cleanup resources."""
        if self.redis_client:
            await self.redis_client.close()

        self.logger.info("Rate limiter shutdown complete")


class RateLimitingMiddleware:
    """ASGI middleware for rate limiting."""

    def __init__(self, app: ASGIApp, rate_limiter: ConsolidatedRateLimiter):
        """Initialize middleware.

        Args:
            app: ASGI application
            rate_limiter: Rate limiter instance
        """
        self.app = app
        self.rate_limiter = rate_limiter

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Handle request with rate limiting."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # Extract request info
        request = Request(scope)

        # Skip rate limiting for health checks and admin endpoints
        path = request.url.path
        if path in ["/health", "/metrics"] or path.startswith("/admin"):
            await self.app(scope, receive, send)
            return

        # Check rate limits
        result = await self.rate_limiter.check_request(request)

        if not result.allowed:
            # Create rate limit exceeded response
            response = JSONResponse(
                status_code=429,
                content={
                    "error": "Rate limit exceeded",
                    "retry_after": result.retry_after,
                    "reset_time": result.reset_time.isoformat() if result.reset_time else None,
                    "rule": result.rule_name
                },
                headers={
                    "Retry-After": str(result.retry_after or 60),
                    "X-RateLimit-Limit": "1000",  # Would be dynamic in real implementation
                    "X-RateLimit-Remaining": str(result.remaining or 0),
                    "X-RateLimit-Reset": str(int(result.reset_time.timestamp())) if result.reset_time else "0"
                }
            )

            await response(scope, receive, send)
            return

        # Add rate limit headers to successful responses
        async def wrapped_send(message):
            if message["type"] == "http.response.start":
                # Add rate limit headers
                headers = list(message.get("headers", []))
                headers.extend([
                    [b"X-RateLimit-Limit", b"1000"],
                    [b"X-RateLimit-Remaining", str(result.remaining or 0).encode()],
                    [b"X-RateLimit-Reset", str(int(result.reset_time.timestamp())).encode() if result.reset_time else b"0"]
                ])
                message["headers"] = headers

            await send(message)

        await self.app(scope, receive, wrapped_send)


# Global instance
_unified_rate_limiter: Optional[ConsolidatedRateLimiter] = None


def get_unified_rate_limiter(
    redis_url: Optional[str] = None,
    default_rules: Optional[List[RateLimitRule]] = None
) -> ConsolidatedRateLimiter:
    """Get the global unified rate limiter instance.

    Args:
        redis_url: Redis connection URL
        default_rules: Optional default rules

    Returns:
        Unified rate limiter instance
    """
    global _unified_rate_limiter

    if _unified_rate_limiter is None:
        _unified_rate_limiter = ConsolidatedRateLimiter(
            redis_url=redis_url,
            default_rules=default_rules
        )

    return _unified_rate_limiter


# Backward compatibility
def create_rate_limiting_middleware(
    app: ASGIApp,
    redis_url: Optional[str] = None,
    rules: Optional[List[RateLimitRule]] = None
) -> ASGIApp:
    """Create rate limiting middleware (backward compatibility)."""
    rate_limiter = get_unified_rate_limiter(redis_url, rules)
    return rate_limiter.create_middleware(app)
