"""Enhanced rate limiting with endpoint-specific limits and burst alerting."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum

from ..telemetry.context import get_request_id, get_tenant_id, log_structured
from .audit import log_rate_limit_violation


class RateLimitStrategy(str, Enum):
    """Rate limiting strategies."""
    FIXED_WINDOW = "fixed_window"
    SLIDING_WINDOW = "sliding_window"
    TOKEN_BUCKET = "token_bucket"
    ADAPTIVE = "adaptive"


class AlertLevel(str, Enum):
    """Alert levels for rate limit violations."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RateLimitRule:
    """Rate limit rule for specific endpoints."""
    endpoint_pattern: str
    method: str = "*"  # * for all methods
    requests_per_minute: int = 60
    burst_limit: int = 10
    strategy: RateLimitStrategy = RateLimitStrategy.SLIDING_WINDOW
    alert_level: AlertLevel = AlertLevel.MEDIUM
    tenant_scoped: bool = True
    user_scoped: bool = False
    exclude_tenants: Set[str] = field(default_factory=set)
    exclude_users: Set[str] = field(default_factory=set)


@dataclass
class RateLimitEvent:
    """Rate limit violation event."""
    endpoint: str
    method: str
    client_id: str  # tenant_id or IP address
    timestamp: float
    request_count: int
    limit_exceeded: int
    alert_level: AlertLevel


class EnhancedRateLimiter:
    """Enhanced rate limiter with endpoint-specific rules and burst alerting."""

    def __init__(self):
        self._rules: List[RateLimitRule] = []
        self._violations: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self._alert_thresholds = {
            AlertLevel.LOW: 5,
            AlertLevel.MEDIUM: 3,
            AlertLevel.HIGH: 2,
            AlertLevel.CRITICAL: 1,
        }
        self._lock = asyncio.Lock()

    def add_rule(self, rule: RateLimitRule) -> None:
        """Add a rate limit rule."""
        self._rules.append(rule)

    def get_applicable_rules(self, endpoint: str, method: str) -> List[RateLimitRule]:
        """Get all rules applicable to the given endpoint and method."""
        applicable = []
        for rule in self._rules:
            if self._matches_rule(rule, endpoint, method):
                applicable.append(rule)
        return applicable

    def _matches_rule(self, rule: RateLimitRule, endpoint: str, method: str) -> bool:
        """Check if endpoint and method match the rule."""
        # Check method match
        if rule.method != "*" and rule.method != method:
            return False

        # Check endpoint pattern match
        # Simple pattern matching - could be enhanced with regex
        if rule.endpoint_pattern.endswith("*"):
            prefix = rule.endpoint_pattern[:-1]
            return endpoint.startswith(prefix)
        elif rule.endpoint_pattern.startswith("*"):
            suffix = rule.endpoint_pattern[1:]
            return endpoint.endswith(suffix)
        else:
            return rule.endpoint_pattern == endpoint

    async def check_rate_limit(
        self,
        endpoint: str,
        method: str,
        client_id: str,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> Tuple[bool, Optional[RateLimitEvent]]:
        """Check if request is within rate limits.

        Returns:
            Tuple of (allowed: bool, violation_event: Optional[RateLimitEvent])
        """
        async with self._lock:
            applicable_rules = self.get_applicable_rules(endpoint, method)

            for rule in applicable_rules:
                # Check if client is excluded
                if tenant_id and tenant_id in rule.exclude_tenants:
                    continue
                if user_id and user_id in rule.exclude_users:
                    continue

                # Determine client identifier for rate limiting
                limit_key = self._get_limit_key(rule, client_id, tenant_id, user_id)

                # Check rate limit for this rule
                violation = await self._check_single_rule(rule, limit_key, endpoint, method, client_id)

                if violation:
                    # Log the violation
                    await self._log_rate_limit_violation(violation)

                    # Check if we should alert
                    await self._check_alert_threshold(violation)

                    return False, violation

            return True, None

    def _get_limit_key(self, rule: RateLimitRule, client_id: str, tenant_id: Optional[str], user_id: Optional[str]) -> str:
        """Generate the key used for rate limiting."""
        if rule.user_scoped and user_id:
            return f"user:{user_id}"
        elif rule.tenant_scoped and tenant_id:
            return f"tenant:{tenant_id}"
        else:
            return f"ip:{client_id}"

    async def _check_single_rule(
        self,
        rule: RateLimitRule,
        limit_key: str,
        endpoint: str,
        method: str,
        client_id: str
    ) -> Optional[RateLimitEvent]:
        """Check rate limit for a single rule."""
        now = time.time()
        window_start = now - 60  # 1 minute window

        # Get or create violation history for this key
        violations = self._violations[limit_key]

        # Remove old violations
        while violations and violations[0].timestamp < window_start:
            violations.popleft()

        # Count recent violations
        recent_violations = len(violations)

        # Calculate current request rate
        # For simplicity, we'll use a basic sliding window approach
        if rule.strategy == RateLimitStrategy.SLIDING_WINDOW:
            return await self._check_sliding_window(rule, limit_key, endpoint, method, client_id, now)
        elif rule.strategy == RateLimitStrategy.TOKEN_BUCKET:
            return await self._check_token_bucket(rule, limit_key, endpoint, method, client_id, now)
        else:  # FIXED_WINDOW
            return await self._check_fixed_window(rule, limit_key, endpoint, method, client_id, now, recent_violations)

    async def _check_sliding_window(
        self,
        rule: RateLimitRule,
        limit_key: str,
        endpoint: str,
        method: str,
        client_id: str,
        now: float
    ) -> Optional[RateLimitEvent]:
        """Check rate limit using sliding window strategy."""
        # Simple implementation - in production this would use Redis or similar
        # For now, we'll just check recent violations
        violations = self._violations[limit_key]

        if len(violations) >= rule.requests_per_minute:
            return RateLimitEvent(
                endpoint=endpoint,
                method=method,
                client_id=client_id,
                timestamp=now,
                request_count=len(violations),
                limit_exceeded=rule.requests_per_minute,
                alert_level=rule.alert_level
            )
        return None

    async def _check_token_bucket(
        self,
        rule: RateLimitRule,
        limit_key: str,
        endpoint: str,
        method: str,
        client_id: str,
        now: float
    ) -> Optional[RateLimitEvent]:
        """Check rate limit using token bucket strategy."""
        # Simple token bucket implementation
        # In production, this would track tokens per client
        violations = self._violations[limit_key]

        # For token bucket, we allow some bursting
        burst_allowance = rule.burst_limit

        if len(violations) >= rule.requests_per_minute + burst_allowance:
            return RateLimitEvent(
                endpoint=endpoint,
                method=method,
                client_id=client_id,
                timestamp=now,
                request_count=len(violations),
                limit_exceeded=rule.requests_per_minute + burst_allowance,
                alert_level=rule.alert_level
            )
        return None

    async def _check_fixed_window(
        self,
        rule: RateLimitRule,
        limit_key: str,
        endpoint: str,
        method: str,
        client_id: str,
        now: float,
        recent_violations: int
    ) -> Optional[RateLimitEvent]:
        """Check rate limit using fixed window strategy."""
        if recent_violations >= rule.requests_per_minute:
            return RateLimitEvent(
                endpoint=endpoint,
                method=method,
                client_id=client_id,
                timestamp=now,
                request_count=recent_violations,
                limit_exceeded=rule.requests_per_minute,
                alert_level=rule.alert_level
            )
        return None

    async def record_request(
        self,
        endpoint: str,
        method: str,
        client_id: str,
        tenant_id: Optional[str] = None,
        user_id: Optional[str] = None
    ) -> None:
        """Record a successful request for rate limiting."""
        # This would increment counters in a real implementation
        # For now, we just log
        pass

    async def _log_rate_limit_violation(self, violation: RateLimitEvent) -> None:
        """Log a rate limit violation."""
        log_rate_limit_violation(
            endpoint=violation.endpoint,
            method=violation.method,
            ip_address=violation.client_id if "@" not in violation.client_id else "unknown",
            limit_type="requests_per_minute"
        )

    async def _check_alert_threshold(self, violation: RateLimitEvent) -> None:
        """Check if we should trigger an alert based on violation patterns."""
        violations = self._violations[violation.client_id]
        threshold = self._alert_thresholds[violation.alert_level]

        # Count violations in the last 5 minutes
        now = time.time()
        recent_count = sum(1 for v in violations if now - v.timestamp < 300)

        if recent_count >= threshold:
            log_structured(
                "error",
                "rate_limit_burst_detected",
                endpoint=violation.endpoint,
                method=violation.method,
                client_id=violation.client_id,
                violation_count=recent_count,
                alert_level=violation.alert_level,
                threshold=threshold
            )

    def get_violation_stats(self) -> Dict[str, int]:
        """Get violation statistics."""
        stats = {}
        for client_id, violations in self._violations.items():
            stats[client_id] = len(violations)
        return stats

    def clear_violations(self, client_id: Optional[str] = None) -> None:
        """Clear violation history."""
        if client_id:
            self._violations.pop(client_id, None)
        else:
            self._violations.clear()


# Global rate limiter instance
_rate_limiter: Optional[EnhancedRateLimiter] = None


def get_rate_limiter() -> EnhancedRateLimiter:
    """Get the global rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = EnhancedRateLimiter()
        _rate_limiter._setup_default_rules()
    return _rate_limiter


def _setup_default_rules(self) -> None:
    """Set up default rate limiting rules."""
    # Critical endpoints with strict limits
    self.add_rule(RateLimitRule(
        endpoint_pattern="/api/v2/admin/*",
        requests_per_minute=10,
        burst_limit=2,
        alert_level=AlertLevel.CRITICAL
    ))

    self.add_rule(RateLimitRule(
        endpoint_pattern="/api/v2/scenarios",
        method="POST",
        requests_per_minute=30,
        burst_limit=5,
        alert_level=AlertLevel.HIGH
    ))

    self.add_rule(RateLimitRule(
        endpoint_pattern="/api/v2/scenarios",
        method="DELETE",
        requests_per_minute=5,
        burst_limit=1,
        alert_level=AlertLevel.CRITICAL
    ))

    # Data ingestion endpoints
    self.add_rule(RateLimitRule(
        endpoint_pattern="/api/v2/eia/*",
        requests_per_minute=100,
        burst_limit=20,
        alert_level=AlertLevel.MEDIUM
    ))

    # General API endpoints
    self.add_rule(RateLimitRule(
        endpoint_pattern="/api/v2/*",
        requests_per_minute=300,
        burst_limit=50,
        alert_level=AlertLevel.LOW
    ))

    # Authentication endpoints (stricter limits)
    self.add_rule(RateLimitRule(
        endpoint_pattern="/auth/*",
        requests_per_minute=20,
        burst_limit=5,
        alert_level=AlertLevel.HIGH
    ))


# Apply monkey patch for setup
EnhancedRateLimiter._setup_default_rules = _setup_default_rules


async def check_endpoint_rate_limit(
    endpoint: str,
    method: str,
    client_ip: str,
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None
) -> bool:
    """Convenience function to check rate limits for an endpoint."""
    rate_limiter = get_rate_limiter()
    allowed, violation = await rate_limiter.check_rate_limit(
        endpoint=endpoint,
        method=method,
        client_id=client_ip,
        tenant_id=tenant_id,
        user_id=user_id
    )
    return allowed


async def record_endpoint_request(
    endpoint: str,
    method: str,
    client_ip: str,
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None
) -> None:
    """Convenience function to record a request for rate limiting."""
    rate_limiter = get_rate_limiter()
    await rate_limiter.record_request(
        endpoint=endpoint,
        method=method,
        client_id=client_ip,
        tenant_id=tenant_id,
        user_id=user_id
    )
