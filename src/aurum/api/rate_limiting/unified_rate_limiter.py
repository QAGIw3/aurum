"""Compatibility shim for legacy unified_rate_limiter API.

The consolidated policy engine (``consolidated_policy_engine``) now owns the
source of truth for rate limiting.  This module keeps the historical import
paths working while gently nudging callers toward the consolidated surface.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

from . import consolidated_policy_engine as _engine
from .consolidated_policy_engine import (
    ConsolidatedRateLimiter,
    RateLimitAlgorithmType,
    RateLimitResult,
    RateLimitRule,
    RateLimitScope,
    RateLimitingMiddleware,
    create_rate_limiting_middleware,
    get_unified_rate_limiter as _engine_get_unified_rate_limiter,
)


# Expose the consolidated limiter under the historical name for callers that
# still refer to ``UnifiedRateLimiter`` directly.
UnifiedRateLimiter = ConsolidatedRateLimiter


@dataclass
class RateLimitPolicy:
    """Legacy policy definition that maps onto ``RateLimitRule``.

    Only a subset of fields were honoured in practice.  We keep them so that
    configuration code relying on the older dataclass can continue to operate
    while we migrate to the consolidated rules API.
    """

    name: str
    algorithm: RateLimitAlgorithmType = RateLimitAlgorithmType.TOKEN_BUCKET
    scope: RateLimitScope = RateLimitScope.GLOBAL
    requests_per_second: int = 10
    burst_size: Optional[int] = None
    window_seconds: int = 60
    enabled: bool = True
    endpoint_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)
    priority: int = 100
    strict_enforcement: bool = True
    max_concurrency: Optional[int] = None
    queue_timeout_seconds: Optional[float] = None

    def to_rule(self) -> RateLimitRule:
        """Translate the legacy policy into a consolidated rule."""
        limit = max(self.requests_per_second * max(self.window_seconds, 1), 0)
        metadata: Dict[str, Any] = {
            "endpoint_patterns": list(self.endpoint_patterns),
            "exclude_patterns": list(self.exclude_patterns),
            "strict_enforcement": self.strict_enforcement,
            "max_concurrency": self.max_concurrency,
            "queue_timeout_seconds": self.queue_timeout_seconds,
        }

        return RateLimitRule(
            name=self.name,
            algorithm=self.algorithm,
            scope=self.scope,
            limit=limit,
            window_seconds=max(self.window_seconds, 1),
            burst_limit=self.burst_size,
            priority=self.priority,
            enabled=self.enabled,
            metadata={
                k: v
                for k, v in metadata.items()
                if v is not None and v != []
            },
        )


def _normalise_rules(
    rules: Optional[Iterable[Union[RateLimitRule, RateLimitPolicy]]]
) -> Optional[List[RateLimitRule]]:
    if rules is None:
        return None

    normalised: List[RateLimitRule] = []
    for rule in rules:
        if isinstance(rule, RateLimitRule):
            normalised.append(rule)
        else:
            normalised.append(rule.to_rule())
    return normalised


def get_unified_rate_limiter(
    redis_url: Optional[str] = None,
    default_rules: Optional[Iterable[Union[RateLimitRule, RateLimitPolicy]]] = None,
) -> UnifiedRateLimiter:
    """Backwards-compatible entry point to the consolidated limiter."""
    warnings.warn(
        "aurum.api.rate_limiting.unified_rate_limiter is deprecated; import "
        "from aurum.api.rate_limiting instead",
        DeprecationWarning,
        stacklevel=2,
    )

    normalised_rules = _normalise_rules(default_rules)
    return _engine_get_unified_rate_limiter(redis_url, normalised_rules)


def set_unified_rate_limiter(rate_limiter: UnifiedRateLimiter) -> None:
    """Retain the legacy setter for compatibility with old bootstrap code."""
    warnings.warn(
        "set_unified_rate_limiter is deprecated; prefer configuring the "
        "consolidated engine directly",
        DeprecationWarning,
        stacklevel=2,
    )
    _engine._unified_rate_limiter = rate_limiter


__all__ = [
    "RateLimitAlgorithmType",
    "RateLimitScope",
    "RateLimitPolicy",
    "RateLimitResult",
    "UnifiedRateLimiter",
    "RateLimitingMiddleware",
    "create_rate_limiting_middleware",
    "get_unified_rate_limiter",
    "set_unified_rate_limiter",
]
