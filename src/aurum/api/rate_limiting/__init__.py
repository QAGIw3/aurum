"""Rate limiting functionality for the Aurum API - Consolidated policy engine."""

# Primary consolidated interface (recommended)
from .consolidated_policy_engine import (
    ConsolidatedRateLimiter,
    RateLimitingMiddleware,
    RateLimitRule,
    RateLimitResult,
    RateLimitStats,
    RateLimitAlgorithm,
    RateLimitAlgorithmType,
    RateLimitScope,
    TokenBucketAlgorithm,
    SlidingWindowAlgorithm,
    FixedWindowAlgorithm,
    AdaptiveAlgorithm,
    get_unified_rate_limiter,
    create_rate_limiting_middleware
)

# Admin management interface
from .admin_router import router as rate_limiting_admin_router

# Legacy interfaces (backward compatibility)
from .sliding_window import RateLimitConfig, RateLimitMiddleware as LegacyRateLimitMiddleware, ratelimit_admin_router as legacy_admin_router
from . import exceptions
from .rate_limit_management import router as rate_limit_management_router
from .rate_limiting import (
    create_rate_limit_manager,
    RateLimitMiddleware as EnhancedRateLimitMiddleware,
    RateLimitManager,
    RateLimitStorage,
    InMemoryRateLimitStorage,
    RedisRateLimitStorage,
    QuotaTier,
    Quota,
    RateLimitRule as LegacyRateLimitRule,
    RateLimitAlgorithm as LegacyRateLimitAlgorithm,
    RateLimitState,
    RateLimitResult as LegacyRateLimitResult,
)
from .quota_manager import APIQuotaExceeded, TenantQuotaManager
from .concurrency_middleware import ConcurrencyMiddleware, local_diagnostics_router
from .redis_concurrency import diagnostics_router

__all__ = [
    # Primary consolidated interface (recommended)
    "ConsolidatedRateLimiter",
    "RateLimitingMiddleware",
    "RateLimitRule",
    "RateLimitResult",
    "RateLimitStats",
    "RateLimitAlgorithm",
    "RateLimitAlgorithmType",
    "RateLimitScope",
    "TokenBucketAlgorithm",
    "SlidingWindowAlgorithm",
    "FixedWindowAlgorithm",
    "AdaptiveAlgorithm",
    "get_unified_rate_limiter",
    "create_rate_limiting_middleware",

    # Admin interface
    "rate_limiting_admin_router",

    # Legacy interfaces (backward compatibility)
    "RateLimitConfig",
    "LegacyRateLimitMiddleware",
    "legacy_admin_router",
    "exceptions",
    "rate_limit_management_router",
    "create_rate_limit_manager",
    "EnhancedRateLimitMiddleware",
    "RateLimitManager",
    "RateLimitStorage",
    "InMemoryRateLimitStorage",
    "RedisRateLimitStorage",
    "QuotaTier",
    "Quota",
    "LegacyRateLimitRule",
    "LegacyRateLimitAlgorithm",
    "RateLimitState",
    "LegacyRateLimitResult",
    "APIQuotaExceeded",
    "TenantQuotaManager",
    "ConcurrencyMiddleware",
    "diagnostics_router",
    "local_diagnostics_router",
]
