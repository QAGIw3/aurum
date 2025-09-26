"""Rate limiting functionality for the Aurum API."""

from .sliding_window import RateLimitConfig, RateLimitMiddleware, ratelimit_admin_router
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
    RateLimitRule,
    RateLimitAlgorithm,
    RateLimitState,
    RateLimitResult,
)
from .quota_manager import APIQuotaExceeded, TenantQuotaManager
from .concurrency_middleware import ConcurrencyMiddleware, local_diagnostics_router
from .redis_concurrency import diagnostics_router

__all__ = [
    # Core Rate Limiting
    "RateLimitConfig",
    "RateLimitMiddleware",
    "ratelimit_admin_router",
    # Enhanced Rate Limiting
    "create_rate_limit_manager",
    "EnhancedRateLimitMiddleware",
    "RateLimitManager",
    "RateLimitStorage",
    "InMemoryRateLimitStorage",
    "RedisRateLimitStorage",
    "QuotaTier",
    "Quota",
    "RateLimitRule",
    "RateLimitAlgorithm",
    "RateLimitState",
    "RateLimitResult",
    # Management
    "rate_limit_management_router",
    # Quota Management
    "APIQuotaExceeded",
    "TenantQuotaManager",
    # Concurrency
    "ConcurrencyMiddleware",
    "diagnostics_router",
    "local_diagnostics_router",
]
