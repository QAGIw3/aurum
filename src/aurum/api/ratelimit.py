"""Deprecated entry point for rate limiting middleware (use aurum.api.rate_limiting)."""

from __future__ import annotations

import warnings

from .rate_limiting import RateLimitConfig, RateLimitMiddleware, ratelimit_admin_router

warnings.warn(
    "aurum.api.ratelimit is deprecated; import from aurum.api.rate_limiting instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["RateLimitConfig", "RateLimitMiddleware", "ratelimit_admin_router"]
