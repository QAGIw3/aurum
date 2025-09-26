"""Deprecated sliding-window rate limiter import shim.

Use :mod:`aurum.api.rate_limiting` instead. This module remains to maintain
backwards compatibility for legacy imports.
"""

from __future__ import annotations

import warnings

from .sliding_window import RateLimitConfig, RateLimitMiddleware, ratelimit_admin_router

warnings.warn(
    "aurum.api.rate_limiting.ratelimit is deprecated; import from "
    "aurum.api.rate_limiting instead",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = ["RateLimitConfig", "RateLimitMiddleware", "ratelimit_admin_router"]
