"""Backward compatible import hook for concurrency middleware."""

from __future__ import annotations

from .rate_limiting.concurrency_middleware import *  # noqa: F401,F403
