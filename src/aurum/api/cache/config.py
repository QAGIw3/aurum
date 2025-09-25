from __future__ import annotations

"""Compatibility shim for cache configuration imports.

Historically, cache modules imported `CacheConfig` from `aurum.api.cache.config`.
The consolidated configuration now lives in `aurum.api.config`.
This module re-exports `CacheConfig` to maintain import compatibility.
"""

from ..config import CacheConfig  # re-export

__all__ = ["CacheConfig"]

