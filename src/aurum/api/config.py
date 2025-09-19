from __future__ import annotations

"""Configuration helpers for the Aurum API service."""

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str | None:
    val = os.getenv(name)
    return val if val is not None else default


@dataclass(frozen=True)
class TrinoConfig:
    host: str = "localhost"
    port: int = 8080
    user: str = "aurum"
    http_scheme: str = "http"

    @classmethod
    def from_env(cls) -> "TrinoConfig":
        host = _env("AURUM_API_TRINO_HOST", "localhost") or "localhost"
        port = int(_env("AURUM_API_TRINO_PORT", "8080") or 8080)
        user = _env("AURUM_API_TRINO_USER", "aurum") or "aurum"
        http_scheme = _env("AURUM_API_TRINO_SCHEME", "http") or "http"
        return cls(host=host, port=port, user=user, http_scheme=http_scheme)


@dataclass(frozen=True)
class CacheConfig:
    redis_url: str | None = None
    ttl_seconds: int = 60

    @classmethod
    def from_env(cls) -> "CacheConfig":
        url = _env("AURUM_API_REDIS_URL")
        ttl = int(_env("AURUM_API_CACHE_TTL", "60") or 60)
        return cls(redis_url=url, ttl_seconds=ttl)


__all__ = ["TrinoConfig", "CacheConfig"]

