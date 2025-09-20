from __future__ import annotations

"""Configuration helpers for the Aurum API service."""

import os
from dataclasses import dataclass, field


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
    mode: str = "standalone"
    sentinel_endpoints: tuple[tuple[str, int], ...] = field(default_factory=tuple)
    sentinel_master: str | None = None
    cluster_nodes: tuple[str, ...] = field(default_factory=tuple)
    username: str | None = None
    password: str | None = None
    namespace: str = "aurum"
    db: int = 0

    @classmethod
    def from_env(cls) -> "CacheConfig":
        url = _env("AURUM_API_REDIS_URL")
        ttl = int(_env("AURUM_API_CACHE_TTL", "60") or 60)
        mode = (_env("AURUM_API_REDIS_MODE", "standalone") or "standalone").lower()
        namespace = _env("AURUM_API_CACHE_NAMESPACE", _env("AURUM_ENV", "aurum")) or "aurum"
        db = int(_env("AURUM_API_REDIS_DB", "0") or 0)

        sentinel_raw = _env("AURUM_API_REDIS_SENTINELS", "") or ""
        sentinel_endpoints: list[tuple[str, int]] = []
        for entry in sentinel_raw.split(","):
            host_port = entry.strip()
            if not host_port:
                continue
            host, _, port = host_port.partition(":")
            try:
                sentinel_endpoints.append((host, int(port or "26379")))
            except ValueError:
                continue

        cluster_raw = _env("AURUM_API_REDIS_CLUSTER_NODES", "") or ""
        cluster_nodes = tuple(node.strip() for node in cluster_raw.split(",") if node.strip())

        return cls(
            redis_url=url,
            ttl_seconds=ttl,
            mode=mode,
            sentinel_endpoints=tuple(sentinel_endpoints),
            sentinel_master=_env("AURUM_API_REDIS_SENTINEL_SERVICE"),
            cluster_nodes=cluster_nodes,
            username=_env("AURUM_API_REDIS_USERNAME"),
            password=_env("AURUM_API_REDIS_PASSWORD"),
            namespace=namespace,
            db=db,
        )


__all__ = ["TrinoConfig", "CacheConfig"]
