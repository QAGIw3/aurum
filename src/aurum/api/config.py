from __future__ import annotations

"""Configuration helpers for the Aurum API service."""

from dataclasses import dataclass, field
from typing import List, Tuple

from aurum.core import AurumSettings
from aurum.core.settings import RedisMode


from enum import Enum


class TrinoCatalogType(str, Enum):
    """Trino catalog types for data lineage and access control."""
    RAW = "iceberg_raw"
    MARKET = "iceberg_market"


class TrinoAccessLevel(str, Enum):
    """Access levels for Trino catalogs."""
    READ_ONLY = "read_only"
    READ_WRITE = "read_write"
    ADMIN = "admin"


@dataclass(frozen=True)
class TrinoCatalogConfig:
    """Configuration for a specific Trino catalog."""
    host: str
    port: int
    user: str
    http_scheme: str
    catalog: str
    schema: str
    access_level: TrinoAccessLevel
    password: str | None = None
    lineage_tags: List[str] = field(default_factory=list)

    @classmethod
    def from_settings(
        cls,
        settings: AurumSettings,
        catalog_type: TrinoCatalogType,
        access_level: TrinoAccessLevel = TrinoAccessLevel.READ_WRITE,
        lineage_tags: List[str] | None = None,
    ) -> "TrinoCatalogConfig":
        trino = settings.trino
        return cls(
            host=trino.host,
            port=trino.port,
            user=trino.user,
            http_scheme=trino.http_scheme,
            catalog=catalog_type.value,
            schema=trino.database_schema,
            access_level=access_level,
            password=trino.password,
            lineage_tags=lineage_tags or [],
        )


# Backward compatibility - single catalog config
@dataclass(frozen=True)
class TrinoConfig:
    host: str = "localhost"
    port: int = 8080
    user: str = "aurum"
    http_scheme: str = "http"
    catalog: str = "iceberg"
    schema: str = "market"
    password: str | None = None

    @classmethod
    def from_settings(cls, settings: AurumSettings) -> "TrinoConfig":
        trino = settings.trino
        return cls(
            host=trino.host,
            port=trino.port,
            user=trino.user,
            http_scheme=trino.http_scheme,
            catalog=trino.catalog,
            schema=trino.database_schema,
            password=trino.password,
        )


@dataclass(frozen=True)
class CacheConfig:
    redis_url: str | None = None
    ttl_seconds: int = 60
    mode: str = RedisMode.STANDALONE.value
    sentinel_endpoints: Tuple[Tuple[str, int], ...] = field(default_factory=tuple)
    sentinel_master: str | None = None
    cluster_nodes: Tuple[str, ...] = field(default_factory=tuple)
    username: str | None = None
    password: str | None = None
    namespace: str = "aurum"
    db: int = 0
    socket_timeout: float = 1.5
    connect_timeout: float = 1.5

    @classmethod
    def from_settings(cls, settings: AurumSettings, *, ttl_override: int | None = None) -> "CacheConfig":
        redis = settings.redis
        ttl = ttl_override if ttl_override is not None else redis.ttl_seconds
        sentinel_pairs: list[Tuple[str, int]] = []
        for entry in redis.sentinel_endpoints:
            parsed = _parse_host_port(entry, default_port=26379)
            if parsed is not None:
                sentinel_pairs.append(parsed)
        cluster_nodes: Tuple[str, ...] = tuple(redis.cluster_nodes)
        return cls(
            redis_url=redis.url,
            ttl_seconds=ttl,
            mode=redis.mode.value if isinstance(redis.mode, RedisMode) else str(redis.mode),
            sentinel_endpoints=tuple(sentinel_pairs),
            sentinel_master=redis.sentinel_master,
            cluster_nodes=cluster_nodes,
            username=redis.username,
            password=redis.password,
            namespace=redis.namespace,
            db=redis.db,
            socket_timeout=redis.socket_timeout,
            connect_timeout=redis.connect_timeout,
        )


def _parse_host_port(value: str, *, default_port: int) -> Tuple[str, int] | None:
    token = value.strip()
    if not token:
        return None
    host, _, port_str = token.partition(":")
    if not host:
        return None
    try:
        port = int(port_str or default_port)
    except ValueError:
        return None
    return host, port


__all__ = ["TrinoCatalogType", "TrinoAccessLevel", "TrinoCatalogConfig", "TrinoConfig", "CacheConfig"]
