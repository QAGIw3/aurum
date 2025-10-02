from __future__ import annotations

import json
from typing import Any, Optional

import redis.asyncio as redis
from redis.asyncio.client import Redis

from .common.config import RedisSettings
from .ports import CacheRepository


class RedisCacheRepo(CacheRepository):
    """Thin Redis-based CacheRepository implementation.

    This provides a minimal key/value cache interface for services, distinct
    from higher-level route-aware CacheManager.
    """

    def __init__(self, settings: RedisSettings) -> None:
        self._settings = settings
        self._client: Optional[Redis] = None

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = redis.Redis(
                host=self._settings.host,
                port=self._settings.port,
                db=self._settings.db,
                password=self._settings.password,
                max_connections=self._settings.max_connections,
                socket_timeout=self._settings.socket_timeout,
                socket_connect_timeout=self._settings.socket_connect_timeout,
            )
        return self._client

    async def get(self, key: str) -> Optional[Any]:
        try:
            client = await self._get_client()
            raw = await client.get(key)
            if not raw:
                return None
            try:
                return json.loads(raw)
            except Exception:
                return raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
        except Exception:
            return None

    async def set(self, key: str, value: Any, *, ttl_seconds: Optional[int] = None) -> bool:
        try:
            client = await self._get_client()
            payload = json.dumps(value, default=str)
            if ttl_seconds and ttl_seconds > 0:
                await client.setex(key, ttl_seconds, payload)
            else:
                await client.set(key, payload)
            return True
        except Exception:
            return False

    async def invalidate(self, key_or_pattern: str) -> int:
        try:
            client = await self._get_client()
            # If pattern contains wildcard, resolve and delete
            if any(ch in key_or_pattern for ch in ["*", "?", "["]):
                keys = await client.keys(key_or_pattern)
                if keys:
                    await client.delete(*keys)
                    return len(keys)
                return 0
            else:
                return int(await client.delete(key_or_pattern) or 0)
        except Exception:
            return 0

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


