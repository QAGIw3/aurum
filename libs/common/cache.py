"""Redis caching layer with TTL management and golden queries."""
from __future__ import annotations

import json
import hashlib
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime

import redis.asyncio as redis
from redis.asyncio.client import Redis

from .config import RedisSettings, CacheSettings

logger = logging.getLogger(__name__)


class CacheManager:
    """Redis cache manager with TTL strategies and golden queries."""
    
    def __init__(self, redis_settings: RedisSettings, cache_settings: CacheSettings):
        self.redis_settings = redis_settings
        self.cache_settings = cache_settings
        self._client: Optional[Redis] = None
        self.namespace = "aurum"
        self._default_entry_ttl = max(1, int(cache_settings.medium_frequency_ttl))
        
        # Golden query list - queries with longer TTL
        self.golden_queries = {
            "curves_list_all",
            "dimensions_metadata", 
            "table_stats_*",
            "scenario_templates",
        }
    
    async def _get_client(self) -> Redis:
        """Get or create Redis client."""
        if self._client is None:
            if self.redis_settings.mode == "cluster":
                from redis.asyncio.cluster import RedisCluster
                self._client = RedisCluster(
                    host=self.redis_settings.host,
                    port=self.redis_settings.port,
                    password=self.redis_settings.password,
                    max_connections=self.redis_settings.max_connections,
                    socket_timeout=self.redis_settings.socket_timeout,
                    socket_connect_timeout=self.redis_settings.socket_connect_timeout,
                )
            else:
                self._client = redis.Redis(
                    host=self.redis_settings.host,
                    port=self.redis_settings.port,
                    db=self.redis_settings.db,
                    password=self.redis_settings.password,
                    max_connections=self.redis_settings.max_connections,
                    socket_timeout=self.redis_settings.socket_timeout,
                    socket_connect_timeout=self.redis_settings.socket_connect_timeout,
                )
        return self._client
    
    @staticmethod
    def build_cache_key(
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        *,
        version: str = "v2",
    ) -> str:
        """Generate a stable cache key for a route/query combination."""
        if query_params:
            query_str = json.dumps(query_params, sort_keys=True)
            query_hash = hashlib.md5(query_str.encode()).hexdigest()[:12]
        else:
            query_hash = "none"

        return f"aurum:{version}:{route}:{query_hash}"

    def _namespaced(self, key: str, *, namespace: Optional[str] = None) -> str:
        """Apply the manager namespace to a raw cache key."""

        prefix = (namespace or self.namespace).rstrip(":")
        if key.startswith(f"{prefix}:") or key.startswith("aurum:"):
            return key
        return f"{prefix}:{key}"
    
    def _get_ttl_for_key(self, cache_key: str) -> int:
        """Get TTL based on cache key and golden query rules."""
        
        # Check if it's a golden query
        for pattern in self.golden_queries:
            if pattern.endswith("*"):
                if pattern[:-1] in cache_key:
                    return self.cache_settings.golden_query_ttl
            elif pattern in cache_key:
                return self.cache_settings.golden_query_ttl
        
        # Determine TTL by route type
        if "metadata" in cache_key or "dimensions" in cache_key:
            return self.cache_settings.metadata_ttl
        elif "curves" in cache_key and "observations" not in cache_key:
            return self.cache_settings.curve_data_ttl
        elif "scenarios" in cache_key:
            return self.cache_settings.scenario_data_ttl
        elif "external" in cache_key:
            return self.cache_settings.external_data_ttl
        else:
            return self.cache_settings.medium_frequency_ttl
    
    async def get(
        self,
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
    ) -> Optional[Dict[str, Any]]:
        """Get cached data for route with query parameters."""
        cache_key = self.build_cache_key(route, query_params, version=version)
        
        try:
            client = await self._get_client()
            cached_data = await client.get(cache_key)
            
            if cached_data:
                data = json.loads(cached_data.decode('utf-8'))
                logger.debug(f"Cache HIT: {cache_key}")
                return data
            
            logger.debug(f"Cache MISS: {cache_key}")
            return None
            
        except Exception as e:
            logger.warning(f"Cache get error for {cache_key}: {e}")
            return None
    
    async def set(
        self,
        route: str,
        data: Dict[str, Any],
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
        ttl_override: Optional[int] = None,
    ) -> bool:
        """Set cached data with appropriate TTL."""
        cache_key = self.build_cache_key(route, query_params, version=version)
        ttl = ttl_override or self._get_ttl_for_key(cache_key)
        
        try:
            client = await self._get_client()
            
            # Add cache metadata
            cache_data = {
                "data": data,
                "cached_at": datetime.utcnow().isoformat(),
                "ttl": ttl,
                "route": route,
            }
            
            serialized_data = json.dumps(cache_data, default=str)
            await client.setex(cache_key, ttl, serialized_data)
            
            logger.debug(f"Cache SET: {cache_key} (TTL: {ttl}s)")
            return True
            
        except Exception as e:
            logger.warning(f"Cache set error for {cache_key}: {e}")
            return False
    
    async def set_negative_cache(
        self,
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
    ) -> bool:
        """Set negative cache for 404s to avoid repeated expensive queries."""
        cache_key = f"404:{self._generate_cache_key(route, query_params, version)}"
        
        try:
            client = await self._get_client()
            cache_data = {
                "status": "not_found",
                "cached_at": datetime.utcnow().isoformat(),
            }
            
            serialized_data = json.dumps(cache_data)
            await client.setex(cache_key, self.cache_settings.negative_cache_ttl, serialized_data)
            
            logger.debug(f"Negative cache SET: {cache_key}")
            return True
            
        except Exception as e:
            logger.warning(f"Negative cache set error for {cache_key}: {e}")
            return False
    
    async def is_negative_cached(
        self,
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
    ) -> bool:
        """Check if a route is negative cached (404)."""
        cache_key = f"404:{self._generate_cache_key(route, query_params, version)}"
        
        try:
            client = await self._get_client()
            exists = await client.exists(cache_key)
            return bool(exists)
            
        except Exception as e:
            logger.warning(f"Negative cache check error for {cache_key}: {e}")
            return False
    
    async def invalidate(
        self,
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
    ) -> bool:
        """Invalidate specific cache entry."""
        cache_key = self._generate_cache_key(route, query_params, version)
        
        try:
            client = await self._get_client()
            await client.delete(cache_key)
            
            # Also clear negative cache
            negative_key = f"404:{cache_key}"
            await client.delete(negative_key)
            
            logger.debug(f"Cache INVALIDATE: {cache_key}")
            return True
            
        except Exception as e:
            logger.warning(f"Cache invalidate error for {cache_key}: {e}")
            return False
    
    async def invalidate_pattern(self, pattern: str, namespace: Optional[str] = None) -> int:
        """Invalidate cache entries matching pattern."""
        try:
            client = await self._get_client()

            key_pattern = pattern
            if "*" not in key_pattern:
                key_pattern = f"{key_pattern}*"
            key_pattern = self._namespaced(key_pattern, namespace=namespace)

            # Find keys matching pattern
            keys = await client.keys(key_pattern)
            if keys:
                await client.delete(*keys)
                logger.info(
                    "Invalidated %s cache entries matching pattern", len(keys),
                    extra={"pattern": key_pattern},
                )
                return len(keys)

            return 0

        except Exception as e:
            logger.warning(f"Cache pattern invalidate error for {pattern}: {e}")
            return 0

    async def get_cache_entry(self, key: str, *, namespace: Optional[str] = None) -> Optional[Any]:
        """Retrieve a raw cache entry using the unified naming convention."""

        cache_key = self._namespaced(key, namespace=namespace)
        try:
            client = await self._get_client()
            payload = await client.get(cache_key)
        except Exception as exc:
            logger.warning(f"Cache entry get error for {cache_key}: {exc}")
            return None

        if payload is None:
            return None

        try:
            if isinstance(payload, bytes):
                return json.loads(payload.decode("utf-8"))
            return json.loads(payload)
        except (json.JSONDecodeError, AttributeError, UnicodeDecodeError):
            return payload

    async def set_cache_entry(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        *,
        namespace: Optional[str] = None,
    ) -> bool:
        """Store a raw payload with optional TTL override."""

        cache_key = self._namespaced(key, namespace=namespace)
        ttl = ttl_seconds or self._default_entry_ttl

        try:
            serialized = json.dumps(value, default=str)
        except (TypeError, ValueError):
            serialized = json.dumps(str(value))

        try:
            client = await self._get_client()
            await client.setex(cache_key, ttl, serialized)
            logger.debug(f"Cache entry SET: {cache_key} (TTL: {ttl}s)")
            return True
        except Exception as exc:
            logger.warning(f"Cache entry set error for {cache_key}: {exc}")
            return False

    async def get_curve_data(
        self,
        iso: str,
        market: str,
        location: str,
        asof: Optional[str] = None,
    ) -> Optional[Any]:
        """Return cached curve payload matching the legacy key format."""

        key_parts = [iso, market, location]
        if asof:
            key_parts.append(asof)
        key = "curve:" + ":".join(str(part) for part in key_parts)
        return await self.get_cache_entry(key)

    async def cache_curve_data(
        self,
        data: Any,
        iso: str,
        market: str,
        location: str,
        asof: Optional[str] = None,
        ttl: Optional[int] = None,
    ) -> bool:
        key_parts = [iso, market, location]
        if asof:
            key_parts.append(asof)
        key = "curve:" + ":".join(str(part) for part in key_parts)
        return await self.set_cache_entry(key, data, ttl_seconds=ttl or self.cache_settings.curve_data_ttl)

    async def get_metadata(self, metadata_type: str, **filters: Any) -> Optional[Any]:
        filter_str = ":".join(f"{k}:{filters[k]}" for k in sorted(filters)) if filters else ""
        key = f"metadata:{metadata_type}:{filter_str}" if filter_str else f"metadata:{metadata_type}"
        return await self.get_cache_entry(key)

    async def cache_metadata(
        self,
        data: Any,
        metadata_type: str,
        ttl: Optional[int] = None,
        **filters: Any,
    ) -> bool:
        filter_str = ":".join(f"{k}:{filters[k]}" for k in sorted(filters)) if filters else ""
        key = f"metadata:{metadata_type}:{filter_str}" if filter_str else f"metadata:{metadata_type}"
        return await self.set_cache_entry(key, data, ttl_seconds=ttl or self.cache_settings.metadata_ttl)

    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        try:
            client = await self._get_client()
            
            # Get Redis info
            info = await client.info()

            # Count keys by pattern
            namespace_prefix = f"{self.namespace}:*"
            aurum_keys = await client.keys(namespace_prefix)
            negative_keys = await client.keys("404:*")
            
            return {
                "redis_info": {
                    "connected_clients": info.get("connected_clients", 0),
                    "used_memory": info.get("used_memory", 0),
                    "used_memory_human": info.get("used_memory_human", "0B"),
                    "keyspace_hits": info.get("keyspace_hits", 0),
                    "keyspace_misses": info.get("keyspace_misses", 0),
                },
                "aurum_keys": len(aurum_keys),
                "negative_cache_keys": len(negative_keys),
                "hit_ratio": self._calculate_hit_ratio(info),
            }
            
        except Exception as e:
            logger.warning(f"Cache stats error: {e}")
            return {"error": str(e)}
    
    def _calculate_hit_ratio(self, redis_info: Dict[str, Any]) -> float:
        """Calculate cache hit ratio."""
        hits = redis_info.get("keyspace_hits", 0)
        misses = redis_info.get("keyspace_misses", 0)
        
        if hits + misses == 0:
            return 0.0
        
        return hits / (hits + misses)
    
    async def close(self):
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
