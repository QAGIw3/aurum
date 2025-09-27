"""Redis caching layer with TTL management and golden queries."""
from __future__ import annotations

import json
import hashlib
import logging
from typing import Any, Dict, Optional, List, Union
from datetime import datetime, timedelta

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
    
    def _generate_cache_key(
        self,
        route: str,
        query_params: Optional[Dict[str, Any]] = None,
        version: str = "v2",
    ) -> str:
        """Generate cache key from route and query parameters."""
        # Create hash from query parameters for consistent keys
        if query_params:
            query_str = json.dumps(query_params, sort_keys=True)
            query_hash = hashlib.md5(query_str.encode()).hexdigest()[:12]
        else:
            query_hash = "none"
        
        return f"aurum:{version}:{route}:{query_hash}"
    
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
        cache_key = self._generate_cache_key(route, query_params, version)
        
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
        cache_key = self._generate_cache_key(route, query_params, version)
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
    
    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        try:
            client = await self._get_client()
            
            # Find keys matching pattern
            keys = await client.keys(f"aurum:*{pattern}*")
            if keys:
                await client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} cache entries matching pattern: {pattern}")
                return len(keys)
            
            return 0
            
        except Exception as e:
            logger.warning(f"Cache pattern invalidate error for {pattern}: {e}")
            return 0
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics."""
        try:
            client = await self._get_client()
            
            # Get Redis info
            info = await client.info()
            
            # Count keys by pattern
            aurum_keys = await client.keys("aurum:*")
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