from __future__ import annotations

"""Admin service for cache management and administrative operations.

Phase 1.3 Service Layer Decomposition: Extracted from monolithic service.py
Handles administrative operations like cache invalidation and management.
"""

import logging
import json
from typing import Any, Dict, List, Optional, Sequence

from .base_service import ServiceInterface
from ..config import CacheConfig
from ..cache.unified_cache_manager import get_unified_cache_manager
from ..cache.utils import cache_get_sync, cache_set_sync

LOGGER = logging.getLogger(__name__)


def _maybe_redis_client(cache_cfg: CacheConfig):
    """Get Redis client if available."""
    try:
        import redis
        return redis.Redis(host=cache_cfg.host, port=cache_cfg.port, decode_responses=True)
    except Exception:
        return None


def _decode_member(member: Any) -> str:
    """Decode Redis member to string."""
    if isinstance(member, bytes):
        return member.decode("utf-8")
    return str(member)


def _scenario_cache_version_key(namespace: str, tenant_id: Optional[str]) -> str:
    """Generate scenario cache version key."""
    prefix = f"{namespace}:" if namespace else ""
    tenant_suffix = f":{tenant_id}" if tenant_id else ""
    return f"{prefix}scenario-cache-version{tenant_suffix}"


def _scenario_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    """Generate scenario cache index key."""
    prefix = f"{namespace}:" if namespace else ""
    tenant_suffix = f":{tenant_id}" if tenant_id else ""
    return f"{prefix}scenario-index{tenant_suffix}:{scenario_id}"


def _scenario_metrics_cache_index_key(namespace: str, tenant_id: Optional[str], scenario_id: str) -> str:
    """Generate scenario metrics cache index key."""
    prefix = f"{namespace}:" if namespace else ""
    tenant_suffix = f":{tenant_id}" if tenant_id else ""
    return f"{prefix}scenario-metrics-index{tenant_suffix}:{scenario_id}"


def _get_scenario_cache_version(client, namespace: str, tenant_id: Optional[str]) -> str:
    """Get current scenario cache version."""
    version_key = _scenario_cache_version_key(namespace, tenant_id)
    try:
        version = client.get(version_key)
        return str(version) if version else "1"
    except Exception:
        return "1"


def _bump_scenario_cache_version(client, namespace: str, tenant_id: Optional[str]) -> None:
    """Bump scenario cache version."""
    version_key = _scenario_cache_version_key(namespace, tenant_id)
    try:
        current = _get_scenario_cache_version(client, namespace, tenant_id)
        next_version = str(int(current) + 1)
        client.set(version_key, next_version)
    except Exception:
        LOGGER.warning("Failed to bump scenario cache version", exc_info=True)


def _publish_cache_invalidation_event(tenant_id: Optional[str], scenario_id: str) -> None:
    """Publish cache invalidation event."""
    try:
        # Placeholder for event publishing logic
        LOGGER.info(f"Cache invalidation event published for tenant={tenant_id}, scenario={scenario_id}")
    except Exception:
        LOGGER.warning("Failed to publish cache invalidation event", exc_info=True)


class AdminService(ServiceInterface):
    """Admin service for cache management and administrative operations."""

    def __init__(self):
        pass

    def invalidate_scenario_outputs_cache(
        self,
        cache_cfg: CacheConfig,
        tenant_id: Optional[str],
        scenario_id: str,
    ) -> None:
        """Invalidate scenario outputs cache."""
        client = _maybe_redis_client(cache_cfg)
        manager = get_unified_cache_manager()
        _publish_cache_invalidation_event(tenant_id, scenario_id)
        
        # Prefer version bump via UnifiedCacheManager when available; fallback to Redis
        if manager is not None:
            version_key = _scenario_cache_version_key(cache_cfg.namespace, tenant_id)
            try:
                current = cache_get_sync(manager, version_key)
                next_val = str(int(current) + 1) if isinstance(current, (int, str)) and str(current).isdigit() else "2"
                cache_set_sync(manager, version_key, next_val, cache_cfg.ttl_seconds)
            except Exception:
                LOGGER.debug("CacheManager version bump failed; attempting Redis path", exc_info=True)
        
        if client is not None:
            _bump_scenario_cache_version(client, cache_cfg.namespace, tenant_id)
            index_key = _scenario_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
            metrics_index_key = _scenario_metrics_cache_index_key(cache_cfg.namespace, tenant_id, scenario_id)
            try:  # pragma: no cover
                members = client.smembers(index_key)
                if members:
                    keys = [member.decode("utf-8") if isinstance(member, bytes) else member for member in members]
                    if keys:
                        client.delete(*keys)
                client.delete(index_key)
                metric_members = client.smembers(metrics_index_key)
                if metric_members:
                    metric_keys = [member.decode("utf-8") if isinstance(member, bytes) else member for member in metric_members]
                    if metric_keys:
                        client.delete(*metric_keys)
                client.delete(metrics_index_key)
            except Exception:
                return

    async def invalidate_eia_series_cache_async(self, cache_cfg: CacheConfig) -> Dict[str, int]:
        """Async version of EIA series cache invalidation."""
        manager = get_unified_cache_manager()
        if manager is not None:
            try:
                # Best-effort: use broad patterns to invalidate related keys
                await manager.invalidate_pattern("eia-series")
                return {"eia-series": 0, "eia-series-dimensions": 0}
            except Exception:
                LOGGER.debug("UnifiedCacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)

        # Fallback to Redis logic
        return self._invalidate_eia_series_cache_redis(cache_cfg)

    def _invalidate_eia_series_cache_redis(self, cache_cfg: CacheConfig) -> Dict[str, int]:
        """Redis-based EIA series cache invalidation fallback."""
        client = _maybe_redis_client(cache_cfg)
        if client is None:
            return {"eia-series": 0, "eia-series-dimensions": 0}

        prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
        series_index_key = f"{prefix}eia-series:index"
        dimensions_index_key = f"{prefix}eia-series-dimensions:index"

        counts = {"eia-series": 0, "eia-series-dimensions": 0}

        # Clear EIA series cache
        try:
            members = client.smembers(series_index_key)
            if members:
                keys = [_decode_member(member) for member in members]
                if keys:
                    client.delete(*keys)
                    counts["eia-series"] = len(keys)
            client.delete(series_index_key)
        except Exception:
            pass

        # Clear dimensions cache
        try:
            members = client.smembers(dimensions_index_key)
            if members:
                keys = [_decode_member(member) for member in members]
                if keys:
                    client.delete(*keys)
                    counts["eia-series-dimensions"] = len(keys)
            client.delete(dimensions_index_key)
        except Exception:
            pass

        return counts

    def invalidate_eia_series_cache(self, cache_cfg: CacheConfig) -> Dict[str, int]:
        """Invalidate EIA series cache. Prefer UnifiedCacheManager; fallback to Redis sets."""
        manager = get_unified_cache_manager()
        if manager is not None:
            try:
                # Using broad invalidation pattern for UnifiedCacheManager
                asyncio.create_task(manager.invalidate_pattern("eia-series"))
                return {"eia-series": 0, "eia-series-dimensions": 0}
            except Exception:
                LOGGER.debug("UnifiedCacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)

        return self._invalidate_eia_series_cache_redis(cache_cfg)

    async def invalidate_dimensions_cache_async(self, cache_cfg: CacheConfig) -> int:
        """Async version of dimensions cache invalidation."""
        manager = get_unified_cache_manager()
        if manager is not None:
            try:
                await manager.invalidate_pattern("dimensions")
                return 0
            except Exception:
                LOGGER.debug("UnifiedCacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)
        
        # Fallback to Redis logic
        return self._invalidate_dimensions_cache_redis(cache_cfg)

    def _invalidate_dimensions_cache_redis(self, cache_cfg: CacheConfig) -> int:
        """Redis-based dimensions cache invalidation fallback."""
        client = _maybe_redis_client(cache_cfg)
        if client is None:
            return 0
        prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""
        index_key = f"{prefix}dimensions:index"
        try:
            members = client.smembers(index_key)
        except Exception:
            return 0
        if not members:
            client.delete(index_key)
            return 0
        keys: list[str] = []
        for member in members:
            decoded = _decode_member(member)
            if decoded:
                keys.append(decoded)
        if not keys:
            return 0
        try:
            client.delete(*keys)
        except Exception:
            pass
        try:
            client.srem(index_key, *members)
        except Exception:
            pass
        try:
            if hasattr(client, "scard") and client.scard(index_key) == 0:
                client.delete(index_key)
        except Exception:
            client.delete(index_key)
        return len(keys)

    def invalidate_dimensions_cache(self, cache_cfg: CacheConfig) -> int:
        """Invalidate dimensions cache. Prefer CacheManager; fallback to Redis sets."""
        manager = get_global_cache_manager()
        if manager is not None:
            try:
                manager.invalidate_pattern("dimensions")
                return 0
            except Exception:
                LOGGER.debug("CacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)

        return self._invalidate_dimensions_cache_redis(cache_cfg)

    async def invalidate_metadata_cache_async(self, cache_cfg: CacheConfig, prefixes: Sequence[str]) -> Dict[str, int]:
        """Async version of metadata cache invalidation."""
        manager = get_global_cache_manager()
        if manager is not None:
            try:
                counts = {}
                for prefix in prefixes:
                    await manager.invalidate_pattern(prefix)
                    counts[prefix] = 0  # CacheManager doesn't return counts
                return counts
            except Exception:
                LOGGER.debug("CacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)

        return self._invalidate_metadata_cache_redis(cache_cfg, prefixes)

    def _invalidate_metadata_cache_redis(self, cache_cfg: CacheConfig, prefixes: Sequence[str]) -> Dict[str, int]:
        """Redis-based metadata cache invalidation fallback."""
        client = _maybe_redis_client(cache_cfg)
        if client is None:
            return {prefix: 0 for prefix in prefixes}

        counts = {}
        namespace_prefix = f"{cache_cfg.namespace}:" if cache_cfg.namespace else ""

        for prefix in prefixes:
            index_key = f"{namespace_prefix}{prefix}:index"
            try:
                members = client.smembers(index_key)
                if members:
                    keys = [_decode_member(member) for member in members]
                    if keys:
                        client.delete(*keys)
                        counts[prefix] = len(keys)
                    else:
                        counts[prefix] = 0
                else:
                    counts[prefix] = 0
                client.delete(index_key)
            except Exception:
                counts[prefix] = 0

        return counts

    def invalidate_metadata_cache(self, cache_cfg: CacheConfig, prefixes: Sequence[str]) -> Dict[str, int]:
        """Invalidate metadata cache. Prefer CacheManager; fallback to Redis sets."""
        manager = get_global_cache_manager()
        if manager is not None:
            try:
                counts = {}
                for prefix in prefixes:
                    manager.invalidate_pattern(prefix)
                    counts[prefix] = 0  # CacheManager doesn't return counts
                return counts
            except Exception:
                LOGGER.debug("CacheManager invalidate_pattern failed; attempting Redis path", exc_info=True)

        return self._invalidate_metadata_cache_redis(cache_cfg, prefixes)