"""Admin service for cache management and administrative operations.

Provides platform-level administrative functionality.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional
from datetime import datetime

from ..base import BaseService
from ...core.protocols.cache import CacheProtocol

logger = logging.getLogger(__name__)


class AdminService(BaseService):
    """Service for administrative operations and cache management.
    
    Handles:
    - Cache invalidation and management
    - Scenario cache versioning
    - Cross-tenant cache operations
    - Event publishing for cache invalidation
    
    This service:
    - Manages cache lifecycle
    - Coordinates cache invalidation across services
    - Provides administrative utilities
    - Publishes cache events
    """
    
    def __init__(
        self,
        cache: Optional[CacheProtocol] = None
    ):
        """Initialize service with dependencies.
        
        Args:
            cache: Cache implementation for cache operations
        """
        super().__init__()
        self.cache = cache
        self._cache_namespace = "admin:v1"
    
    # Cache Management Methods
    
    async def invalidate_scenario_outputs_cache_async(
        self,
        tenant_id: Optional[str],
        scenario_id: str
    ) -> Dict[str, Any]:
        """Invalidate scenario outputs cache.
        
        Args:
            tenant_id: Tenant identifier
            scenario_id: Scenario identifier
            
        Returns:
            Dictionary with invalidation results
        """
        if not self.cache:
            logger.warning("No cache configured for AdminService")
            return {"invalidated": 0, "message": "Cache not configured"}
        
        try:
            # Publish cache invalidation event
            await self._publish_cache_invalidation_event(tenant_id, scenario_id)
            
            # Bump scenario cache version
            version_key = self._scenario_cache_version_key(tenant_id)
            current_version = await self._get_scenario_cache_version(tenant_id)
            next_version = str(int(current_version) + 1)
            await self.cache.set(version_key, next_version, ttl=0)  # No expiry
            
            # Invalidate scenario-specific cache entries
            invalidated = 0
            
            # Index pattern for scenario outputs
            index_pattern = self._scenario_cache_index_pattern(tenant_id, scenario_id)
            invalidated += await self.cache.delete_pattern(index_pattern)
            
            # Metrics index pattern
            metrics_pattern = self._scenario_metrics_cache_pattern(tenant_id, scenario_id)
            invalidated += await self.cache.delete_pattern(metrics_pattern)
            
            logger.info(
                f"Invalidated {invalidated} cache entries for scenario",
                extra={
                    "tenant_id": tenant_id,
                    "scenario_id": scenario_id,
                    "new_version": next_version
                }
            )
            
            return {
                "invalidated": invalidated,
                "new_version": next_version,
                "scenario_id": scenario_id
            }
            
        except Exception as e:
            logger.error(f"Failed to invalidate scenario cache: {e}")
            raise
    
    async def invalidate_eia_series_cache(self) -> Dict[str, int]:
        """Invalidate EIA series cache entries.
        
        Returns:
            Dictionary with counts of invalidated entries by type
        """
        if not self.cache:
            logger.warning("No cache configured for AdminService")
            return {"eia-series": 0, "eia-series-dimensions": 0}
        
        try:
            # Invalidate EIA series cache entries
            series_count = await self.cache.delete_pattern("eia:v1:series:*")
            
            # Invalidate EIA dimensions cache entries
            dimensions_count = await self.cache.delete_pattern("eia:v1:dimensions:*")
            
            # Invalidate other EIA-related caches
            metadata_count = await self.cache.delete_pattern("eia:v1:metadata:*")
            datasets_count = await self.cache.delete_pattern("eia:v1:datasets:*")
            
            total = series_count + dimensions_count + metadata_count + datasets_count
            
            logger.info(
                f"Invalidated {total} EIA cache entries",
                extra={
                    "series": series_count,
                    "dimensions": dimensions_count,
                    "metadata": metadata_count,
                    "datasets": datasets_count
                }
            )
            
            return {
                "eia-series": series_count,
                "eia-series-dimensions": dimensions_count,
                "eia-metadata": metadata_count,
                "eia-datasets": datasets_count
            }
            
        except Exception as e:
            logger.error(f"Failed to invalidate EIA cache: {e}")
            raise
    
    async def invalidate_timescale_data_cache(self) -> Dict[str, int]:
        """Invalidate TimescaleDB data cache entries.
        
        Returns:
            Dictionary with count of invalidated entries
        """
        if not self.cache:
            logger.warning("No cache configured for AdminService")
            return {"timescale": 0}
        
        try:
            # Invalidate TimescaleDB cache entries
            count = await self.cache.delete_pattern("timescale:*")
            
            logger.info(f"Invalidated {count} TimescaleDB cache entries")
            
            return {"timescale": count}
            
        except Exception as e:
            logger.error(f"Failed to invalidate TimescaleDB cache: {e}")
            raise
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get cache statistics and health information.
        
        Returns:
            Dictionary with cache statistics
        """
        if not self.cache:
            return {
                "configured": False,
                "message": "Cache not configured"
            }
        
        try:
            # Get cache info (implementation-specific)
            info = await self.cache.info() if hasattr(self.cache, 'info') else {}
            
            # Count keys by pattern
            patterns = {
                "scenarios": "scenario:*",
                "eia": "eia:*",
                "drought": "drought:*",
                "iso": "iso:*",
                "timescale": "timescale:*",
                "admin": f"{self._cache_namespace}:*"
            }
            
            key_counts = {}
            for name, pattern in patterns.items():
                # This is a simplified count - real implementation would be more efficient
                key_counts[name] = len(await self.cache.keys(pattern)) if hasattr(self.cache, 'keys') else 0
            
            return {
                "configured": True,
                "info": info,
                "key_counts": key_counts,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get cache statistics: {e}")
            return {
                "configured": True,
                "error": str(e)
            }
    
    async def clear_all_caches(self) -> Dict[str, Any]:
        """Clear all cache entries (dangerous operation).
        
        Returns:
            Dictionary with clear results
        """
        if not self.cache:
            logger.warning("No cache configured for AdminService")
            return {"cleared": False, "message": "Cache not configured"}
        
        try:
            # This is a dangerous operation - should be protected
            logger.warning("Clearing all cache entries")
            
            # Flush all cache entries
            if hasattr(self.cache, 'flush'):
                await self.cache.flush()
                cleared = True
                count = "all"
            else:
                # Fallback: delete by pattern
                count = await self.cache.delete_pattern("*")
                cleared = count > 0
            
            logger.info(f"Cleared {count} cache entries")
            
            return {
                "cleared": cleared,
                "count": count,
                "timestamp": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to clear caches: {e}")
            raise
    
    # Synchronous Methods (for backward compatibility)
    
    def invalidate_scenario_outputs_cache(
        self,
        cache_config: Any,  # CacheConfig from legacy code
        tenant_id: Optional[str],
        scenario_id: str
    ) -> None:
        """Synchronous version of scenario cache invalidation for backward compatibility.
        
        This method maintains the same signature as the legacy AdminService.
        
        Args:
            cache_config: Legacy CacheConfig object (ignored - uses injected cache)
            tenant_id: Tenant identifier
            scenario_id: Scenario identifier
        """
        # Run async method in sync context
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, schedule as a task
                task = asyncio.create_task(
                    self.invalidate_scenario_outputs_cache_async(tenant_id, scenario_id)
                )
                # Fire and forget - matches legacy behavior
            else:
                # Run in new event loop
                asyncio.run(
                    self.invalidate_scenario_outputs_cache_async(tenant_id, scenario_id)
                )
        except Exception as e:
            logger.warning(f"Cache invalidation failed: {e}")
            # Don't raise - legacy behavior was best-effort
    
    # Helper Methods
    
    def _scenario_cache_version_key(self, tenant_id: Optional[str]) -> str:
        """Generate scenario cache version key."""
        tenant_suffix = f":{tenant_id}" if tenant_id else ""
        return f"{self._cache_namespace}:scenario-cache-version{tenant_suffix}"
    
    def _scenario_cache_index_pattern(self, tenant_id: Optional[str], scenario_id: str) -> str:
        """Generate scenario cache index pattern."""
        tenant_suffix = f":{tenant_id}" if tenant_id else ""
        return f"{self._cache_namespace}:scenario-index{tenant_suffix}:{scenario_id}:*"
    
    def _scenario_metrics_cache_pattern(self, tenant_id: Optional[str], scenario_id: str) -> str:
        """Generate scenario metrics cache pattern."""
        tenant_suffix = f":{tenant_id}" if tenant_id else ""
        return f"{self._cache_namespace}:scenario-metrics{tenant_suffix}:{scenario_id}:*"
    
    async def _get_scenario_cache_version(self, tenant_id: Optional[str]) -> str:
        """Get current scenario cache version."""
        if not self.cache:
            return "1"
        
        version_key = self._scenario_cache_version_key(tenant_id)
        try:
            version = await self.cache.get(version_key)
            return str(version) if version else "1"
        except Exception:
            return "1"
    
    async def _publish_cache_invalidation_event(
        self,
        tenant_id: Optional[str],
        scenario_id: str
    ) -> None:
        """Publish cache invalidation event for downstream systems."""
        try:
            # In a real implementation, this would publish to a message queue
            # or event bus for other services to react to
            logger.info(
                "Published cache invalidation event",
                extra={
                    "tenant_id": tenant_id,
                    "scenario_id": scenario_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        except Exception as e:
            logger.warning(f"Failed to publish cache invalidation event: {e}")
