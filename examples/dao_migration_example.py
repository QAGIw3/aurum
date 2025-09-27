"""Example showing how to migrate DAO cache usage to UnifiedCacheManager.

This example demonstrates updating DAO classes to use the unified cache system
instead of scattered cache implementations.
"""

from typing import Dict, Any, List, Optional
from aurum.api.cache.unified_cache_manager import get_unified_cache_manager
from aurum.api.cache.cache_governance import CacheNamespace
from aurum.core import AurumSettings
from aurum.api.logging.structured_logger import get_logger

LOGGER = get_logger(__name__)


class ModernizedCurvesDao:
    """Example of how CurvesDao would look with unified cache manager."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings
        # Get the unified cache manager instead of creating multiple managers
        self._cache_manager = get_unified_cache_manager()
    
    async def query_curves_with_caching(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        asof: Optional[str] = None,
        strip: Optional[str] = None,
        offset: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Query curve data with unified caching."""
        
        # Create cache key using unified approach
        if self._cache_manager:
            # Use the unified cache manager's curve data methods
            cache_key_parts = [str(iso), str(market), str(location)]
            if product:
                cache_key_parts.append(str(product))
            if block:
                cache_key_parts.append(str(block))
            if strip:
                cache_key_parts.append(str(strip))
            if asof:
                cache_key_parts.append(str(asof))
                
            cache_key = ":".join(cache_key_parts) + f":{offset}:{limit}"
            
            # Try to get from cache first
            cached_result = await self._cache_manager.get(
                cache_key, 
                namespace=CacheNamespace.CURVES
            )
            
            if cached_result is not None:
                LOGGER.info("Cache hit for curves query", extra={"cache_key": cache_key})
                return cached_result
        
        # Execute query if not in cache
        from ..database import get_trino_client
        
        query, params = self._build_curve_query(
            iso=iso, market=market, location=location, product=product,
            block=block, asof=asof, strip=strip, offset=offset, limit=limit
        )
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params)
        result = [dict(row) for row in rows]
        
        # Cache the result using unified cache manager
        if self._cache_manager:
            await self._cache_manager.set(
                cache_key,
                result,
                namespace=CacheNamespace.CURVES
                # TTL automatically determined by governance policy (5 minutes for curves)
            )
            LOGGER.info("Cached curves query result", extra={"cache_key": cache_key})
        
        return result
    
    async def invalidate_curve_cache_unified(
        self,
        *,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
    ) -> Dict[str, int]:
        """Invalidate curve cache using unified cache manager."""
        if not self._cache_manager:
            return {"invalidated_count": 0}
        
        # Build pattern for invalidation
        pattern_parts = []
        if iso:
            pattern_parts.append(str(iso))
        else:
            pattern_parts.append("*")
            
        if market:
            pattern_parts.append(str(market))
        else:
            pattern_parts.append("*")
            
        if location:
            pattern_parts.append(str(location))
        else:
            pattern_parts.append("*")
        
        # Use namespace-based clearing for better performance
        if not any([iso, market, location]):
            # Clear entire curves namespace
            await self._cache_manager.clear(namespace=CacheNamespace.CURVES)
            invalidated_count = 1  # Simplified count
        else:
            # Pattern-based invalidation
            pattern = ":".join(pattern_parts) + ":*"
            invalidated_count = await self._cache_manager.invalidate_pattern(pattern)
        
        LOGGER.info(
            "Invalidated curve cache",
            extra={
                "iso": iso,
                "market": market, 
                "location": location,
                "invalidated_count": invalidated_count
            }
        )
        
        return {"invalidated_count": invalidated_count}
    
    def _build_curve_query(self, **kwargs) -> tuple:
        """Build SQL query (same as original implementation)."""
        # Implementation would be the same as original
        pass


class ModernizedMetadataDao:
    """Example of how MetadataDao would look with unified cache manager."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings
        self._cache_manager = get_unified_cache_manager()
    
    async def get_locations_with_caching(
        self,
        *,
        iso: Optional[str] = None,
        region: Optional[str] = None,
        active_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Get locations with unified caching."""
        
        if self._cache_manager:
            # Use unified cache manager's metadata methods for backward compatibility
            cache_result = await self._cache_manager.get_metadata(
                "locations",
                iso=iso or "all",
                region=region or "all", 
                active_only=active_only
            )
            
            if cache_result is not None:
                return cache_result
        
        # Query database
        from ..database import get_trino_client
        
        query = "SELECT * FROM locations WHERE 1=1"
        params = {}
        
        if iso:
            query += " AND iso = %(iso)s"
            params["iso"] = iso
            
        if region:
            query += " AND region = %(region)s"
            params["region"] = region
            
        if active_only:
            query += " AND active = true"
        
        trino_client = get_trino_client()
        rows = await trino_client.execute_query(query, params)
        result = [dict(row) for row in rows]
        
        # Cache using unified manager
        if self._cache_manager:
            await self._cache_manager.cache_metadata(
                result,
                "locations",
                iso=iso or "all",
                region=region or "all",
                active_only=active_only
                # TTL automatically set to 30 minutes for metadata
            )
        
        return result
    
    async def invalidate_metadata_cache(self, metadata_type: str) -> int:
        """Invalidate specific metadata cache."""
        if not self._cache_manager:
            return 0
            
        # Pattern to match all entries for this metadata type
        pattern = f"metadata:{metadata_type}:*"
        return await self._cache_manager.invalidate_pattern(pattern)


class CacheAwareService:
    """Example service class using unified cache manager."""
    
    def __init__(self):
        self._cache_manager = get_unified_cache_manager()
        self._curves_dao = ModernizedCurvesDao()
        self._metadata_dao = ModernizedMetadataDao()
    
    async def get_enriched_curve_data(
        self,
        iso: str,
        market: str,
        location: str
    ) -> Dict[str, Any]:
        """Get curve data enriched with location metadata."""
        
        # This method demonstrates how different cache namespaces work together
        
        # Get curve data (uses CURVES namespace with 5-minute TTL)
        curves = await self._curves_dao.query_curves_with_caching(
            iso=iso, market=market, location=location, limit=1000
        )
        
        # Get location metadata (uses METADATA namespace with 30-minute TTL)
        location_info = await self._metadata_dao.get_locations_with_caching(
            iso=iso
        )
        
        # Create enriched result
        enriched_result = {
            "curves": curves,
            "location_metadata": location_info,
            "iso": iso,
            "market": market,
            "location": location
        }
        
        # Cache the enriched result with custom TTL
        if self._cache_manager:
            cache_key = f"enriched:{iso}:{market}:{location}"
            await self._cache_manager.set(
                cache_key,
                enriched_result,
                ttl=600,  # 10 minutes - between curve and metadata TTLs
                namespace=CacheNamespace.CURVES  # Use curves namespace for business logic
            )
        
        return enriched_result
    
    async def invalidate_related_caches(self, iso: str, market: str) -> Dict[str, Any]:
        """Invalidate all caches related to an ISO/market combination."""
        results = {}
        
        if self._cache_manager:
            # Invalidate curve caches
            curve_result = await self._curves_dao.invalidate_curve_cache_unified(
                iso=iso, market=market
            )
            results["curves"] = curve_result
            
            # Invalidate location metadata (if affected by ISO changes)
            metadata_result = await self._metadata_dao.invalidate_metadata_cache("locations")
            results["metadata"] = {"invalidated_count": metadata_result}
            
            # Invalidate enriched results
            enriched_pattern = f"enriched:{iso}:{market}:*"
            enriched_count = await self._cache_manager.invalidate_pattern(enriched_pattern)
            results["enriched"] = {"invalidated_count": enriched_count}
        
        return results
    
    async def get_cache_health_status(self) -> Dict[str, Any]:
        """Get comprehensive cache health status."""
        if not self._cache_manager:
            return {"status": "cache_manager_not_available"}
        
        stats = await self._cache_manager.get_stats()
        
        # Add business logic specific health checks
        health_status = {
            "unified_cache_available": True,
            "total_requests": stats["unified_cache"]["total_requests"],
            "hit_rate": stats["unified_cache"]["hit_rate"],
            "average_response_time_ms": stats["unified_cache"]["average_response_time_ms"],
            "namespace_health": {}
        }
        
        # Check each namespace
        for namespace, ns_stats in stats["unified_cache"]["namespace_stats"].items():
            total_ops = ns_stats["hits"] + ns_stats["misses"]
            if total_ops > 0:
                hit_rate = ns_stats["hits"] / total_ops
                health_status["namespace_health"][namespace] = {
                    "hit_rate": round(hit_rate, 4),
                    "total_operations": total_ops,
                    "status": "healthy" if hit_rate > 0.7 else "degraded"
                }
        
        return health_status


# Migration utility functions
async def migrate_dao_to_unified_cache(old_dao_instance, unified_cache_manager):
    """Utility to help migrate existing DAO instances."""
    
    # This is a conceptual example - in practice, you'd update the DAO classes directly
    if hasattr(old_dao_instance, '_cache_config'):
        print(f"Migrating {type(old_dao_instance).__name__} to unified cache")
        
        # Update cache manager reference
        old_dao_instance._cache_manager = unified_cache_manager
        
        # Log the migration
        LOGGER.info(
            "DAO migrated to unified cache",
            extra={"dao_class": type(old_dao_instance).__name__}
        )


def get_cache_migration_benefits():
    """Return list of benefits from migrating to unified cache."""
    return [
        "Consistent TTL policies across all data types",
        "Unified analytics and monitoring",
        "Better namespace management", 
        "Reduced memory overhead from multiple cache instances",
        "Simplified cache invalidation patterns",
        "Built-in governance and validation",
        "Enhanced error handling and logging",
        "Support for advanced features like cache warming"
    ]


if __name__ == "__main__":
    import asyncio
    
    async def demo():
        print("DAO Migration Benefits:")
        for benefit in get_cache_migration_benefits():
            print(f"- {benefit}")
    
    asyncio.run(demo())