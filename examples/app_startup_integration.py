"""Example showing how to integrate UnifiedCacheManager into application startup.

This example demonstrates proper initialization, configuration, and shutdown
of the unified cache system in a FastAPI application.
"""

import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import JSONResponse

# Unified cache imports
from aurum.api.cache.initialization import (
    initialize_unified_cache_system,
    shutdown_unified_cache_system,
    validate_cache_system
)
from aurum.api.cache.unified_cache_manager import get_unified_cache_manager
from aurum.api.cache.cache import CacheBackend
from aurum.api.cache.cache_governance import CacheNamespace
from aurum.api.deps import get_unified_cache_manager_dep
from aurum.core.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager with unified cache initialization."""
    # Startup
    print("Starting application with unified cache system...")
    
    try:
        # Initialize the unified cache system
        settings = get_settings()
        backend = CacheBackend.HYBRID if settings.redis_host else CacheBackend.MEMORY
        
        unified_manager = await initialize_unified_cache_system(backend=backend)
        
        # Store in app state for dependency injection
        app.state.unified_cache_manager = unified_manager
        
        # Validate the cache system
        validation_results = await validate_cache_system()
        if validation_results["errors"]:
            print(f"Cache validation warnings: {validation_results['errors']}")
        else:
            print("Cache system validation passed")
        
        # Warm critical caches
        await warm_critical_caches(unified_manager)
        
        print("Application startup complete with unified cache system")
        
        yield
        
    except Exception as e:
        print(f"Failed to initialize cache system: {e}")
        raise
    
    # Shutdown
    print("Shutting down unified cache system...")
    try:
        await shutdown_unified_cache_system(unified_manager)
        print("Cache system shutdown complete")
    except Exception as e:
        print(f"Error during cache shutdown: {e}")


async def warm_critical_caches(unified_manager):
    """Warm up critical caches during application startup."""
    
    async def warm_metadata():
        """Warm metadata cache."""
        return {
            "locations:all": {"count": 150, "regions": ["west", "east", "central"]},
            "isos:active": {"CAISO", "PJM", "ERCOT", "MISO", "SPP"},
            "markets:all": {"RT", "DA", "FTR"}
        }
    
    async def warm_system_config():
        """Warm system configuration cache."""
        return {
            "feature_flags": {"new_ui": True, "enhanced_analytics": True},
            "rate_limits": {"default": 1000, "premium": 5000},
            "maintenance_mode": False
        }
    
    # Warm different namespaces
    await unified_manager.warm_cache(CacheNamespace.METADATA, warm_metadata)
    await unified_manager.warm_cache(CacheNamespace.SYSTEM_CONFIG, warm_system_config)
    
    print("Critical caches warmed successfully")


# Create FastAPI app with unified cache integration
app = FastAPI(
    title="Aurum API with Unified Cache",
    description="Example API using UnifiedCacheManager",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Basic health check."""
    return {"status": "healthy", "cache_system": "unified"}


@app.get("/cache/health")
async def cache_health(
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Detailed cache health check."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    validation_results = await validate_cache_system()
    return JSONResponse(
        content=validation_results,
        status_code=200 if not validation_results["errors"] else 503
    )


@app.get("/cache/stats")
async def cache_statistics(
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Get comprehensive cache statistics."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    stats = await cache_manager.get_stats()
    return stats


@app.post("/cache/warm/{namespace}")
async def warm_cache_namespace(
    namespace: str,
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Warm a specific cache namespace."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    try:
        cache_namespace = CacheNamespace(namespace)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid namespace: {namespace}")
    
    async def sample_warm_function():
        """Sample warming function - in practice, this would load real data."""
        return {
            f"sample_key_{i}": f"sample_value_{i}" 
            for i in range(10)
        }
    
    result = await cache_manager.warm_cache(cache_namespace, sample_warm_function)
    return result


@app.delete("/cache/clear/{namespace}")
async def clear_cache_namespace(
    namespace: str,
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Clear a specific cache namespace."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    try:
        cache_namespace = CacheNamespace(namespace)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid namespace: {namespace}")
    
    await cache_manager.clear(namespace=cache_namespace)
    return {"status": "cleared", "namespace": namespace}


@app.get("/curves/{iso}/{market}/{location}")
async def get_curve_data(
    iso: str,
    market: str,
    location: str,
    asof: str = None,
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Get curve data with caching."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    # Try cache first
    cached_data = await cache_manager.get_curve_data(iso, market, location, asof)
    if cached_data:
        return {
            "data": cached_data,
            "source": "cache",
            "iso": iso,
            "market": market,
            "location": location
        }
    
    # Simulate database query
    curve_data = {
        "iso": iso,
        "market": market,
        "location": location,
        "asof": asof,
        "values": [100 + i for i in range(24)],  # Sample hourly data
        "generated_at": "2024-01-01T00:00:00Z"
    }
    
    # Cache the result
    await cache_manager.cache_curve_data(curve_data, iso, market, location, asof)
    
    return {
        "data": curve_data,
        "source": "database",
        "iso": iso,
        "market": market,
        "location": location
    }


@app.get("/metadata/{metadata_type}")
async def get_metadata(
    metadata_type: str,
    region: str = None,
    active_only: bool = True,
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Get metadata with caching."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    # Build filters
    filters = {"active_only": active_only}
    if region:
        filters["region"] = region
    
    # Try cache first
    cached_data = await cache_manager.get_metadata(metadata_type, **filters)
    if cached_data:
        return {
            "data": cached_data,
            "source": "cache",
            "metadata_type": metadata_type,
            "filters": filters
        }
    
    # Simulate database query
    if metadata_type == "locations":
        metadata = [
            {"id": i, "name": f"Location_{i}", "region": region or "west", "active": active_only}
            for i in range(1, 11)
        ]
    else:
        metadata = {"type": metadata_type, "count": 0, "items": []}
    
    # Cache the result
    await cache_manager.cache_metadata(metadata, metadata_type, **filters)
    
    return {
        "data": metadata,
        "source": "database",
        "metadata_type": metadata_type,
        "filters": filters
    }


@app.post("/cache/invalidate/pattern")
async def invalidate_cache_pattern(
    pattern: str,
    cache_manager = Depends(get_unified_cache_manager_dep)
):
    """Invalidate cache entries matching a pattern."""
    if not cache_manager:
        raise HTTPException(status_code=503, detail="Cache manager not available")
    
    count = await cache_manager.invalidate_pattern(pattern)
    return {"invalidated_count": count, "pattern": pattern}


# Alternative startup configuration (without lifespan context manager)
async def configure_cache_on_startup():
    """Alternative startup configuration function."""
    try:
        # This could be called from a separate startup event handler
        unified_manager = await initialize_unified_cache_system(
            backend=CacheBackend.MEMORY  # or HYBRID for production
        )
        
        # Warm critical caches
        await warm_critical_caches(unified_manager)
        
        return unified_manager
        
    except Exception as e:
        print(f"Cache configuration failed: {e}")
        return None


# Example middleware to add cache headers
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

class CacheHeaderMiddleware(BaseHTTPMiddleware):
    """Middleware to add cache-related headers."""
    
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        
        # Add cache system identifier
        response.headers["X-Cache-System"] = "unified"
        
        # Add cache statistics if available
        cache_manager = getattr(request.app.state, "unified_cache_manager", None)
        if cache_manager:
            stats = await cache_manager.get_stats()
            hit_rate = stats["unified_cache"]["hit_rate"]
            response.headers["X-Cache-Hit-Rate"] = str(round(hit_rate, 4))
        
        return response


# Add middleware
app.add_middleware(CacheHeaderMiddleware)


# Example configuration for different environments
def get_cache_config_for_environment(env: str) -> Dict[str, Any]:
    """Get cache configuration based on environment."""
    
    configs = {
        "development": {
            "backend": CacheBackend.MEMORY,
            "warm_on_startup": False,
            "enable_analytics": True
        },
        "staging": {
            "backend": CacheBackend.HYBRID, 
            "warm_on_startup": True,
            "enable_analytics": True
        },
        "production": {
            "backend": CacheBackend.HYBRID,
            "warm_on_startup": True,
            "enable_analytics": True,
            "redis_cluster": True
        }
    }
    
    return configs.get(env, configs["development"])


if __name__ == "__main__":
    # Example of running the app
    import uvicorn
    
    print("Starting Aurum API with UnifiedCacheManager")
    print("Visit http://localhost:8000/docs for API documentation")
    print("Cache endpoints:")
    print("- GET /cache/health - Cache health check")
    print("- GET /cache/stats - Cache statistics") 
    print("- POST /cache/warm/{namespace} - Warm cache namespace")
    print("- DELETE /cache/clear/{namespace} - Clear cache namespace")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)