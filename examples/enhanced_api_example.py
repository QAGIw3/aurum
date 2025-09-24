"""Example demonstrating the enhanced Aurum API architecture."""

import asyncio
import logging
from datetime import date, timedelta
from typing import List

# Enhanced core components
from src.aurum.core import (
    # Dependency injection
    get_container,
    register_singleton,
    ServiceLifecycle,
    
    # Async context management
    request_context,
    managed_resources,
    
    # Caching
    create_multi_level_cache,
    CacheLevel,
    
    # Resilience patterns
    create_resilient_service,
    circuit_breaker,
    retry,
    CircuitBreakerConfig,
    RetryConfig,
)

# Enhanced API components
from src.aurum.api.enhanced_app import create_enhanced_app, EnhancedAppConfig
from src.aurum.api.security.security_middleware import SecurityConfig
from src.aurum.api.modules.curves.service import AsyncCurveService, CurveQuery

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def demonstrate_dependency_injection():
    """Demonstrate enhanced dependency injection."""
    print("\n=== Dependency Injection Example ===")
    
    container = get_container()
    
    # Register services with different lifecycles
    register_singleton(str, instance="Global Configuration Service")
    
    # Get service
    config_service = container.get(str)
    print(f"Retrieved service: {config_service}")
    
    # Demonstrate scoped services
    async with container.scope() as scope:
        print("Created new service scope")
        # Scoped services would be cleaned up automatically


async def demonstrate_async_context():
    """Demonstrate async context management."""
    print("\n=== Async Context Management Example ===")
    
    # Use request context
    async with request_context(
        request_id="demo-req-001",
        tenant_id="demo-tenant",
        user_id="demo-user",
        operation="demonstrate_context"
    ) as context:
        print(f"Request context: {context.to_dict()}")
        
        # Use managed resources
        async with managed_resources() as resource_manager:
            # Simulate resource registration
            resource_manager.register_resource("demo_resource")
            print("Resources will be cleaned up automatically")


async def demonstrate_caching():
    """Demonstrate multi-level caching."""
    print("\n=== Multi-Level Caching Example ===")
    
    # Create cache system
    cache = create_multi_level_cache(
        memory_config={"max_size": 1000, "max_memory_mb": 50}
    )
    
    # Store and retrieve data
    await cache.set("demo_key", {"data": "cached_value"}, ttl=3600)
    
    cached_data = await cache.get("demo_key")
    print(f"Retrieved from cache: {cached_data}")
    
    # Demonstrate cache invalidation by tags
    await cache.set("iso:PJM:data", {"iso": "PJM"}, tags={"iso:PJM"})
    await cache.set("iso:CAISO:data", {"iso": "CAISO"}, tags={"iso:CAISO"})
    
    # Invalidate all PJM data
    invalidated = await cache.invalidate_by_tags({"iso:PJM"})
    print(f"Invalidated {invalidated} cache entries")
    
    # Show metrics
    metrics = cache.get_combined_metrics()
    for level, metric in metrics.items():
        print(f"{level}: {metric.hits} hits, {metric.misses} misses, {metric.hit_rate:.2%} hit rate")


async def demonstrate_resilience_patterns():
    """Demonstrate resilience patterns."""
    print("\n=== Resilience Patterns Example ===")
    
    # Create resilient service
    resilience_manager = create_resilient_service(
        failure_threshold=3,
        retry_attempts=2,
        timeout_seconds=5.0,
        max_concurrent=10
    )
    
    # Simulate a service that sometimes fails
    call_count = 0
    
    async def unreliable_service():
        nonlocal call_count
        call_count += 1
        
        if call_count <= 2:
            # Fail first two calls
            raise Exception(f"Simulated failure #{call_count}")
        
        return {"success": True, "call_count": call_count}
    
    try:
        # Execute with resilience patterns
        result = await resilience_manager.execute(unreliable_service)
        print(f"Service succeeded after resilience handling: {result}")
    except Exception as e:
        print(f"Service failed even with resilience: {e}")
    
    # Show resilience metrics
    metrics = resilience_manager.get_metrics()
    print(f"Resilience metrics: {metrics}")


@circuit_breaker(CircuitBreakerConfig(failure_threshold=2))
async def demo_circuit_breaker_service():
    """Demonstrate circuit breaker decorator."""
    # This would be a real service call
    raise Exception("Service unavailable")


@retry(RetryConfig(max_attempts=3, base_delay=0.1))
async def demo_retry_service():
    """Demonstrate retry decorator."""
    # This would be a real service call that might succeed on retry
    import random
    if random.random() < 0.7:  # 70% chance of failure
        raise Exception("Temporary service failure")
    return {"success": True}


async def demonstrate_enhanced_curve_service():
    """Demonstrate enhanced curve service."""
    print("\n=== Enhanced Curve Service Example ===")
    
    # Create cache and service
    cache = create_multi_level_cache()
    
    # Note: In real implementation, data_source would be properly configured
    curve_service = AsyncCurveService(
        cache=cache,
        data_source=None,  # Would be injected in real app
        batch_size=50
    )
    
    # Demonstrate single curve fetch
    try:
        curves, meta = await curve_service.fetch_curves(
            iso="PJM",
            market="DAY_AHEAD",
            location="HUB",
            start_date=date.today(),
            end_date=date.today() + timedelta(days=1),
            use_cache=True,
            warm_cache=True
        )
        print(f"Fetched {len(curves)} curve points")
        print(f"Request metadata: {meta}")
    except Exception as e:
        print(f"Curve service error (expected in demo): {e}")
    
    # Demonstrate batch fetching
    queries = [
        CurveQuery(iso="PJM", market="DAY_AHEAD", location="HUB"),
        CurveQuery(iso="CAISO", market="DAY_AHEAD", location="SP15"),
        CurveQuery(iso="ERCOT", market="REAL_TIME", location="HUB"),
    ]
    
    try:
        batch_results = await curve_service.batch_fetch_curves(queries)
        print(f"Batch fetched {len(batch_results)} query results")
    except Exception as e:
        print(f"Batch fetch error (expected in demo): {e}")


async def demonstrate_enhanced_app():
    """Demonstrate enhanced application creation."""
    print("\n=== Enhanced Application Example ===")
    
    # Create enhanced app configuration
    config = EnhancedAppConfig(
        title="Demo Enhanced Aurum API",
        version="2.0.0-demo",
        
        # Performance settings
        enable_performance_monitoring=True,
        max_concurrent_requests=100,
        request_timeout=30.0,
        
        # Caching settings
        enable_caching=True,
        memory_cache_size=1000,
        memory_cache_mb=50,
        
        # Security settings (disabled for demo)
        security_config=SecurityConfig(
            enable_rate_limiting=False,
            enable_ip_filtering=False,
            enable_api_key_validation=False,
        ),
        
        # Resilience settings
        enable_circuit_breakers=True,
        enable_retries=True,
    )
    
    # Create enhanced app
    app = create_enhanced_app(config)
    
    print(f"Created enhanced app: {app.title} v{app.version}")
    print("App includes:")
    print("- Enhanced dependency injection")
    print("- Multi-level caching")
    print("- Performance monitoring")
    print("- Security middleware")
    print("- Resilience patterns")
    print("- Modular API structure")
    
    # In a real application, you would run:
    # uvicorn enhanced_api_example:app --host 0.0.0.0 --port 8000


async def main():
    """Run all demonstration examples."""
    print("🚀 Enhanced Aurum API Architecture Demonstration")
    print("=" * 60)
    
    try:
        await demonstrate_dependency_injection()
        await demonstrate_async_context()
        await demonstrate_caching()
        await demonstrate_resilience_patterns()
        await demonstrate_enhanced_curve_service()
        await demonstrate_enhanced_app()
        
        print("\n" + "=" * 60)
        print("✅ All demonstrations completed successfully!")
        print("\nKey improvements implemented:")
        print("• Enhanced dependency injection with lifecycle management")
        print("• Multi-level caching with intelligent eviction")
        print("• Comprehensive resilience patterns")
        print("• Advanced async context management") 
        print("• Modular API architecture")
        print("• Performance monitoring and optimization")
        print("• Enhanced security and validation")
        print("• Streaming and batch operations")
        
    except Exception as e:
        print(f"\n❌ Demo error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run the demonstration
    asyncio.run(main())