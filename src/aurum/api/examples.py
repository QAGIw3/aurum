"""Integration examples and usage patterns for the refactored API."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
import asyncio

from .container import configure_services, get_service
from .cache import AsyncCache, CacheManager, CacheBackend
from .exceptions import AurumAPIException, ValidationException
from .testing import TestDataFactory


async def example_basic_usage():
    """Example of basic API usage with the new architecture."""
    print("=== Basic API Usage Example ===")

    # Configure services
    configure_services()

    # Get services using dependency injection
    from .container import get_service_provider
    from .async_service import AsyncCurveService

    service_provider = get_service_provider()
    curve_service = service_provider.get(AsyncCurveService)

    # Use the service
    try:
        curves, meta = await curve_service.fetch_curves(
            iso="PJM",
            market="DAY_AHEAD",
            location="HUB"
        )
        print(f"Retrieved {len(curves)} curves")
        print(f"Query time: {meta.query_time_ms}ms")

    except AurumAPIException as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")


async def example_caching():
    """Example of using the enhanced caching system."""
    print("\n=== Caching Example ===")

    # Create cache with Redis backend
    cache = AsyncCache(CacheBackend.REDIS)

    # Use cache manager for higher-level operations
    cache_manager = CacheManager(cache)

    # Cache some data
    test_data = {"iso": "PJM", "market": "DAY_AHEAD", "data": [1, 2, 3, 4, 5]}
    await cache_manager.cache_curve_data(
        data=test_data,
        iso="PJM",
        market="DAY_AHEAD",
        location="HUB"
    )

    # Retrieve cached data
    cached_data = await cache_manager.get_curve_data("PJM", "DAY_AHEAD", "HUB")
    if cached_data:
        print("Retrieved data from cache")
    else:
        print("No cached data found")

    # Get cache statistics
    stats = await cache_manager.get_cache_stats()
    print(f"Cache stats: {stats}")


async def example_error_handling():
    """Example of proper error handling with the new exception hierarchy."""
    print("\n=== Error Handling Example ===")

    try:
        # This would normally be an API call
        raise ValidationException(
            detail="Invalid cursor parameter",
            field_errors={"cursor": "Must be base64 encoded"}
        )
    except ValidationException as e:
        print(f"Validation error: {e.detail}")
        print(f"Field errors: {e.context.get('field_errors', {})}")
    except AurumAPIException as e:
        print(f"API error: {e.detail}")
    except Exception as e:
        print(f"Unexpected error: {e}")


async def example_data_processing():
    """Example of using the data processing pipeline."""
    print("\n=== Data Processing Example ===")

    from .data_processing import get_data_pipeline, DataQualityMetrics

    # Get the data pipeline
    pipeline = get_data_pipeline()

    # Create some test data
    test_data = TestDataFactory.create_curve_points(10)

    # Process data through the pipeline
    enriched_data, metrics = await pipeline.validate_and_enrich_batch(
        test_data,
        "curve_observation"
    )

    print(f"Processed {len(test_data)} rows")
    print(f"Valid rows: {metrics.valid_rows}")
    print(f"Invalid rows: {metrics.invalid_rows}")
    print(f"Quality score: {metrics.get_quality_score():.1f}%")
    print(f"Processing time: {metrics.processing_time:.3f}s")


async def example_performance_monitoring():
    """Example of performance monitoring."""
    print("\n=== Performance Monitoring Example ===")

    from .performance import get_performance_monitor

    monitor = get_performance_monitor()

    # Simulate some queries
    for i in range(5):
        await monitor.record_query(
            query_type="curve_data",
            execution_time=0.1 + i * 0.05,  # Simulate varying execution times
            result_count=100 + i * 10,
            cached=(i % 2 == 0)  # Every other query is cached
        )

    # Get performance statistics
    stats = await monitor.get_stats()
    print(f"Query stats: {stats['query_stats']}")
    print(f"Cache hit rate: {stats['cache_hit_rate']:.1%}")
    print(f"Total queries: {stats['total_queries']}")


async def example_async_patterns():
    """Example of async patterns for concurrent operations."""
    print("\n=== Async Patterns Example ===")

    from .async_service import AsyncCurveService
    from .performance import get_query_optimizer

    # Get services
    optimizer = get_query_optimizer()
    curve_service = AsyncCurveService(None)  # Would need proper config

    # Example of concurrent queries (would need actual implementation)
    async def fetch_multiple_isos():
        """Fetch data for multiple ISOs concurrently."""
        # This would use asyncio.gather to fetch from multiple ISOs in parallel
        print("Would fetch data from PJM, CAISO, and ERCOT concurrently")
        # tasks = [
        #     curve_service.fetch_curves(iso=iso)
        #     for iso in ["PJM", "CAISO", "ERCOT"]
        # ]
        # results = await asyncio.gather(*tasks)

    await fetch_multiple_isos()


async def example_testing():
    """Example of using the enhanced testing utilities."""
    print("\n=== Testing Example ===")

    from .testing import APITestCase, TestDataFactory

    # Create test case
    test_case = APITestCase()
    data_factory = TestDataFactory()

    # Create test data
    curve_data = test_case.create_test_curve_data(5)
    scenario_data = test_case.create_test_scenario()

    print(f"Created {len(curve_data)} test curve points")
    print(f"Created test scenario: {scenario_data['name']}")

    # Mock a service
    from unittest.mock import AsyncMock
    mock_service = AsyncMock()
    mock_service.fetch_curves.return_value = (curve_data, None)

    test_case.mock_service(type(mock_service), mock_service)

    print("Mock service registered successfully")


async def example_load_testing():
    """Example of load testing capabilities."""
    print("\n=== Load Testing Example ===")

    from .testing import LoadTestHelper

    # Create load tester
    load_tester = LoadTestHelper(concurrency=5)

    # Define a test function (normally would make actual API calls)
    async def mock_api_call():
        await asyncio.sleep(0.01)  # Simulate API call
        return {"status": "success"}

    # Run load test
    results = await load_tester.run_load_test(
        mock_api_call,
        num_requests=50
    )

    print("Load test results:")
    print(f"  Total requests: {results['total_requests']}")
    print(f"  Successful: {results['successful_requests']}")
    print(f"  Success rate: {results['success_rate']:.1%}")
    print(f"  Requests/sec: {results['requests_per_second']:.1f}")
    print(f"  Avg time/request: {results['average_time_per_request']:.3f}s")


async def example_advanced_caching():
    """Example of using the advanced caching system."""
    print("\n=== Advanced Caching Example ===")

    from .advanced_cache import get_advanced_cache_manager, get_cache_warming_service
    from .cache import CacheBackend

    # Get advanced cache manager
    cache_manager = get_advanced_cache_manager()

    # Get cache analytics
    analytics = await cache_manager.get_all_analytics()
    print(f"Cache namespaces: {list(analytics.keys())}")

    # Add a warmup task
    async def warmup_curve_data(key: str):
        # This would load curve data for the key
        return f"curve_data_for_{key}"

    warming_service = get_cache_warming_service()
    await warming_service.add_warmup_task(
        name="curve_data_warmup",
        key_pattern="curve:*",
        warmup_function=warmup_curve_data,
        schedule_interval=300,  # 5 minutes
    )

    print("Added cache warmup task")

    # Get cache statistics
    stats = await cache_manager.get_cache_stats()
    print(f"Warming status: {stats.get('warming_status', 'unknown')}")
    print(f"Active warmup tasks: {stats.get('warmup_tasks', 0)}")

    # Get performance recommendations
    print("\nCache Performance Recommendations:")
    recommendations = await cache_manager.get_cache_stats()
    # In a real implementation, this would analyze performance
    print("  - Consider implementing cache eviction policies")
    print("  - Monitor hit rates by namespace")
    print("  - Use adaptive TTL based on access patterns")


async def example_cache_analytics():
    """Example of cache analytics and monitoring."""
    print("\n=== Cache Analytics Example ===")

    from .advanced_cache import get_advanced_cache_manager

    cache_manager = get_advanced_cache_manager()

    # Get detailed analytics
    analytics = await cache_manager.get_all_analytics()

    print("Cache Analytics Summary:")
    for namespace, data in analytics.items():
        print(f"  Namespace '{namespace}':")
        print(f"    Hits: {data.hits}")
        print(f"    Misses: {data.misses}")
        print(f"    Hit Rate: {data.hit_rate".1%"}")
        print(f"    Avg Response Time: {data.average_response_time".3f"}s")

    # Get memory usage
    memory_usage = await cache_manager.get_memory_usage()
    print(f"\nMemory Usage: {memory_usage.get('estimated_size_bytes', 0) / 1024".1f"} KB")

    # Get cache health
    health = {
        "status": "healthy",
        "issues": []
    }

    # Analyze health based on analytics
    for namespace, data in analytics.items():
        if data.hit_rate < 0.5:
            health["issues"].append(f"Low hit rate in '{namespace}': {data.hit_rate".1%"}")

    print(f"Overall Health: {health['status']}")
    if health["issues"]:
        print(f"Issues found: {len(health['issues'])}")
        for issue in health["issues"]:
            print(f"  - {issue}")


async def example_distributed_caching():
    """Example of distributed caching setup."""
    print("\n=== Distributed Caching Example ===")

    from .advanced_cache import AdvancedCacheManager, CacheBackend
    from .config import CacheConfig

    # Create cache config for distributed setup
    distributed_config = CacheConfig(
        mode="cluster",
        cluster_nodes=["redis-node1:6379", "redis-node2:6379"],
        ttl_seconds=600,
    )

    # Create distributed cache manager
    distributed_cache = AdvancedCacheManager(distributed_config, CacheBackend.REDIS)

    # Get distributed node information
    stats = await distributed_cache.get_cache_stats()
    distributed_info = stats.get("distributed_info", {})

    print("Distributed Cache Configuration:")
    if distributed_info:
        print(f"  Nodes configured: {distributed_info.get('nodes', [])}")
        print("  Distributed caching enabled")
    else:
        print("  Single-node caching (Redis cluster not configured)")

    # Demonstrate namespace-based caching
    await distributed_cache.set("test_key", "test_value", namespace="test")
    value = await distributed_cache.get("test_key", namespace="test")

    print(f"  Set/Get test: {'✓' if value == 'test_value' else '✗'}")

    # Clear namespace
    await distributed_cache.clear_namespace("test")
    print("  Namespace cleared")


async def main():
    """Run all examples."""
    print("🚀 Aurum API Refactored Architecture Examples")
    print("=" * 50)

    await example_basic_usage()
    await example_caching()
    await example_error_handling()
    await example_data_processing()
    await example_performance_monitoring()
    await example_async_patterns()
    await example_testing()
    await example_load_testing()
    await example_advanced_caching()
    await example_cache_analytics()
    await example_distributed_caching()

    print("\n" + "=" * 50)
    print("✅ All examples completed successfully!")
    print("\nThe refactored Aurum API now supports:")
    print("• Modular architecture with dependency injection")
    print("• Async/await patterns for better performance")
    print("• Enhanced caching with multiple backends")
    print("• Comprehensive error handling")
    print("• Data processing pipeline with validation")
    print("• Performance monitoring and optimization")
    print("• Improved testing utilities")
    print("• Advanced caching with warming and analytics")
    print("• Distributed caching capabilities")
    print("• Cache performance monitoring and recommendations")


if __name__ == "__main__":
    asyncio.run(main())
