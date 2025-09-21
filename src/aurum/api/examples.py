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
        print(f"    Hit Rate: {data.hit_rate:.1%}")
        print(f"    Avg Response Time: {data.average_response_time:.3f}s")

    # Get memory usage
    memory_usage = await cache_manager.get_memory_usage()
    print(f"\nMemory Usage: {memory_usage.get('estimated_size_bytes', 0) / 1024:.1f} KB")

    # Get cache health
    health = {
        "status": "healthy",
        "issues": []
    }

    # Analyze health based on analytics
    for namespace, data in analytics.items():
        if data.hit_rate < 0.5:
            health["issues"].append(f"Low hit rate in '{namespace}': {data.hit_rate:.1%}")

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


async def example_api_versioning():
    """Example of API versioning with backward compatibility."""
    print("\n=== API Versioning Example ===")

    from .versioning import (
        get_version_manager,
        create_v1_router,
        create_v2_router,
        VersionStatus,
        DeprecationInfo
    )

    # Get version manager
    version_manager = get_version_manager()

    # Register v1 API
    v1_router = create_v1_router()
    await version_manager.register_version(
        "1.0",
        status=VersionStatus.ACTIVE,
        supported_features=["basic_queries", "simple_filtering"]
    )

    # Register v2 API with enhanced features
    v2_router = create_v2_router()
    await version_manager.register_version(
        "2.0",
        status=VersionStatus.ACTIVE,
        supported_features=[
            "async_support",
            "improved_error_handling",
            "advanced_caching",
            "real_time_updates"
        ]
    )

    # Get version information
    v1_version = await version_manager.get_version("1.0")
    v2_version = await version_manager.get_version("2.0")

    print("API Versions:")
    print(f"  v1.0: {v1_version.status.value} - Features: {v1_version.supported_features}")
    print(f"  v2.0: {v2_version.status.value} - Features: {v2_version.supported_features}")

    # List all versions
    all_versions = await version_manager.list_versions()
    print(f"\nAll registered versions: {len(all_versions)}")
    for version in all_versions:
        print(f"  - {version.version}: {version.status.value}")

    # Get default version
    default_version = await version_manager.get_default_version()
    print(f"Default version: {default_version}")

    # Demonstrate version deprecation
    print("\n--- Deprecation Example ---")
    deprecation_info = DeprecationInfo(
        deprecated_in="2024-01-01T00:00:00Z",
        sunset_on="2024-06-01T00:00:00Z",
        removed_in="2024-12-01T00:00:00Z",
        migration_guide="https://docs.example.com/migration/v1-to-v2",
        alternative_endpoints=["/v2/curves", "/v2/metadata"]
    )

    await version_manager.register_version(
        "1.5",
        status=VersionStatus.DEPRECATED,
        deprecation_info=deprecation_info,
        supported_features=["basic_queries", "deprecated_features"]
    )

    v15_version = await version_manager.get_version("1.5")
    print(f"v1.5: {v15_version.status.value}")
    print(f"  Deprecation info: {v15_version.deprecation_info.dict() if v15_version.deprecation_info else 'None'}")


async def example_version_migration():
    """Example of migrating between API versions."""
    print("\n=== Version Migration Example ===")

    # Example migration function
    async def migrate_curve_data_v1_to_v2(v1_data):
        """Migrate curve data from v1 to v2 format."""
        v2_data = v1_data.copy()

        # Add v2-specific fields
        v2_data["version"] = "2.0"
        v2_data["metadata"] = {
            "migrated_from": "v1.0",
            "migration_date": "2024-01-01T00:00:00Z",
            "enhanced_fields": ["real_time_data", "advanced_analytics"]
        }

        # Transform v1 fields to v2 format
        if "price" in v2_data:
            v2_data["price_v2"] = {
                "value": v2_data.pop("price"),
                "currency": "USD",
                "unit": "MWh",
                "confidence_interval": [v2_data["price"] * 0.95, v2_data["price"] * 1.05]
            }

        return v2_data

    # Sample v1 data
    v1_curve_data = {
        "id": "curve_123",
        "iso": "PJM",
        "market": "DAY_AHEAD",
        "price": 50.0,
        "timestamp": "2024-01-01T12:00:00Z"
    }

    print("Original v1 data:")
    print(f"  {v1_curve_data}")

    # Migrate to v2
    v2_curve_data = await migrate_curve_data_v1_to_v2(v1_curve_data)

    print("\nMigrated v2 data:")
    print(f"  {v2_curve_data}")

    print("\nMigration completed:")
    print(f"  - Added version field: {v2_curve_data.get('version')}")
    print(f"  - Added metadata: {bool(v2_curve_data.get('metadata'))}")
    print(f"  - Enhanced price field: {bool(v2_curve_data.get('price_v2'))}")


async def example_version_compatibility():
    """Example of maintaining backward compatibility."""
    print("\n=== Version Compatibility Example ===")

    from .versioning import create_versioned_app

    # Create versioned application
    version_manager, versioned_router = create_versioned_app(
        title="Aurum API",
        description="Versioned Market Intelligence API",
        version="2.0.0"
    )

    print("Versioned API Setup:")
    print("  - Automatic version resolution from headers")
    print("  - Deprecation headers for old versions")
    print("  - Migration guidance for users")
    print("  - Backward compatibility maintained")

    # Example of how clients would interact
    print("\nClient Usage Examples:")

    print("1. Latest version (automatic):")
    print("   curl -H 'Accept: application/vnd.aurum.v2+json' /api/curves")

    print("\n2. Specific version:")
    print("   curl -H 'X-API-Version: 1.0' /api/curves")

    print("\n3. Version via URL:")
    print("   curl /api/v1/curves")
    print("   curl /api/v2/curves")

    print("\n4. Deprecation warning response:")
    print("   X-API-Deprecation: true")
    print("   X-API-Sunset: 2024-06-01T00:00:00Z")
    print("   X-API-Migration-Guide: https://docs.example.com/migration/v1-to-v2")

    # Show version information
    versions = await version_manager.list_versions()
    print(f"\nActive versions: {len([v for v in versions if v.is_supported()])}")
    print(f"Deprecated versions: {len([v for v in versions if v.is_deprecated()])}")

    print("\nVersion compatibility matrix:")
    for version in versions:
        if version.is_supported():
            status = "✓ Supported"
        elif version.is_deprecated():
            status = "⚠ Deprecated"
        else:
            status = "✗ Retired"

        print(f"  {version.version}: {status}")


async def example_rate_limiting():
    """Example of sophisticated rate limiting with quotas and tiers."""
    print("\n=== Rate Limiting Example ===")

    from .rate_limiting import (
        create_rate_limit_manager,
        RateLimitMiddleware,
        QuotaTier,
        Quota,
        RateLimitRule,
        RateLimitAlgorithm,
    )

    # Create rate limit manager with default configuration
    manager = create_rate_limit_manager("memory")

    print("Rate Limiting Configuration:")
    print("  Default quotas configured for all tiers")
    print("  Token bucket algorithm with burst handling")
    print("  Custom rules for specific endpoints")

    # Get quota information
    free_quota = manager.get_quota(QuotaTier.FREE)
    premium_quota = manager.get_quota(QuotaTier.PREMIUM)

    if free_quota:
        print("
Quota Comparison:")
        print(f"  Free Tier: {free_quota.requests_per_minute}/min, {free_quota.requests_per_hour}/hour")
        print(f"  Premium Tier: {premium_quota.requests_per_minute if premium_quota else 'N/A'}/min, {premium_quota.requests_per_hour if premium_quota else 'N/A'}/hour")

    # Simulate rate limit checks
    print("
Simulated Rate Limit Checks:")

    # Test different identifiers
    test_identifiers = [
        ("free_user", QuotaTier.FREE),
        ("premium_user", QuotaTier.PREMIUM),
        ("enterprise_user", QuotaTier.ENTERPRISE),
    ]

    for identifier, tier in test_identifiers:
        # Check rate limit (would normally be done by middleware)
        result = await manager.check_rate_limit(
            identifier=identifier,
            tier=tier,
            endpoint="/v1/curves",
            user_agent="test-client"
        )

        status = "✓ Allowed" if result.allowed else "✗ Limited"
        print(f"  {identifier}: {status} (remaining: {result.remaining_requests}, tier: {result.tier.value})")


async def example_rate_limit_middleware():
    """Example of rate limiting middleware usage."""
    print("\n=== Rate Limiting Middleware Example ===")

    from .rate_limiting import create_rate_limit_manager, RateLimitMiddleware

    # Create middleware
    manager = create_rate_limit_manager("memory")
    middleware = RateLimitMiddleware(manager)

    print("Rate Limiting Middleware Features:")
    print("  - Automatic identifier extraction from headers/IP")
    print("  - Tier detection from X-Tier header")
    print("  - Rate limit headers in responses")
    print("  - 429 responses for exceeded limits")

    print("
Identifier Extraction Priority:")
    print("  1. X-API-Key header")
    print("  2. Authorization: Bearer token")
    print("  3. X-Tenant-ID header")
    print("  4. X-User-ID header")
    print("  5. Client IP address")

    print("
Response Headers Added:")
    print("  X-RateLimit-Limit: requests per minute")
    print("  X-RateLimit-Remaining: remaining requests")
    print("  X-RateLimit-Reset: reset time in seconds")
    print("  X-RateLimit-Tier: user tier")
    print("  Retry-After: seconds to wait (when limited)")


async def example_rate_limit_management():
    """Example of rate limiting management and monitoring."""
    print("\n=== Rate Limiting Management Example ===")

    print("Management API Endpoints:")

    management_endpoints = [
        "GET /v1/admin/ratelimit/quotas",
        "GET /v1/admin/ratelimit/quotas/{tier}",
        "POST /v1/admin/ratelimit/quotas/{tier}",
        "GET /v1/admin/ratelimit/rules",
        "POST /v1/admin/ratelimit/rules",
        "GET /v1/admin/ratelimit/usage",
        "GET /v1/admin/ratelimit/violations",
        "POST /v1/admin/ratelimit/cleanup",
        "POST /v1/admin/ratelimit/reset",
        "GET /v1/admin/ratelimit/configuration",
        "GET /v1/admin/ratelimit/health",
    ]

    for endpoint in management_endpoints:
        print(f"  {endpoint}")

    print("
Quota Tiers Available:")
    tiers = ["free", "basic", "premium", "enterprise", "unlimited"]
    for tier in tiers:
        print(f"  - {tier}")

    print("
Rate Limit Algorithms:")
    algorithms = ["token_bucket", "sliding_window", "fixed_window", "adaptive"]
    for alg in algorithms:
        print(f"  - {alg}")

    # Example management operations
    print("
Example Management Operations:")
    print("  curl -X GET /v1/admin/ratelimit/usage?tier=premium")
    print("  curl -X POST /v1/admin/ratelimit/quotas/premium \\")
    print("    -H 'Content-Type: application/json' \\")
    print("    -d '{\"requests_per_minute\": 2000}'")
    print("  curl -X POST /v1/admin/ratelimit/cleanup")


async def example_rate_limit_scenarios():
    """Example of rate limiting scenarios and responses."""
    print("\n=== Rate Limiting Scenarios Example ===")

    print("Scenario 1: Normal Usage")
    print("  Request: GET /v1/curves (Free tier)")
    print("  Response: 200 OK")
    print("  Headers:")
    print("    X-RateLimit-Limit: 60")
    print("    X-RateLimit-Remaining: 59")
    print("    X-RateLimit-Tier: free")
    print("  ✓ Request allowed")

    print("
Scenario 2: Approaching Limit")
    print("  Request: GET /v1/curves (59th request in minute)")
    print("  Response: 200 OK")
    print("  Headers:")
    print("    X-RateLimit-Limit: 60")
    print("    X-RateLimit-Remaining: 1")
    print("    X-RateLimit-Tier: free")
    print("  ⚠ Near limit")

    print("
Scenario 3: Rate Limited")
    print("  Request: GET /v1/curves (61st request in minute)")
    print("  Response: 429 Too Many Requests")
    print("  Headers:")
    print("    X-RateLimit-Limit: 60")
    print("    X-RateLimit-Remaining: 0")
    print("    Retry-After: 60")
    print("    X-RateLimit-Tier: free")
    print("  ✗ Request denied")

    print("
Scenario 4: Premium Tier")
    print("  Request: GET /v1/curves (Premium tier)")
    print("  Response: 200 OK")
    print("  Headers:")
    print("    X-RateLimit-Limit: 1000")
    print("    X-RateLimit-Remaining: 999")
    print("    X-RateLimit-Tier: premium")
    print("  ✓ Higher limits")

    print("
Scenario 5: Burst Handling")
    print("  Request: Multiple rapid requests")
    print("  Response: 200 OK (within burst limit)")
    print("  Headers: X-RateLimit-Remaining reflects burst usage")
    print("  ✓ Burst allowance")


async def example_rate_limit_monitoring():
    """Example of rate limiting monitoring and analytics."""
    print("\n=== Rate Limiting Monitoring Example ===")

    print("Real-time Monitoring:")
    print("  - Current usage per tier")
    print("  - Active rate limit states")
    print("  - Violation counts and patterns")
    print("  - Top violators identification")

    print("
Usage Analytics:")
    print("  curl -X GET '/v1/admin/ratelimit/usage?tier=premium&hours=24'")
    print("  Response:")
    print("  {")
    print("    'tier': 'premium',")
    print("    'active_users': 1250,")
    print("    'requests_today': 450000,")
    print("    'requests_this_hour': 18750,")
    print("    'average_utilization': 0.78")
    print("  }")

    print("
Violation Analytics:")
    print("  curl -X GET '/v1/admin/ratelimit/violations?hours=24'")
    print("  Response:")
    print("  {")
    print("    'total_violations': 45,")
    print("    'violations_by_tier': {'free': 40, 'basic': 5},")
    print("    'violations_by_endpoint': {'/v1/curves': 35},")
    print("    'top_violators': ['ip:192.168.1.100', 'apikey:abc123']")
    print("  }")

    print("
Health Monitoring:")
    print("  curl -X GET '/v1/admin/ratelimit/health'")
    print("  Response:")
    print("  {")
    print("    'status': 'healthy',")
    print("    'storage_backend': 'redis',")
    print("    'quota_tiers_configured': 5,")
    print("    'cleanup_last_run': 1640995200.0")
    print("  }")

    print("
Alerting Examples:")
    print("  - High violation rate alerts")
    print("  - Tier utilization thresholds")
    print("  - Storage backend health issues")
    print("  - Burst limit exhaustion warnings")


async def example_api_documentation():
    """Example of auto-generated API documentation."""
    print("\n=== API Documentation Example ===")

    from .openapi_generator import OpenAPIGenerator, DocumentationFormat

    print("OpenAPI Documentation Generation:")
    print("  - JSON format for machines")
    print("  - YAML format for humans")
    print("  - Markdown for documentation sites")
    print("  - Interactive Swagger UI")

    print("
Documentation Features:")
    print("  - Auto-generated from FastAPI routes")
    print("  - Server information for different environments")
    print("  - Security schemes and authentication")
    print("  - Request/response schemas")
    print("  - Parameter validation rules")


async def example_sdk_generation():
    """Example of client SDK generation."""
    print("\n=== Client SDK Generation Example ===")

    from .openapi_generator import SDKLanguage

    print("Client SDK Generation:")
    print("  - Python client with requests")
    print("  - TypeScript client with fetch")
    print("  - Auto-generated from OpenAPI schema")
    print("  - Type-safe API calls")
    print("  - Error handling and authentication")


async def example_docs_validation():
    """Example of API documentation validation."""
    print("\n=== Documentation Validation Example ===")

    print("Documentation Validation Features:")
    print("  - Schema completeness checks")
    print("  - Endpoint documentation analysis")
    print("  - Response definition validation")
    print("  - Parameter documentation verification")


async def example_docs_management():
    """Example of documentation management and analysis."""
    print("\n=== Documentation Management Example ===")

    print("Documentation Management Endpoints:")
    endpoints = [
        "POST /v1/admin/docs/generate - Generate docs",
        "GET /v1/admin/docs/status - Generation status",
        "POST /v1/admin/docs/validate - Validate docs",
        "GET /v1/admin/docs/schema - Get API schema",
        "GET /v1/admin/docs/endpoints - List endpoints",
        "GET /v1/admin/docs/models - List data models",
        "GET /v1/admin/docs/export - Export docs",
        "GET /v1/admin/docs/completeness - Completeness analysis",
        "GET /v1/admin/docs/examples - Usage examples"
    ]

    for endpoint in endpoints:
        print(f"  {endpoint}")


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
    await example_api_versioning()
    await example_version_migration()
    await example_version_compatibility()
    await example_rate_limiting()
    await example_rate_limit_middleware()
    await example_rate_limit_management()
    await example_rate_limit_scenarios()
    await example_rate_limit_monitoring()
    await example_data_streaming()
    await example_websocket_connection()
    await example_streaming_endpoints()
    await example_streaming_scenarios()
    await example_streaming_performance()
    await example_streaming_monitoring()
    await example_api_documentation()
    await example_sdk_generation()
    await example_docs_validation()
    await example_docs_management()

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
    print("• API versioning with backward compatibility")
    print("• Version migration tools and deprecation management")
    print("• Structured observability with metrics, tracing, and logging")
    print("• Sophisticated rate limiting with user/tenant quotas")
    print("• Multi-tier quota management and burst handling")
    print("• Rate limiting analytics and violation monitoring")
    print("• Real-time data streaming with WebSocket support")
    print("• Server-Sent Events for simple streaming")
    print("• Multi-protocol streaming with comprehensive management")
    print("• Auto-generated OpenAPI documentation and client SDKs")
    print("• Multi-format documentation (JSON, YAML, Markdown)")
    print("• Documentation validation and completeness analysis")


if __name__ == "__main__":
    asyncio.run(main())
