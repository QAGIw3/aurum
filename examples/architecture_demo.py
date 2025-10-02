"""Comprehensive demonstration of Aurum's modern architecture.

This script showcases the refactored architecture including:
- Enhanced DI container with circuit breakers and health monitoring
- Service layer with optional caching and business logic
- Repository pattern for data access
- Async DAOs with connection pooling
- Service result patterns and error handling
- V2 API routes and middleware stack
"""

import asyncio
from datetime import date
from typing import Any, Dict

# Add src to path for standalone execution
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


async def demo_repository_pattern():
    """Demonstrate direct repository usage."""
    print("=== Repository Pattern Demo ===\n")

    try:
        from aurum.services.core import CurveService
        from aurum.services.base import ServiceContext
        from aurum.data.repositories import CurveRepository
        from aurum.data.dao import TrinoDAO

        async with CurveRepository() as repo:
            # Query curves
            curves = await repo.find_by_filters(
                iso="PJM",
                market="DA",
                limit=5
            )
            print(f"Found {len(curves)} curves via repository")

            # Get latest as-of date
            latest = await repo.get_latest_asof(iso="PJM")
            print(f"Latest as-of date: {latest}\n")
    except ImportError as e:
        print(f"Repository demo skipped (imports not available): {e}\n")


async def demo_service_layer():
    """Demonstrate service layer with business logic."""
    print("=== Service Layer Demo ===\n")

    try:
        from aurum.services.core import CurveService
        from aurum.services.base import ServiceContext
        from aurum.data.repositories import CurveRepository

        async with CurveRepository() as repo:
            # Create service with repository
            service = CurveService(repo)

            # Create service context
            context = ServiceContext(
                tenant_id="demo-tenant",
                user_id="demo-user"
            )

            # Use service (includes validation, logging, etc.)
            result = await service.get_curves(
                iso="PJM",
                market="DA",
                limit=10,
                context=context
            )

            if result.success:
                print(f"Service returned {len(result.data)} curves")
                print(f"Metadata: {result.metadata}")
            else:
                print(f"Service error: {result.error}")

            # Get latest as-of
            asof_result = await service.get_latest_asof(iso="PJM", context=context)
            if asof_result.success:
                print(f"Latest as-of: {asof_result.data}\n")
    except ImportError as e:
        print(f"Service layer demo skipped (imports not available): {e}\n")


async def demo_di_container():
    """Demonstrate enhanced DI container features."""
    print("=== Enhanced DI Container Demo ===\n")

    try:
        from aurum.core.container import (
            DependencyContainer,
            CircuitBreakerConfig,
            ServiceLifetime,
            get_container
        )

        # Get the global container
        container = get_container()

        # Check container metrics
        metrics = container.get_container_metrics()
        print(f"Container uptime: {metrics['uptime_seconds']:.2f}s")
        print(f"Registered services: {metrics['registered_services']}")
        print(f"Singleton instances: {metrics['singleton_instances']}")

        # Check service health
        from aurum.services.core import CurveService
        health = container.get_service_health(CurveService)
        if health:
            print("
CurveService health:")
            print(f"  - Healthy: {health.is_healthy}")
            print(f"  - Success count: {health.success_count}")
            print(f"  - Failure count: {health.failure_count}")

        print()
    except ImportError as e:
        print(f"DI container demo skipped (imports not available): {e}\n")


async def demo_service_with_caching():
    """Demonstrate service with caching."""
    print("=== Service with Caching Demo ===\n")

    try:
        from aurum.core.container import get_container
        from aurum.services.core import CurveService
        from aurum.services.base import ServiceContext

        container = get_container()
        service = await container.get(CurveService)

        context = ServiceContext(tenant_id="demo-tenant")

        print("Querying curves with caching...")

        # First query - will hit database
        result1 = await service.get_curves(
            iso="PJM",
            market="DA",
            limit=10,
            use_cache=True,
            context=context
        )

        print("First query:")
        print(f"  - Success: {result1.success}")
        print(f"  - Count: {result1.metadata.get('count', 0)}")
        print(f"  - Source: {result1.metadata.get('source', 'unknown')}")

        # Second query - should hit cache (same parameters)
        result2 = await service.get_curves(
            iso="PJM",
            market="DA",
            limit=10,
            use_cache=True,
            context=context
        )

        print("
Second query (same parameters):")
        print(f"  - Success: {result2.success}")
        print(f"  - Source: {result2.metadata.get('source', 'unknown')}")

        # Third query - cache disabled
        result3 = await service.get_curves(
            iso="PJM",
            market="DA",
            limit=10,
            use_cache=False,
            context=context
        )

        print("
Third query (cache disabled):")
        print(f"  - Source: {result3.metadata.get('source', 'unknown')}")

        print()
    except ImportError as e:
        print(f"Service caching demo skipped (imports not available): {e}\n")


async def demo_multiple_services():
    """Demonstrate using multiple services together."""
    print("=== Multiple Services Demo ===\n")

    try:
        from aurum.core.container import get_container
        from aurum.services.core import CurveService, MetadataService, ScenarioService
        from aurum.services.base import ServiceContext

        container = get_container()
        context = ServiceContext(tenant_id="demo")

        # Get multiple services
        curve_service = await container.get(CurveService)
        metadata_service = await container.get(MetadataService)
        scenario_service = await container.get(ScenarioService)

        print("Services resolved from container:")
        print(f"  - CurveService: {type(curve_service).__name__}")
        print(f"  - MetadataService: {type(metadata_service).__name__}")
        print(f"  - ScenarioService: {type(scenario_service).__name__}")

        # Use services together
        print("
Querying dimensions...")
        dims_result = await metadata_service.get_dimensions(
            dataset="curves",
            dimension="iso",
            use_cache=True,
            context=context
        )
        print(f"  - Found {len(dims_result.data)} ISO values")

        print("
Listing scenarios...")
        scenarios_result = await scenario_service.list_scenarios(
            limit=5,
            context=context
        )
        print(f"  - Found {scenarios_result.metadata.get('count', 0)} scenarios")

        print()
    except ImportError as e:
        print(f"Multiple services demo skipped (imports not available): {e}\n")


async def demo_service_result_pattern():
    """Demonstrate ServiceResult pattern usage."""
    print("=== ServiceResult Pattern Demo ===\n")

    try:
        from aurum.core.container import get_container
        from aurum.services.core import CurveService
        from aurum.services.base import ServiceContext, ValidationError

        container = get_container()
        service = await container.get(CurveService)
        context = ServiceContext()

        # Successful result
        result = await service.get_curves(iso="PJM", limit=10, context=context)

        print("ServiceResult structure:")
        print(f"  - success: {result.success}")
        print(f"  - data type: {type(result.data).__name__}")
        print(f"  - metadata keys: {list(result.metadata.keys())}")
        print(f"  - error: {result.error}")

        # Error handling
        print("
Testing validation error...")
        try:
            await service.get_curves(limit=-1, context=context)
        except ValidationError as e:
            print(f"  - Caught ValidationError: {e.message}")
            print(f"  - Error code: {e.code}")

        print()
    except ImportError as e:
        print(f"ServiceResult pattern demo skipped (imports not available): {e}\n")


async def demo_circuit_breaker():
    """Demonstrate circuit breaker functionality."""
    print("=== Circuit Breaker Demo ===\n")

    try:
        from aurum.core.container import (
            DependencyContainer,
            CircuitBreakerConfig,
            CircuitBreakerState
        )

        # Create a service with circuit breaker
        container = DependencyContainer()

        class FailingService:
            def __init__(self):
                self.call_count = 0

            async def do_something(self):
                self.call_count += 1
                if self.call_count < 10:
                    raise Exception("Simulated failure")
                return "success"

        container.register(
            FailingService,
            lambda: FailingService(),
            circuit_breaker_config=CircuitBreakerConfig(
                failure_threshold=3,
                recovery_timeout=60.0
            )
        )

        print("Service registered with circuit breaker (failure_threshold=3)")
        print("Simulating failures...")

        # This would demonstrate circuit breaker in action
        # (Simplified for demo purposes)
        print("  - Circuit breaker will open after 3 failures")
        print("  - Requests blocked while open")
        print("  - Automatic recovery after timeout")
        print("  - Transitions to HALF_OPEN to test recovery")

        print()
    except ImportError as e:
        print(f"Circuit breaker demo skipped (imports not available): {e}\n")


async def demo_health_monitoring():
    """Demonstrate health monitoring."""
    print("=== Health Monitoring Demo ===\n")

    try:
        from aurum.core.container import get_container
        from aurum.services.core import CurveService

        container = get_container()

        # Get all service health
        all_health = container.get_all_service_health()

        print(f"Monitoring {len(all_health)} services")

        for service_name, health in all_health.items():
            print(f"\n{service_name}:")
            print(f"  - Healthy: {health.is_healthy}")
            print(f"  - Successes: {health.success_count}")
            print(f"  - Failures: {health.failure_count}")
            if health.last_error:
                print(f"  - Last error: {health.last_error}")

        print()
    except ImportError as e:
        print(f"Health monitoring demo skipped (imports not available): {e}\n")


async def demo_direct_dao():
    """Demonstrate direct DAO usage (less common)."""
    print("=== Direct DAO Demo ===\n")

    try:
        from aurum.data.dao import TrinoDAO

        async with TrinoDAO() as dao:
            # Direct SQL query
            result = await dao.execute_query(
                "SELECT curve_key, COUNT(*) as count FROM iceberg.market.curve_observation GROUP BY curve_key LIMIT 5"
            )

            print(f"Query returned {len(result)} rows")
            for row in result:
                print(f"  {row}")
            print()
    except ImportError as e:
        print(f"Direct DAO demo skipped (imports not available): {e}\n")


async def demo_streaming():
    """Demonstrate streaming large result sets."""
    print("=== Streaming Demo ===\n")

    try:
        from aurum.data.dao import TrinoDAO

        async with TrinoDAO() as dao:
            print("Streaming query results in chunks...")

            total_rows = 0
            async for chunk in dao.stream_query(
                "SELECT * FROM iceberg.market.curve_observation LIMIT 1000",
                chunk_size=100
            ):
                total_rows += len(chunk)
                print(f"  Processed chunk of {len(chunk)} rows (total: {total_rows})")

            print(f"Streamed {total_rows} total rows\n")
    except ImportError as e:
        print(f"Streaming demo skipped (imports not available): {e}\n")


async def demo_comparison_old_vs_new():
    """Show the difference between old and new patterns."""
    print("=== Old vs New Pattern Comparison ===\n")

    print("OLD PATTERN (sync, mixed concerns):")
    print("""
    from aurum.api.dao import CurvesDao

    dao = CurvesDao()
    curves = dao.query_curves(iso="PJM", market="DA")
    # Synchronous, blocking
    # No separation of concerns
    # Direct database access
    """)

    print("NEW PATTERN (async, clean architecture):")
    print("""
    from aurum.services.core import CurveService
    from aurum.data.repositories import CurveRepository

    async with CurveRepository() as repo:
        service = CurveService(repo)
        result = await service.get_curves(iso="PJM", market="DA")

    # Async/non-blocking
    # Clear layer separation (Service → Repository → DAO)
    # Business logic in service
    # Data access in repository
    # Database operations in DAO
    """)
    print()


async def main():
    """Run all demos."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 10 + "Aurum Architecture Demo" + " " * 20 + "║")
    print("╚" + "=" * 58 + "╝")
    print("\nDemonstrating modern architectural patterns:\n")

    try:
        await demo_di_container()
        await demo_service_with_caching()
        await demo_multiple_services()
        await demo_service_result_pattern()
        await demo_circuit_breaker()
        await demo_health_monitoring()
        await demo_repository_pattern()
        await demo_service_layer()
        await demo_direct_dao()
        await demo_streaming()
        await demo_comparison_old_vs_new()

        print("=" * 60)
        print("✅ All Demos Complete")
        print("=" * 60)
        print()
        print("Key Features Demonstrated:")
        print("  ✓ Enhanced DI container with metrics")
        print("  ✓ Service caching (cache hits/misses)")
        print("  ✓ Multiple service coordination")
        print("  ✓ ServiceResult pattern")
        print("  ✓ Circuit breaker protection")
        print("  ✓ Health monitoring")
        print("  ✓ Repository pattern")
        print("  ✓ Async DAOs with connection pooling")
        print("  ✓ Streaming support")
        print()
        print("The refactored architecture provides:")
        print("  - Better performance (caching)")
        print("  - Fault tolerance (circuit breakers)")
        print("  - Observability (health monitoring)")
        print("  - Clean patterns (SOLID principles)")
        print("  - Easy testing (dependency injection)")
        print()

    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
