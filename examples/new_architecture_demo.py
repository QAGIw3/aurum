"""Demonstration of the new refactored architecture.

This script shows how to use the new service layer, repository pattern,
and async DAOs in a complete end-to-end flow.
"""

import asyncio
from datetime import date

# New architecture imports
from aurum.services.core import CurveService
from aurum.services.base import ServiceContext
from aurum.data.repositories import CurveRepository
from aurum.data.dao import TrinoDAO


async def demo_repository_pattern():
    """Demonstrate direct repository usage."""
    print("=== Repository Pattern Demo ===\n")
    
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


async def demo_service_layer():
    """Demonstrate service layer with business logic."""
    print("=== Service Layer Demo ===\n")
    
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


async def demo_direct_dao():
    """Demonstrate direct DAO usage (less common)."""
    print("=== Direct DAO Demo ===\n")
    
    async with TrinoDAO() as dao:
        # Direct SQL query
        result = await dao.execute_query(
            "SELECT curve_key, COUNT(*) as count FROM iceberg.market.curve_observation GROUP BY curve_key LIMIT 5"
        )
        
        print(f"Query returned {len(result)} rows")
        for row in result:
            print(f"  {row}")
        print()


async def demo_error_handling():
    """Demonstrate error handling."""
    print("=== Error Handling Demo ===\n")
    
    async with CurveRepository() as repo:
        service = CurveService(repo)
        
        try:
            # Invalid query (no filters)
            result = await service.get_curves()
            print("This shouldn't happen")
        except Exception as e:
            print(f"Caught expected error: {type(e).__name__}: {e}")
        
        try:
            # Curve not found
            result = await service.get_curve_by_key("NONEXISTENT_CURVE")
        except Exception as e:
            print(f"Caught expected error: {type(e).__name__}: {e}")
        
        print()


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


async def demo_streaming():
    """Demonstrate streaming large result sets."""
    print("=== Streaming Demo ===\n")
    
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


async def main():
    """Run all demos."""
    print("\n" + "="*60)
    print("NEW ARCHITECTURE DEMONSTRATION")
    print("="*60 + "\n")
    
    try:
        await demo_repository_pattern()
        await demo_service_layer()
        await demo_direct_dao()
        await demo_error_handling()
        await demo_comparison_old_vs_new()
        await demo_streaming()
        
        print("="*60)
        print("DEMO COMPLETE")
        print("="*60 + "\n")
        
        print("Key Benefits of New Architecture:")
        print("  ✓ Async-first for better performance")
        print("  ✓ Clean separation of concerns")
        print("  ✓ SOLID principles enforced")
        print("  ✓ Easy to test with mocks")
        print("  ✓ Connection pooling built-in")
        print("  ✓ Consistent error handling")
        print("  ✓ Streaming support for large datasets")
        print()
        
    except Exception as e:
        print(f"\nDemo error: {e}")
        print("Note: Demos require actual database connections.")
        print("Set up databases or mock for testing.")


if __name__ == "__main__":
    asyncio.run(main())

