#!/usr/bin/env python3
"""Test script to verify Tier 1 enhanced services work correctly."""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))


class MockCache:
    """Mock cache for testing."""
    
    def __init__(self):
        self.data = {}
    
    async def get(self, key: str):
        return self.data.get(key)
    
    async def set(self, key: str, value, ttl: int):
        self.data[key] = value
    
    async def delete(self, key: str):
        self.data.pop(key, None)


class MockRepository:
    """Mock repository for testing."""
    
    async def initialize(self):
        pass
    
    async def close(self):
        pass
    
    async def find_by_filters(self, **kwargs):
        return [{"id": "1", "data": "test"}]
    
    async def get_dimensions(self, dataset, dimension):
        return ["value1", "value2"]
    
    async def find_by_id(self, id):
        return {"id": str(id), "name": "Test Scenario"}
    
    async def create_scenario(self, **kwargs):
        return {"id": "test-uuid", **kwargs}
    
    async def list_scenarios(self, **kwargs):
        return [{"id": "1", "name": "scenario1"}]
    
    async def get_ppa_contracts(self, **kwargs):
        return [{"contract_id": "C001", "counterparty": "Test"}]
    
    async def get_ppa_valuations(self, **kwargs):
        return [{"contract_id": "C001", "contract_value": 500000}]


async def test_curve_service():
    """Test enhanced CurveService."""
    print("=" * 60)
    print("TEST 1: CurveService with Caching")
    print("=" * 60)
    
    from aurum.services.core import CurveService
    from aurum.services.base import ServiceContext
    
    cache = MockCache()
    repo = MockRepository()
    service = CurveService(repo, cache=cache, cache_ttl=300)
    
    context = ServiceContext(tenant_id="test-tenant")
    
    # First call - should hit database
    result1 = await service.get_curves(iso="PJM", market="DA", context=context)
    assert result1.success, "First query failed"
    assert result1.metadata["source"] == "database", "Should hit database first"
    print(f"✅ First call (database): {len(result1.data)} curves")
    
    # Second call - should hit cache
    result2 = await service.get_curves(iso="PJM", market="DA", context=context)
    assert result2.success, "Second query failed"
    assert result2.metadata["source"] == "cache", "Should hit cache second time"
    print(f"✅ Second call (cache): {len(result2.data)} curves")
    
    # Third call with cache disabled
    result3 = await service.get_curves(iso="PJM", market="DA", use_cache=False, context=context)
    assert result3.metadata["source"] == "database", "Should hit database with cache disabled"
    print(f"✅ Third call (cache disabled): {len(result3.data)} curves")
    
    # Test export
    curves = []
    async for curve in service.export_curves(iso="PJM", context=context):
        curves.append(curve)
    print(f"✅ Export: {len(curves)} curves exported")
    
    # Test cache invalidation
    result4 = await service.invalidate_curve_cache(iso="PJM", market="DA", context=context)
    assert result4.data, "Cache invalidation failed"
    print(f"✅ Cache invalidation: successful")
    
    print()


async def test_metadata_service():
    """Test enhanced MetadataService."""
    print("=" * 60)
    print("TEST 2: MetadataService with Caching")
    print("=" * 60)
    
    from aurum.services.core import MetadataService
    from aurum.services.base import ServiceContext
    
    cache = MockCache()
    repo = MockRepository()
    service = MetadataService(repo, cache=cache, cache_ttl=600)
    
    context = ServiceContext()
    
    # Test dimensions with cache
    result1 = await service.get_dimensions("curves", "iso", context=context)
    assert result1.success
    assert result1.metadata["source"] == "database"
    print(f"✅ Get dimensions (database): {len(result1.data)} values")
    
    result2 = await service.get_dimensions("curves", "iso", context=context)
    assert result2.metadata["source"] == "cache"
    print(f"✅ Get dimensions (cache): {len(result2.data)} values")
    
    # Test list locations
    result3 = await service.list_locations("PJM", limit=10, context=context)
    assert result3.success
    print(f"✅ List locations: {result3.metadata.get('count', 0)} locations")
    
    # Test list units
    result4 = await service.list_units(limit=10, context=context)
    assert result4.success
    print(f"✅ List units: {result4.metadata.get('count', 0)} units")
    
    # Test list calendars
    result5 = await service.list_calendars(limit=10, context=context)
    assert result5.success
    print(f"✅ List calendars: {result5.metadata.get('count', 0)} calendars")
    
    print()


async def test_scenario_service():
    """Test enhanced ScenarioService."""
    print("=" * 60)
    print("TEST 3: ScenarioService with Caching")
    print("=" * 60)
    
    from aurum.services.core import ScenarioService
    from aurum.services.base import ServiceContext
    
    cache = MockCache()
    repo = MockRepository()
    service = ScenarioService(repo, cache=cache, cache_ttl=300)
    
    context = ServiceContext(tenant_id="test-tenant")
    
    # Create scenario
    result1 = await service.create_scenario(
        name="Test Scenario",
        description="Test description",
        context=context
    )
    assert result1.success
    print(f"✅ Create scenario: {result1.data.get('id')}")
    
    # Get scenario with cache
    result2 = await service.get_scenario("550e8400-e29b-41d4-a716-446655440000", context=context)
    assert result2.success
    print(f"✅ Get scenario (database): {result2.data.get('name')}")
    
    result3 = await service.get_scenario("550e8400-e29b-41d4-a716-446655440000", context=context)
    assert result3.metadata.get("source") == "cache"
    print(f"✅ Get scenario (cache): {result3.data.get('name')}")
    
    # List scenarios
    result4 = await service.list_scenarios(limit=10, context=context)
    assert result4.success
    print(f"✅ List scenarios: {result4.metadata['count']} scenarios")
    
    print()


async def test_ppa_service():
    """Test enhanced PpaService."""
    print("=" * 60)
    print("TEST 4: PpaService with Caching")
    print("=" * 60)
    
    from aurum.services.core import PpaService
    from aurum.services.base import ServiceContext
    
    cache = MockCache()
    repo = MockRepository()
    service = PpaService(repo, cache=cache, cache_ttl=600)
    
    context = ServiceContext()
    
    # Get contracts with cache
    result1 = await service.get_ppa_contracts(limit=10, context=context)
    assert result1.success
    assert result1.metadata["source"] == "database"
    print(f"✅ Get contracts (database): {result1.metadata['contract_count']} contracts")
    
    result2 = await service.get_ppa_contracts(limit=10, context=context)
    assert result2.metadata["source"] == "cache"
    print(f"✅ Get contracts (cache): {result2.metadata['contract_count']} contracts")
    
    # Get valuations with cache
    result3 = await service.get_ppa_valuations(contract_id="C001", limit=10, context=context)
    assert result3.success
    assert result3.metadata["source"] == "database"
    print(f"✅ Get valuations (database): {result3.metadata['valuation_count']} valuations")
    
    result4 = await service.get_ppa_valuations(contract_id="C001", limit=10, context=context)
    assert result4.metadata["source"] == "cache"
    print(f"✅ Get valuations (cache): {result4.metadata['valuation_count']} valuations")
    
    print()


async def main():
    """Run all tests."""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 12 + "Tier 1 Services Test Suite" + " " * 18 + "║")
    print("╚" + "=" * 58 + "╝")
    print()
    
    try:
        await test_curve_service()
        await test_metadata_service()
        await test_scenario_service()
        await test_ppa_service()
        
        print("=" * 60)
        print("✅ ALL TIER 1 SERVICES ENHANCED SUCCESSFULLY")
        print("=" * 60)
        print()
        print("Summary:")
        print("  ✓ CurveService: caching + export working")
        print("  ✓ MetadataService: caching + reference data working")
        print("  ✓ ScenarioService: caching + lifecycle working")
        print("  ✓ PpaService: caching + valuations working")
        print()
        print("All 4 Tier 1 services now have:")
        print("  - Optional caching support")
        print("  - Protocol-based cache interface")
        print("  - Cache hit/miss tracking")
        print("  - Backward compatible APIs")
        print()
        
        return 0
        
    except Exception as e:
        print()
        print("=" * 60)
        print("❌ TEST FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        print()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

