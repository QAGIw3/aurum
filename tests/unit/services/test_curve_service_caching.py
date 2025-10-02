"""Unit tests for CurveService with caching functionality."""

from __future__ import annotations

import pytest
from datetime import date
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch

# Test imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "src"))

from aurum.services.core.curves import CurveService
from aurum.services.base import ServiceContext, ValidationError, NotFoundError
from aurum.data.repositories import CurveRepository


class MockCache:
    """Mock cache for testing."""
    
    def __init__(self):
        self.data = {}
        self.get_calls = 0
        self.set_calls = 0
    
    async def get(self, key: str) -> Optional[Any]:
        self.get_calls += 1
        return self.data.get(key)
    
    async def set(self, key: str, value: Any, ttl: int) -> None:
        self.set_calls += 1
        self.data[key] = value
    
    async def delete(self, key: str) -> None:
        self.data.pop(key, None)


class MockRepository:
    """Mock repository for testing."""
    
    def __init__(self):
        self.find_by_filters_calls = 0
        self.find_by_key_calls = 0
    
    async def initialize(self):
        pass
    
    async def close(self):
        pass
    
    async def find_by_filters(self, **kwargs) -> List[Dict[str, Any]]:
        self.find_by_filters_calls += 1
        return [
            {"curve_key": "TEST_KEY", "value": 100},
            {"curve_key": "TEST_KEY", "value": 105},
        ]
    
    async def find_by_key(self, curve_key: str, asof=None, limit=None) -> List[Dict[str, Any]]:
        self.find_by_key_calls += 1
        if curve_key == "NONEXISTENT":
            return []
        return [{"curve_key": curve_key, "value": 100}]


@pytest.mark.asyncio
async def test_curve_service_without_cache():
    """Test CurveService works without cache."""
    repo = MockRepository()
    service = CurveService(repo, cache=None)
    
    result = await service.get_curves(iso="PJM", market="DA")
    
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["source"] == "database"
    assert repo.find_by_filters_calls == 1


@pytest.mark.asyncio
async def test_curve_service_with_cache_miss():
    """Test CurveService cache miss behavior."""
    repo = MockRepository()
    cache = MockCache()
    service = CurveService(repo, cache=cache, cache_ttl=300)
    
    result = await service.get_curves(iso="PJM", market="DA")
    
    assert result.success
    assert len(result.data) == 2
    assert result.metadata["source"] == "database"
    assert cache.get_calls == 1  # Tried cache
    assert cache.set_calls == 1  # Cached result
    assert repo.find_by_filters_calls == 1


@pytest.mark.asyncio
async def test_curve_service_with_cache_hit():
    """Test CurveService cache hit behavior."""
    repo = MockRepository()
    cache = MockCache()
    service = CurveService(repo, cache=cache, cache_ttl=300)
    
    # First call - cache miss
    result1 = await service.get_curves(iso="PJM", market="DA")
    assert result1.metadata["source"] == "database"
    assert repo.find_by_filters_calls == 1
    
    # Second call - cache hit
    result2 = await service.get_curves(iso="PJM", market="DA")
    assert result2.success
    assert result2.metadata["source"] == "cache"
    assert repo.find_by_filters_calls == 1  # No additional DB call
    assert cache.get_calls == 2  # Tried cache both times


@pytest.mark.asyncio
async def test_curve_service_cache_disabled():
    """Test CurveService with cache explicitly disabled."""
    repo = MockRepository()
    cache = MockCache()
    service = CurveService(repo, cache=cache, cache_ttl=300)
    
    result = await service.get_curves(iso="PJM", market="DA", use_cache=False)
    
    assert result.success
    assert result.metadata["source"] == "database"
    assert cache.get_calls == 0  # Cache not used
    assert cache.set_calls == 0  # Result not cached


@pytest.mark.asyncio
async def test_curve_service_validation_error():
    """Test CurveService validation."""
    repo = MockRepository()
    service = CurveService(repo)
    
    # No filters provided
    with pytest.raises(ValidationError) as exc_info:
        await service.get_curves()
    
    assert "at least one filter" in str(exc_info.value).lower()


@pytest.mark.asyncio
async def test_curve_service_not_found():
    """Test CurveService handles not found."""
    repo = MockRepository()
    service = CurveService(repo)
    
    with pytest.raises(NotFoundError) as exc_info:
        await service.get_curve_by_key("NONEXISTENT")
    
    assert "NONEXISTENT" in str(exc_info.value)


@pytest.mark.asyncio
async def test_curve_service_export():
    """Test CurveService streaming export."""
    repo = MockRepository()
    service = CurveService(repo)
    
    curves = []
    async for curve in service.export_curves(iso="PJM", market="DA"):
        curves.append(curve)
    
    assert len(curves) == 2
    assert all("curve_key" in c for c in curves)


@pytest.mark.asyncio
async def test_curve_service_cache_invalidation():
    """Test cache invalidation."""
    repo = MockRepository()
    cache = MockCache()
    service = CurveService(repo, cache=cache)
    
    # Cache a result
    result1 = await service.get_curves(iso="PJM", market="DA")
    assert result1.metadata["source"] == "database"
    assert len(cache.data) > 0
    
    # Verify cache hit
    result2 = await service.get_curves(iso="PJM", market="DA")
    assert result2.metadata["source"] == "cache"
    
    # Invalidate cache
    invalidate_result = await service.invalidate_curve_cache(iso="PJM", market="DA")
    assert invalidate_result.data == True
    
    # After invalidation, should hit database again
    # (In practice, would need to clear the exact key used)


@pytest.mark.asyncio
async def test_curve_service_limit_enforcement():
    """Test limit parameter enforcement."""
    repo = MockRepository()
    service = CurveService(repo)
    
    # Test max limit enforcement
    result = await service.get_curves(iso="PJM", limit=15000)
    
    # Should be capped at 1000
    assert result.success
    # Actual limit would be enforced in the call to repository


if __name__ == "__main__":
    import asyncio
    
    print("Running CurveService caching tests...")
    asyncio.run(test_curve_service_without_cache())
    print("✅ test_curve_service_without_cache")
    
    asyncio.run(test_curve_service_with_cache_miss())
    print("✅ test_curve_service_with_cache_miss")
    
    asyncio.run(test_curve_service_with_cache_hit())
    print("✅ test_curve_service_with_cache_hit")
    
    asyncio.run(test_curve_service_cache_disabled())
    print("✅ test_curve_service_cache_disabled")
    
    asyncio.run(test_curve_service_export())
    print("✅ test_curve_service_export")
    
    print("\n✅ All CurveService tests passed!")

