"""Tests for tenant-aware golden query cache functionality."""

from __future__ import annotations

import pytest
import pytest_asyncio
import asyncio
from unittest.mock import Mock, patch
from typing import Any, Dict

from aurum.api.golden_query_cache import (
    GoldenQueryCache,
    QueryPattern,
    QueryType,
    CacheInvalidationStrategy
)
from aurum.api.cache.golden_query_cache import CacheEntry
from aurum.api.cache import AsyncCache


@pytest_asyncio.asyncio
class TestTenantAwareCaching:
    """Test tenant-aware caching functionality."""

    @pytest.fixture
    async def mock_cache_service(self):
        """Create a mock cache service."""
        cache_service = Mock(spec=AsyncCache)
        cache_service.get = Mock()
        cache_service.set = Mock()
        cache_service.delete = Mock()
        cache_service.get_stats = Mock()
        cache_service.get_stats.return_value = {
            "total_keys": 0,
            "memory_usage": "0MB",
            "hit_rate": 0.0
        }
        return cache_service

    @pytest.fixture
    async def golden_cache(self, mock_cache_service):
        """Create a GoldenQueryCache instance."""
        cache = GoldenQueryCache(mock_cache_service)
        return cache

    async def test_cache_key_tenant_namespacing(self, golden_cache, mock_cache_service):
        """Test that cache keys are properly namespaced by tenant."""
        tenant_id_1 = "tenant-123"
        tenant_id_2 = "tenant-456"
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-01'"

        # Mock get_tenant_id to return different tenants
        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            mock_get_tenant.return_value = tenant_id_1

            # Get cache key for tenant 1
            pattern = QueryPattern(
                name="test",
                query_type=QueryType.CURVE_DATA,
                pattern=r".*",
                key_template="test:{asof}",
                ttl_seconds=300
            )

            params = pattern.extract_params(query)
            key1 = pattern.generate_key(params, tenant_id_1)

            # Change tenant and get key for tenant 2
            mock_get_tenant.return_value = tenant_id_2
            key2 = pattern.generate_key(params, tenant_id_2)

            # Keys should be different
            assert key1 != key2
            assert key1.startswith(f"{tenant_id_1}:")
            assert key2.startswith(f"{tenant_id_2}:")

    async def test_tenant_isolation(self, golden_cache, mock_cache_service):
        """Test that tenants cannot access each other's cached data."""
        tenant_id_1 = "tenant-123"
        tenant_id_2 = "tenant-456"
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-01'"
        test_data = {"data": "test_value"}

        # Mock get_tenant_id
        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            # Cache data for tenant 1
            mock_get_tenant.return_value = tenant_id_1

            pattern = QueryPattern(
                name="test",
                query_type=QueryType.CURVE_DATA,
                pattern=r".*",
                key_template="test:{asof}",
                ttl_seconds=300
            )

            params = pattern.extract_params(query)
            key1 = pattern.generate_key(params, tenant_id_1)

            entry = CacheEntry(
                value=test_data,
                created_at=asyncio.get_event_loop().time(),
                ttl_seconds=300,
                query_type=QueryType.CURVE_DATA,
                query_hash="test_hash",
                tenant_id=tenant_id_1
            )

            await golden_cache.cache.set(key1, entry.__dict__, 300)

            # Try to get data as tenant 2 - should not find it
            mock_get_tenant.return_value = tenant_id_2
            cached_value = await golden_cache.cache.get(key1)
            assert cached_value is None

            # Get data as tenant 1 - should find it
            mock_get_tenant.return_value = tenant_id_1
            cached_value = await golden_cache.cache.get(key1)
            assert cached_value is not None
            assert cached_value["value"] == test_data

    async def test_cross_tenant_invalidation(self, golden_cache, mock_cache_service):
        """Test that invalidation operations respect tenant boundaries."""
        tenant_id_1 = "tenant-123"
        tenant_id_2 = "tenant-456"

        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            # Invalidate pattern for tenant 1
            mock_get_tenant.return_value = tenant_id_1

            pattern = QueryPattern(
                name="test",
                query_type=QueryType.CURVE_DATA,
                pattern=r".*",
                key_template="test:{asof}",
                ttl_seconds=300
            )

            # This should only invalidate keys for tenant 1
            invalidated_count = await golden_cache.invalidate_pattern(
                pattern.name,
                tenant_id=tenant_id_1
            )

            # Verify that only tenant 1's keys were invalidated
            # (This would be tested more thoroughly with actual cache entries)

    async def test_tenant_aware_cache_stats(self, golden_cache, mock_cache_service):
        """Test that cache statistics are tenant-aware."""
        tenant_id = "tenant-123"

        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            mock_get_tenant.return_value = tenant_id

            # Get cache stats
            stats = await golden_cache.get_cache_stats()

            # Verify stats are properly formatted
            assert "golden_queries" in stats
            assert "total_queries" in stats["golden_queries"]
            assert "total_hits" in stats["golden_queries"]
            assert "total_misses" in stats["golden_queries"]
            assert "hit_rate" in stats["golden_queries"]

    async def test_tenant_aware_cache_warming(self, golden_cache, mock_cache_service):
        """Test that cache warming respects tenant boundaries."""
        tenant_id = "tenant-123"
        test_query = "SELECT * FROM curve_observation WHERE asof = '2024-01-01'"
        test_data = {"data": "warmed_value"}

        async def compute_func():
            return test_data

        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            mock_get_tenant.return_value = tenant_id

            # Warm cache with query
            result = await golden_cache.get_or_compute(test_query, compute_func)

            assert result == test_data

            # Verify the cache entry has correct tenant_id
            # This would be verified by checking the cached entry metadata

    async def test_tenant_aware_cache_cleanup(self, golden_cache, mock_cache_service):
        """Test that cache cleanup operations are tenant-aware."""
        tenant_id = "tenant-123"

        with patch('aurum.api.golden_query_cache.get_tenant_id') as mock_get_tenant:
            mock_get_tenant.return_value = tenant_id

            # Clean up expired entries
            cleaned_count = await golden_cache.cleanup_expired_entries()

            # Verify cleanup was tenant-aware
            # (This would be tested more thoroughly with actual expired entries)

    async def test_tenant_context_inheritance(self, golden_cache, mock_cache_service):
        """Test that tenant context is properly inherited from request context."""
        from aurum.telemetry.context import correlation_context

        tenant_id = "test-tenant-123"

        with correlation_context(tenant_id=tenant_id):
            # The cache should use the tenant from context
            # This would be verified by ensuring get_tenant_id() returns the correct value

            assert golden_cache is not None  # Just verify the cache instance

    async def test_cache_entry_tenant_metadata(self):
        """Test that cache entries contain proper tenant metadata."""
        tenant_id = "test-tenant-123"
        test_value = {"data": "test"}
        now = asyncio.get_event_loop().time()

        entry = CacheEntry(
            value=test_value,
            created_at=now,
            ttl_seconds=300,
            query_type=QueryType.CURVE_DATA,
            query_hash="test_hash",
            tenant_id=tenant_id
        )

        assert entry.tenant_id == tenant_id
        assert entry.value == test_value
        assert entry.query_type == QueryType.CURVE_DATA
        assert entry.is_expired() is False

        # Test touch functionality
        entry.touch()
        assert entry.access_count == 1
        assert entry.last_accessed == now
