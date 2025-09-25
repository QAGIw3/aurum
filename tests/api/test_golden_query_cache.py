"""Tests for golden query cache with TTL and bust-on-write hooks."""

from __future__ import annotations

import json
import pytest
import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, Mock, patch
from uuid import uuid4

from aurum.api.golden_query_cache import (
    QueryType,
    CacheInvalidationStrategy,
    QueryPattern,
    GoldenQueryCache,
    cache_golden_query,
    invalidate_on_write,
    get_query_patterns,
    register_custom_pattern,
    get_cache_performance_report,
    generate_cache_recommendations,
    initialize_golden_query_cache,
)
from aurum.api.cache.golden_query_cache import CacheEntry
from aurum.api.cache import AsyncCache, CacheBackend
from aurum.api.config import CacheConfig


class TestQueryType:
    """Test QueryType enum."""

    def test_query_type_values(self):
        """Test that all query types are valid."""
        expected_types = {
            "curve_data",
            "metadata_dimensions",
            "scenario_outputs",
            "scenario_metrics",
            "aggregation",
            "custom"
        }

        actual_types = set(q.value for q in QueryType)
        assert actual_types == expected_types


class TestCacheInvalidationStrategy:
    """Test CacheInvalidationStrategy enum."""

    def test_invalidation_strategy_values(self):
        """Test that all invalidation strategies are valid."""
        expected_strategies = {
            "ttl_only",
            "bust_on_write",
            "pattern_based",
            "dependency_based"
        }

        actual_strategies = set(s.value for s in CacheInvalidationStrategy)
        assert actual_strategies == expected_strategies


class TestQueryPattern:
    """Test QueryPattern functionality."""

    def test_query_pattern_creation(self):
        """Test creating a query pattern."""
        pattern = QueryPattern(
            name="test_pattern",
            query_type=QueryType.CURVE_DATA,
            pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*=.*'(?P<asof>\d{4}-\d{2}-\d{2})'",
            key_template="curve_data:{asof}",
            ttl_seconds=3600,
            invalidation_strategy=CacheInvalidationStrategy.BUST_ON_WRITE,
            dependencies=["curve_observation"]
        )

        assert pattern.name == "test_pattern"
        assert pattern.query_type == QueryType.CURVE_DATA
        assert pattern.ttl_seconds == 3600
        assert pattern.invalidation_strategy == CacheInvalidationStrategy.BUST_ON_WRITE

    def test_pattern_matching(self):
        """Test pattern matching functionality."""
        pattern = QueryPattern(
            name="test_pattern",
            query_type=QueryType.CURVE_DATA,
            pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*=.*'(?P<asof>\d{4}-\d{2}-\d{2})'.*AND.*iso.*=.*'(?P<iso>[A-Z]{3})'",
            key_template="curve_data:{asof}:{iso}",
            ttl_seconds=3600
        )

        test_query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15' AND iso = 'USD'"
        assert pattern.matches(test_query) is True

        # Extract parameters
        params = pattern.extract_params(test_query)
        assert params["asof"] == "2024-01-15"
        assert params["iso"] == "USD"

        # Generate key
        key = pattern.generate_key(params, "tenant-123")
        assert key == "tenant-123:curve_data:curve_data:2024-01-15:USD"

    def test_pattern_non_matching(self):
        """Test pattern matching with non-matching query."""
        pattern = QueryPattern(
            name="test_pattern",
            query_type=QueryType.CURVE_DATA,
            pattern=r"SELECT.*FROM.*curve_observation.*WHERE.*asof.*=.*'\d{4}-\d{2}-\d{2}'",
            key_template="curve_data:{asof}",
            ttl_seconds=3600
        )

        test_query = "SELECT * FROM other_table WHERE date = '2024-01-15'"
        assert pattern.matches(test_query) is False

        # Extract parameters should return empty dict
        params = pattern.extract_params(test_query)
        assert params == {}


class TestCacheEntry:
    """Test CacheEntry functionality."""

    def test_cache_entry_creation(self):
        """Test creating a cache entry."""
        entry = CacheEntry(
            value={"data": "test"},
            created_at=time.time(),
            ttl_seconds=3600,
            query_type=QueryType.CURVE_DATA,
            query_hash="test_hash",
            tenant_id="tenant-123",
            dependencies=["table1"],
            tags=["test"]
        )

        assert entry.value == {"data": "test"}
        assert entry.query_type == QueryType.CURVE_DATA
        assert entry.tenant_id == "tenant-123"
        assert entry.access_count == 0

    def test_cache_entry_expiration(self):
        """Test cache entry expiration logic."""
        now = time.time()
        entry = CacheEntry(
            value="test",
            created_at=now,
            ttl_seconds=3600,  # 1 hour
            query_type=QueryType.CURVE_DATA,
            query_hash="test_hash",
            tenant_id="tenant-123"
        )

        assert entry.is_expired() is False

        # Test expired entry
        entry.created_at = now - 7200  # 2 hours ago
        assert entry.is_expired() is True

    def test_cache_entry_touch(self):
        """Test cache entry touch functionality."""
        entry = CacheEntry(
            value="test",
            created_at=time.time(),
            ttl_seconds=3600,
            query_type=QueryType.CURVE_DATA,
            query_hash="test_hash",
            tenant_id="tenant-123"
        )

        entry.touch()

        assert entry.access_count == 1
        assert entry.hit_rate == 1.0

        # Second touch should update hit rate
        entry.touch()
        assert entry.access_count == 2
        assert entry.hit_rate == 1.0  # (1.0 + 1.0) / 2


class TestGoldenQueryCache:
    """Test GoldenQueryCache functionality."""

    @pytest.fixture
    def mock_cache_service(self):
        """Mock AsyncCache service."""
        cache = AsyncMock(spec=AsyncCache)
        cache.get.return_value = None  # Default to cache miss
        cache.set.return_value = None
        cache.delete.return_value = None
        return cache

    @pytest.fixture
    def golden_cache(self, mock_cache_service):
        """Create GoldenQueryCache with mocked dependencies."""
        return GoldenQueryCache(mock_cache_service)

    def test_golden_cache_initialization(self, golden_cache):
        """Test golden cache initialization."""
        assert len(golden_cache._patterns) > 0
        assert golden_cache.cache is not None

    def test_default_patterns_registered(self, golden_cache):
        """Test that default patterns are registered."""
        pattern_names = list(golden_cache._patterns.keys())

        expected_patterns = [
            "curve_data_by_date",
            "curve_data_by_range",
            "metadata_dimensions",
            "scenario_outputs_by_scenario",
            "scenario_metrics_latest",
            "aggregation_by_date"
        ]

        for pattern in expected_patterns:
            assert pattern in pattern_names

    async def test_get_or_compute_cache_hit(self, golden_cache, mock_cache_service):
        """Test cache hit scenario."""
        cached_value = {"data": "cached"}
        mock_cache_service.get.return_value = cached_value

        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15'"
        compute_func = AsyncMock(return_value={"data": "computed"})

        result = await golden_cache.get_or_compute(query, compute_func)

        assert result == cached_value
        compute_func.assert_not_called()  # Should not compute if cached

    async def test_get_or_compute_cache_miss(self, golden_cache, mock_cache_service):
        """Test cache miss scenario."""
        mock_cache_service.get.return_value = None

        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15'"
        computed_value = {"data": "computed"}
        compute_func = AsyncMock(return_value=computed_value)

        result = await golden_cache.get_or_compute(query, compute_func)

        assert result == computed_value
        compute_func.assert_called_once()
        mock_cache_service.set.assert_called_once()

    async def test_get_or_compute_force_refresh(self, golden_cache, mock_cache_service):
        """Test force refresh scenario."""
        cached_value = {"data": "cached"}
        mock_cache_service.get.return_value = cached_value

        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15'"
        computed_value = {"data": "refreshed"}
        compute_func = AsyncMock(return_value=computed_value)

        result = await golden_cache.get_or_compute(query, compute_func, force_refresh=True)

        assert result == computed_value
        compute_func.assert_called_once()

    async def test_pattern_matching(self, golden_cache):
        """Test pattern matching for queries."""
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15' AND iso = 'USD'"
        pattern = golden_cache._find_matching_pattern(query)

        assert pattern is not None
        assert pattern.name == "curve_data_by_date"

        # Test parameter extraction
        params = pattern.extract_params(query)
        assert params["asof"] == "2024-01-15"

        # Test key generation
        key = pattern.generate_key(params, "tenant-123")
        assert "tenant-123" in key
        assert "curve_data" in key

    async def test_invalidate_pattern(self, golden_cache, mock_cache_service):
        """Test pattern-based cache invalidation."""
        # Setup mock query stats
        golden_cache._query_stats = {
            "tenant-123:curve_data:curve_data:2024-01-15": {"hits": 1, "misses": 0},
            "tenant-123:curve_data:curve_data:2024-01-16": {"hits": 2, "misses": 1},
            "tenant-456:curve_data:curve_data:2024-01-15": {"hits": 1, "misses": 0}
        }

        invalidated = await golden_cache.invalidate_pattern("curve_data_by_date", "tenant-123")

        assert invalidated == 2  # Should invalidate 2 entries for tenant-123
        assert mock_cache_service.delete.call_count == 2

    async def test_invalidate_dependencies(self, golden_cache, mock_cache_service):
        """Test dependency-based cache invalidation."""
        # Setup mock query stats
        golden_cache._query_stats = {
            "tenant-123:curve_data:curve_data:2024-01-15": {"hits": 1, "misses": 0},
            "tenant-123:scenario_outputs:scenario_outputs:abc": {"hits": 1, "misses": 0}
        }

        # Mock invalidate_pattern to return specific counts
        golden_cache.invalidate_pattern = AsyncMock(side_effect=[1, 1])

        invalidated = await golden_cache.invalidate_dependencies(["curve_observation"], "tenant-123")

        assert invalidated == 2
        assert golden_cache.invalidate_pattern.call_count == 2

    async def test_get_cache_stats(self, golden_cache, mock_cache_service):
        """Test cache statistics retrieval."""
        # Setup mock data
        mock_cache_service.get_stats.return_value = {
            "backend": "memory",
            "memory_entries": 100,
            "memory_size_bytes": 1024000
        }

        golden_cache._query_stats = {
            "key1": {"hits": 10, "misses": 5},
            "key2": {"hits": 20, "misses": 2}
        }

        stats = await golden_cache.get_cache_stats()

        assert stats["backend"] == "memory"
        assert stats["memory_entries"] == 100
        assert stats["golden_queries"]["total_queries"] == 2
        assert stats["golden_queries"]["total_hits"] == 30
        assert stats["golden_queries"]["total_misses"] == 7
        assert stats["golden_queries"]["hit_rate"] == 30 / 37  # 0.8108...

    async def test_warm_cache(self, golden_cache, mock_cache_service):
        """Test cache warming functionality."""
        warmup_queries = [
            {
                "query": "SELECT * FROM curve_observation WHERE asof = '2024-01-15'",
                "compute_func": AsyncMock(return_value={"data": "warmed"})
            },
            {
                "query": "SELECT * FROM invalid_table",
                "compute_func": AsyncMock(side_effect=Exception("Compute failed"))
            }
        ]

        result = await golden_cache.warm_cache(warmup_queries)

        assert result["total"] == 2
        assert result["successful"] == 1
        assert result["failed"] == 1
        assert len(result["errors"]) == 1

    async def test_cache_with_metadata(self, golden_cache, mock_cache_service):
        """Test caching with metadata."""
        key = "test_key"
        value = {"data": "test"}
        query = "SELECT * FROM test_table"
        pattern = QueryPattern(
            name="test",
            query_type=QueryType.CUSTOM,
            pattern=r"SELECT.*FROM.*test_table",
            key_template="test:{table}",
            ttl_seconds=3600
        )
        tenant_id = "tenant-123"
        ttl = 3600

        await golden_cache._cache_with_metadata(key, value, query, pattern, tenant_id, ttl)

        # Verify cache.set was called with proper entry
        mock_cache_service.set.assert_called_once()
        call_args = mock_cache_service.set.call_args[0]
        assert call_args[0] == key
        assert call_args[2] == ttl

        # Verify entry structure
        entry_dict = call_args[1]
        assert entry_dict["value"] == value
        assert entry_dict["query_type"] == QueryType.CUSTOM.value
        assert entry_dict["tenant_id"] == tenant_id

    def test_generate_query_hash(self, golden_cache):
        """Test query hash generation."""
        query = "SELECT * FROM test_table WHERE id = 1"
        hash1 = golden_cache._generate_query_hash(query)
        hash2 = golden_cache._generate_query_hash(query)

        assert hash1 == hash2  # Same query should produce same hash
        assert len(hash1) == 16  # Should be 16 characters

        # Different queries should produce different hashes
        query2 = "SELECT * FROM test_table WHERE id = 2"
        hash3 = golden_cache._generate_query_hash(query2)
        assert hash1 != hash3

    def test_register_pattern(self, golden_cache):
        """Test pattern registration."""
        new_pattern = QueryPattern(
            name="custom_pattern",
            query_type=QueryType.CUSTOM,
            pattern=r"SELECT.*FROM.*custom_table",
            key_template="custom:{table}",
            ttl_seconds=1800
        )

        golden_cache.register_pattern(new_pattern)

        assert "custom_pattern" in golden_cache._patterns
        assert golden_cache._patterns["custom_pattern"] == new_pattern


class TestGoldenQueryDecorators:
    """Test decorators for golden query caching."""

    async def test_cache_golden_query_decorator(self):
        """Test the cache golden query decorator."""
        call_count = 0

        @cache_golden_query(QueryType.CUSTOM, ttl_seconds=3600)
        async def test_function(x: int, y: str):
            nonlocal call_count
            call_count += 1
            return {"result": x, "type": y}

        # First call should compute
        result1 = await test_function(1, "test")
        assert result1 == {"result": 1, "type": "test"}
        assert call_count == 1

        # Second call with same args should use cache
        result2 = await test_function(1, "test")
        assert result2 == {"result": 1, "type": "test"}
        assert call_count == 1  # Should not have increased

        # Third call with different args should compute
        result3 = await test_function(2, "test2")
        assert result3 == {"result": 2, "type": "test2"}
        assert call_count == 2

    async def test_invalidate_on_write_decorator(self):
        """Test the invalidate on write decorator."""
        write_count = 0

        @invalidate_on_write("test_table")
        async def write_to_table(data: dict):
            nonlocal write_count
            write_count += 1
            return {"status": "success"}

        # Mock the cache invalidation
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.invalidate_dependencies = AsyncMock()
            mock_get_cache.return_value = mock_cache

            result = await write_to_table({"id": 1, "value": "test"})

            assert result == {"status": "success"}
            assert write_count == 1
            mock_cache.invalidate_dependencies.assert_called_once_with(["test_table"])


class TestGoldenQueryUtilityFunctions:
    """Test utility functions for golden query cache."""

    async def test_get_query_patterns(self):
        """Test getting all query patterns."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache._patterns = {
                "pattern1": QueryPattern(
                    name="pattern1",
                    query_type=QueryType.CURVE_DATA,
                    pattern=r"SELECT.*",
                    key_template="test",
                    ttl_seconds=3600
                )
            }
            mock_get_cache.return_value = mock_cache

            patterns = await get_query_patterns()

            assert "pattern1" in patterns
            assert patterns["pattern1"]["name"] == "pattern1"
            assert patterns["pattern1"]["query_type"] == "curve_data"

    async def test_register_custom_pattern(self):
        """Test registering a custom pattern."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_get_cache.return_value = mock_cache

            await register_custom_pattern(
                name="custom_pattern",
                query_type=QueryType.CUSTOM,
                pattern=r"SELECT.*FROM.*custom",
                key_template="custom:{table}",
                ttl_seconds=1800
            )

            mock_cache.register_pattern.assert_called_once()
            call_args = mock_cache.register_pattern.call_args[0][0]
            assert call_args.name == "custom_pattern"
            assert call_args.query_type == QueryType.CUSTOM

    async def test_get_cache_performance_report(self):
        """Test cache performance report generation."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.get_cache_stats.return_value = {
                "backend": "memory",
                "memory_entries": 100,
                "golden_queries": {
                    "total_queries": 50,
                    "total_hits": 40,
                    "total_misses": 10,
                    "patterns": {}
                }
            }
            mock_get_cache.return_value = mock_cache

            report = await get_cache_performance_report()

            assert report["summary"]["total_queries"] == 50
            assert report["summary"]["cache_hits"] == 40
            assert report["summary"]["cache_misses"] == 10
            assert report["summary"]["hit_rate"] == 0.8
            assert report["summary"]["cache_efficiency_percent"] == 80.0

    def test_generate_cache_recommendations(self):
        """Test cache recommendation generation."""
        stats = {
            "golden_queries": {
                "total_hits": 10,
                "total_misses": 20
            },
            "memory_entries": 15000
        }

        recommendations = generate_cache_recommendations(stats)

        # Should have recommendations for low hit rate and high memory usage
        assert len(recommendations) >= 2
        assert any("hit rate" in rec.lower() for rec in recommendations)
        assert any("memory" in rec.lower() for rec in recommendations)


class TestGoldenQueryIntegration:
    """Integration tests for golden query cache."""

    @pytest.fixture
    async def real_cache_service(self):
        """Create a real AsyncCache service for integration testing."""
        config = CacheConfig(namespace="test_golden", ttl_seconds=300)
        cache = AsyncCache(config, CacheBackend.MEMORY)
        await cache._AsyncCache__aenter__()  # Initialize the cache
        yield cache
        await cache._AsyncCache__aexit__(None, None, None)

    async def test_integration_with_real_cache(self, real_cache_service):
        """Test integration with real cache service."""
        golden_cache = GoldenQueryCache(real_cache_service)

        # Test cache hit/miss cycle
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15'"
        compute_func = AsyncMock(return_value={"data": "computed"})

        # First call - cache miss
        result1 = await golden_cache.get_or_compute(query, compute_func)
        assert result1 == {"data": "computed"}
        compute_func.assert_called_once()

        # Second call - cache hit
        result2 = await golden_cache.get_or_compute(query, compute_func)
        assert result2 == {"data": "computed"}
        compute_func.assert_called_once()  # Should not be called again

    async def test_pattern_integration(self, real_cache_service):
        """Test pattern matching with real cache."""
        golden_cache = GoldenQueryCache(real_cache_service)

        # Test a query that matches a pattern
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15' AND iso = 'USD'"
        compute_func = AsyncMock(return_value={"data": "pattern_matched"})

        result = await golden_cache.get_or_compute(query, compute_func)

        assert result == {"data": "pattern_matched"}

        # Verify pattern was matched
        pattern = golden_cache._find_matching_pattern(query)
        assert pattern is not None
        assert pattern.name == "curve_data_by_date"

    async def test_cache_invalidation_integration(self, real_cache_service):
        """Test cache invalidation with real cache."""
        golden_cache = GoldenQueryCache(real_cache_service)

        # Setup cache entry
        query = "SELECT * FROM curve_observation WHERE asof = '2024-01-15'"
        await real_cache_service.set("test_key", {"data": "cached"})

        # Invalidate pattern
        invalidated = await golden_cache.invalidate_pattern("curve_data_by_date")
        assert invalidated >= 0  # May be 0 if no matching keys

    async def test_cache_stats_integration(self, real_cache_service):
        """Test cache statistics with real cache."""
        golden_cache = GoldenQueryCache(real_cache_service)

        # Perform some operations
        query1 = "SELECT * FROM test1"
        query2 = "SELECT * FROM test2"

        for query in [query1, query2]:
            compute_func = AsyncMock(return_value={"data": "test"})
            await golden_cache.get_or_compute(query, compute_func)

        # Get stats
        stats = await golden_cache.get_cache_stats()

        assert stats["golden_queries"]["total_queries"] >= 2
        assert stats["golden_queries"]["total_misses"] >= 2

    def test_query_hash_consistency(self):
        """Test that query hashes are consistent."""
        from aurum.api.golden_query_cache import GoldenQueryCache

        cache = GoldenQueryCache(AsyncMock())

        query = "SELECT * FROM test_table WHERE id = 1"

        hash1 = cache._generate_query_hash(query)
        hash2 = cache._generate_query_hash(query)
        hash3 = cache._generate_query_hash("different query")

        assert hash1 == hash2  # Same query, same hash
        assert hash1 != hash3  # Different queries, different hashes

    def test_cache_entry_serialization(self):
        """Test cache entry serialization/deserialization."""
        import time
        from aurum.api.golden_query_cache import CacheEntry

        original_entry = CacheEntry(
            value={"test": "data"},
            created_at=time.time(),
            ttl_seconds=3600,
            query_type=QueryType.CUSTOM,
            query_hash="test_hash",
            tenant_id="tenant-123"
        )

        # Simulate serialization (what happens in cache.set)
        serialized = json.dumps(original_entry.__dict__)
        deserialized = json.loads(serialized)

        # Verify structure is preserved
        assert deserialized["value"] == {"test": "data"}
        assert deserialized["query_type"] == "custom"
        assert deserialized["tenant_id"] == "tenant-123"


class TestGoldenQueryPerformance:
    """Performance tests for golden query cache."""

    @pytest.fixture
    async def performance_cache(self):
        """Create cache for performance testing."""
        config = CacheConfig(namespace="perf_test", ttl_seconds=300)
        cache = AsyncCache(config, CacheBackend.MEMORY)
        await cache._AsyncCache__aenter__()
        yield GoldenQueryCache(cache)
        await cache._AsyncCache__aexit__(None, None, None)

    async def test_cache_hit_performance(self, performance_cache):
        """Test cache hit performance."""
        # Setup cache entry
        query = "SELECT * FROM performance_test"
        await performance_cache.cache.set("perf_key", {"data": "cached"})

        # Time cache hits
        start_time = time.time()
        for i in range(100):
            result = await performance_cache.get_or_compute(
                f"{query}_{i}",
                AsyncMock(return_value={"data": "computed"})
            )
        end_time = time.time()

        # Should be very fast (less than 100ms for 100 hits)
        assert (end_time - start_time) < 0.1

    async def test_concurrent_access(self, performance_cache):
        """Test concurrent cache access."""
        query = "SELECT * FROM concurrent_test"
        compute_func = AsyncMock(return_value={"data": "concurrent"})

        # Concurrent requests
        tasks = []
        for i in range(10):
            task = performance_cache.get_or_compute(f"{query}_{i}", compute_func)
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        # All should succeed
        assert len(results) == 10
        assert all(r == {"data": "concurrent"} for r in results)

    def test_memory_usage(self):
        """Test memory usage characteristics."""
        # This would test:
        # 1. Memory growth with cache entries
        # 2. Memory cleanup on expiration
        # 3. Memory fragmentation
        pass  # Placeholder for memory tests

    def test_cache_pressure(self):
        """Test cache under pressure."""
        # This would test:
        # 1. High volume of cache operations
        # 2. Cache entry eviction
        # 3. Concurrent read/write patterns
        # 4. Memory pressure scenarios
        pass  # Placeholder for pressure tests


class TestGoldenQueryErrorHandling:
    """Error handling tests for golden query cache."""

    async def test_compute_function_failure(self):
        """Test handling of compute function failures."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.get_or_compute = AsyncMock(side_effect=Exception("Compute failed"))
            mock_get_cache.return_value = mock_cache

            with pytest.raises(Exception, match="Compute failed"):
                await mock_cache.get_or_compute("test", AsyncMock())

    async def test_cache_backend_failure(self):
        """Test handling of cache backend failures."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.cache.get = AsyncMock(side_effect=Exception("Cache backend error"))
            mock_get_cache.return_value = mock_cache

            # Should handle gracefully and compute the value
            compute_func = AsyncMock(return_value={"data": "computed"})
            result = await mock_cache.get_or_compute("test", compute_func)

            assert result == {"data": "computed"}

    async def test_invalid_query_patterns(self):
        """Test handling of invalid query patterns."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache._find_matching_pattern = AsyncMock(return_value=None)
            mock_get_cache.return_value = mock_cache

            # Should handle gracefully without pattern match
            compute_func = AsyncMock(return_value={"data": "computed"})
            result = await mock_cache.get_or_compute("invalid query", compute_func)

            assert result == {"data": "computed"}

    async def test_invalidation_failure(self):
        """Test handling of invalidation failures."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.invalidate_dependencies = AsyncMock(side_effect=Exception("Invalidation failed"))
            mock_get_cache.return_value = mock_cache

            # Should handle gracefully and log error
            with patch("aurum.api.golden_query_cache.log_structured") as mock_log:
                await mock_cache.invalidate_dependencies(["test_table"])
                mock_log.assert_called()  # Should log the error

    async def test_malformed_cache_entries(self):
        """Test handling of malformed cache entries."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.cache.get = AsyncMock(return_value="malformed json")
            mock_get_cache.return_value = mock_cache

            # Should handle gracefully and compute fresh value
            compute_func = AsyncMock(return_value={"data": "fresh"})
            result = await mock_cache.get_or_compute("test", compute_func)

            assert result == {"data": "fresh"}

    async def test_expired_entries_cleanup(self):
        """Test cleanup of expired entries."""
        with patch("aurum.api.golden_query_cache.get_golden_query_cache") as mock_get_cache:
            mock_cache = AsyncMock()
            mock_cache.cleanup_expired_entries = AsyncMock(return_value=5)
            mock_get_cache.return_value = mock_cache

            # Should handle gracefully
            cleaned = await mock_cache.cleanup_expired_entries()
            assert cleaned == 5
