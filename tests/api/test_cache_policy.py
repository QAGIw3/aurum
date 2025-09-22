"""Test the production cache policy implementation."""

from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import Mock, patch, AsyncMock
from typing import Any, Dict, List
from datetime import date

from aurum.api.golden_query_cache import (
    GoldenQueryCache,
    QueryType,
    CacheInvalidationStrategy,
    QueryPattern,
    invalidate_cache_on_data_publish
)
from aurum.api.cache import AsyncCache
from aurum.core import AurumSettings
from aurum.core.settings import DataBackendType


@pytest_asyncio.asyncio
class TestProductionCachePolicy:
    """Test the production cache policy enhancements."""

    @pytest.fixture
    async def mock_cache_service(self):
        """Mock cache service for testing."""
        cache = Mock(spec=AsyncCache)
        cache.get = AsyncMock(return_value=None)
        cache.set = AsyncMock()
        cache.delete = AsyncMock()
        cache.get_stats = AsyncMock(return_value={
            "backend": "memory",
            "memory_entries": 0,
            "memory_size_bytes": 0
        })
        return cache

    @pytest.fixture
    async def golden_cache(self, mock_cache_service):
        """Golden query cache instance for testing."""
        return GoldenQueryCache(mock_cache_service)

    async def test_query_types_include_diff_and_strips(self):
        """Test that new query types for diff and strips are defined."""
        assert QueryType.CURVE_DIFF == "curve_diff"
        assert QueryType.CURVE_STRIPS == "curve_strips"

    async def test_cache_patterns_include_diff_and_strips(self, golden_cache):
        """Test that cache patterns include diff and strips patterns."""
        patterns = golden_cache._patterns

        # Should have patterns for diff and strips
        diff_pattern = patterns.get("curve_diff_by_dates")
        strips_pattern = patterns.get("curve_strips_by_type")

        assert diff_pattern is not None
        assert diff_pattern.query_type == QueryType.CURVE_DIFF
        assert "asof_a" in diff_pattern.pattern
        assert "asof_b" in diff_pattern.pattern

        assert strips_pattern is not None
        assert strips_pattern.query_type == QueryType.CURVE_STRIPS
        assert "asof" in strips_pattern.pattern
        assert "tenor_type" in strips_pattern.pattern

    async def test_dynamic_ttl_settings(self, golden_cache):
        """Test that cache patterns use dynamic TTL based on settings."""
        settings = golden_cache._settings.api.cache

        # Check that patterns use settings-based TTL
        curve_pattern = golden_cache._patterns.get("curve_data_by_date")
        assert curve_pattern.ttl_seconds == settings.cache_ttl_curve_data

        metadata_pattern = golden_cache._patterns.get("metadata_dimensions")
        assert metadata_pattern.ttl_seconds == settings.cache_ttl_metadata

        scenario_pattern = golden_cache._patterns.get("scenario_outputs_by_scenario")
        assert scenario_pattern.ttl_seconds == settings.cache_ttl_scenario_data

    async def test_asof_publish_invalidation(self, golden_cache):
        """Test cache invalidation on as-of data publish."""
        # Mock some cache entries
        golden_cache._query_stats = {
            "tenant123:curve_data:2024-01-01:PJM": {"hits": 5, "misses": 1},
            "tenant123:curve_diff:2024-01-01:2024-01-02:PJM": {"hits": 3, "misses": 2},
            "tenant123:metadata_dimensions:area:locations": {"hits": 10, "misses": 0}
        }

        # Mock cache delete
        golden_cache.cache.delete = AsyncMock()

        # Invalidate on publish
        invalidated_count = await golden_cache.invalidate_on_asof_publish(
            "curve_observation", "2024-01-15", "tenant123"
        )

        # Should have deleted curve-related cache entries
        assert golden_cache.cache.delete.call_count >= 2
        assert invalidated_count >= 2

    async def test_cache_stats_enhanced_metrics(self, golden_cache):
        """Test that cache stats include enhanced metrics."""
        # Mock some query stats
        golden_cache._query_stats = {
            "tenant123:curve_data:2024-01-01:PJM": {"hits": 10, "misses": 2},
            "tenant123:curve_diff:2024-01-01:2024-01-02:PJM": {"hits": 5, "misses": 5},
            "tenant123:metadata_dimensions:area:locations": {"hits": 20, "misses": 0}
        }

        stats = await golden_cache.get_cache_stats()

        # Check enhanced metrics
        assert "cache_performance" in stats
        assert "cache_efficiency_percent" in stats["golden_queries"]
        assert "average_pattern_hit_rate" in stats["golden_queries"]
        assert "cache_savings_ratio" in stats["cache_performance"]

        # Calculate expected values
        total_hits = 35  # 10 + 5 + 20
        total_misses = 7  # 2 + 5 + 0
        expected_hit_rate = total_hits / (total_hits + total_misses)

        assert stats["golden_queries"]["total_hits"] == total_hits
        assert stats["golden_queries"]["total_misses"] == total_misses
        assert stats["golden_queries"]["hit_rate"] == expected_hit_rate

    async def test_per_dimension_ttl_settings(self):
        """Test that per-dimension TTL settings are available."""
        settings = AurumSettings.from_env()

        cache_settings = settings.api.cache

        # Check that new TTL settings are available
        assert hasattr(cache_settings, 'cache_ttl_high_frequency')
        assert hasattr(cache_settings, 'cache_ttl_medium_frequency')
        assert hasattr(cache_settings, 'cache_ttl_low_frequency')
        assert hasattr(cache_settings, 'cache_ttl_static')
        assert hasattr(cache_settings, 'cache_ttl_curve_data')
        assert hasattr(cache_settings, 'cache_ttl_metadata')
        assert hasattr(cache_settings, 'cache_ttl_external_data')
        assert hasattr(cache_settings, 'cache_ttl_scenario_data')

        # Check default values
        assert cache_settings.cache_ttl_high_frequency == 60
        assert cache_settings.cache_ttl_medium_frequency == 300
        assert cache_settings.cache_ttl_low_frequency == 3600
        assert cache_settings.cache_ttl_static == 7200

    async def test_invalidate_cache_utility_function(self):
        """Test the utility function for cache invalidation."""
        with patch('aurum.api.golden_query_cache.get_golden_query_cache') as mock_get_cache:
            mock_cache = Mock()
            mock_cache.invalidate_on_asof_publish = AsyncMock(return_value=5)
            mock_get_cache.return_value = mock_cache

            invalidated = await invalidate_cache_on_data_publish(
                "curve_observation", "2024-01-15", "tenant123"
            )

            assert invalidated == 5
            mock_cache.invalidate_on_asof_publish.assert_called_once_with(
                "curve_observation", "2024-01-15", "tenant123"
            )

    async def test_cache_pattern_dependencies(self, golden_cache):
        """Test that cache patterns have correct dependencies."""
        patterns = golden_cache._patterns

        # Check that patterns have correct dependencies
        curve_pattern = patterns.get("curve_data_by_date")
        assert "curve_observation" in curve_pattern.dependencies

        diff_pattern = patterns.get("curve_diff_by_dates")
        assert "curve_observation" in diff_pattern.dependencies

        strips_pattern = patterns.get("curve_strips_by_type")
        assert "curve_observation" in strips_pattern.dependencies

        metadata_pattern = patterns.get("metadata_dimensions")
        assert "metadata_dimensions" in metadata_pattern.dependencies

    async def test_cache_invalidation_strategies(self, golden_cache):
        """Test that patterns use appropriate invalidation strategies."""
        patterns = golden_cache._patterns

        # Most patterns should use BUST_ON_WRITE for data consistency
        for pattern_name, pattern in patterns.items():
            if pattern_name in ["curve_data_by_date", "curve_diff_by_dates", "curve_strips_by_type"]:
                assert pattern.invalidation_strategy == CacheInvalidationStrategy.BUST_ON_WRITE
            elif pattern_name == "metadata_dimensions":
                assert pattern.invalidation_strategy == CacheInvalidationStrategy.BUST_ON_WRITE
