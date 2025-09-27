"""Tests for the cache governance system."""

import pytest
from unittest.mock import AsyncMock, Mock

from aurum.api.cache.cache_governance import (
    CacheGovernanceManager,
    TTLPolicy,
    TTLConfiguration,
    KeyNamingPattern,
)
from aurum.api.cache.enhanced_cache_manager import CacheNamespace


@pytest.fixture
def mock_cache_manager():
    """Mock enhanced cache manager."""
    manager = Mock()
    manager.get = AsyncMock(return_value=None)
    manager.set = AsyncMock()
    manager.delete = AsyncMock(return_value=True)
    return manager


@pytest.fixture
def governance_manager(mock_cache_manager):
    """Create cache governance manager."""
    return CacheGovernanceManager(mock_cache_manager)


class TestTTLConfiguration:
    """Test TTL configuration."""
    
    def test_default_policies(self):
        """Test default TTL policies are configured correctly."""
        policies = TTLConfiguration.get_default_policies()
        
        assert TTLPolicy.ULTRA_SHORT in policies
        assert policies[TTLPolicy.ULTRA_SHORT].seconds == 30
        assert TTLPolicy.SHORT in policies
        assert policies[TTLPolicy.SHORT].seconds == 300
        assert TTLPolicy.PERSISTENT in policies
        assert policies[TTLPolicy.PERSISTENT].seconds == 604800


class TestKeyNamingPattern:
    """Test key naming pattern validation."""
    
    def test_curve_pattern_validation(self):
        """Test curve key pattern validation."""
        # Valid curve keys
        assert KeyNamingPattern.validate_key("curves:nyiso:energy:zone_a", CacheNamespace.CURVES)
        assert KeyNamingPattern.validate_key("curves:pjm:capacity:rto:2024-01-15", CacheNamespace.CURVES)
        
        # Invalid curve keys
        assert not KeyNamingPattern.validate_key("CURVES:NYISO:ENERGY:ZONE_A", CacheNamespace.CURVES)
        assert not KeyNamingPattern.validate_key("curves_nyiso_energy", CacheNamespace.CURVES)
    
    def test_metadata_pattern_validation(self):
        """Test metadata key pattern validation."""
        # Valid metadata keys
        assert KeyNamingPattern.validate_key("metadata:iso_zones", CacheNamespace.METADATA)
        assert KeyNamingPattern.validate_key("metadata:market_types:nyiso", CacheNamespace.METADATA)
        
        # Invalid metadata keys
        assert not KeyNamingPattern.validate_key("metadata:INVALID-KEY", CacheNamespace.METADATA)


class TestCacheGovernanceManager:
    """Test cache governance manager."""
    
    def test_initialization(self, governance_manager):
        """Test governance manager initialization."""
        assert governance_manager.cache_manager is not None
        assert len(governance_manager.governance_policies) == 7  # All namespaces
        assert len(governance_manager.ttl_policies) == 6  # All TTL policies
    
    def test_get_ttl_for_namespace(self, governance_manager):
        """Test TTL retrieval for namespaces."""
        # Curves should use SHORT policy (300 seconds)
        ttl = governance_manager.get_ttl_for_namespace(CacheNamespace.CURVES)
        assert ttl == 300
        
        # Metadata should use MEDIUM policy (1800 seconds)
        ttl = governance_manager.get_ttl_for_namespace(CacheNamespace.METADATA)
        assert ttl == 1800
    
    def test_validate_cache_key_valid(self, governance_manager):
        """Test cache key validation for valid keys."""
        # Valid curve key
        assert governance_manager.validate_cache_key("curves:nyiso:energy:zone_a", CacheNamespace.CURVES)
        
        # Valid metadata key
        assert governance_manager.validate_cache_key("metadata:iso_zones", CacheNamespace.METADATA)
    
    @pytest.mark.asyncio
    async def test_get_governance_stats(self, governance_manager):
        """Test governance statistics retrieval."""
        stats = await governance_manager.get_governance_stats()
        
        assert "total_operations" in stats
        assert "policy_violations" in stats
        assert "ttl_overrides" in stats
        assert "hit_rates_by_namespace" in stats
        assert "governance_policies" in stats
        
        # Check that all namespaces are included
        policies = stats["governance_policies"]
        assert "curves" in policies
        assert "metadata" in policies
        assert policies["curves"]["ttl_policy"] == "short"
        assert policies["metadata"]["ttl_policy"] == "medium"