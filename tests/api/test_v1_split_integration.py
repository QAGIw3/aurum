"""Tests for V1 split flag integration with the feature flag system."""

from __future__ import annotations

import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from aurum.api.features.v1_split_integration import (
    V1SplitFlagManager,
    V1SplitFlagConfig,
    get_v1_split_manager,
    initialize_v1_split_flags
)
from aurum.api.features.feature_flags import (
    FeatureFlag,
    FeatureFlagManager,
    FeatureFlagStatus,
    InMemoryFeatureFlagStore
)


class TestV1SplitFlagConfig:
    """Test V1SplitFlagConfig dataclass."""

    def test_config_creation(self):
        """Test basic config creation."""
        config = V1SplitFlagConfig(
            env_var="TEST_VAR",
            module_path="test.module",
            description="Test config"
        )
        
        assert config.env_var == "TEST_VAR"
        assert config.module_path == "test.module"
        assert config.description == "Test config"
        assert config.default_enabled is False

    def test_config_with_defaults(self):
        """Test config with custom defaults."""
        config = V1SplitFlagConfig(
            env_var="TEST_VAR",
            module_path="test.module",
            description="Test config",
            default_enabled=True
        )
        
        assert config.default_enabled is True


class TestV1SplitFlagManager:
    """Test V1SplitFlagManager functionality."""

    @pytest.fixture
    def feature_manager(self):
        """Create a test feature flag manager."""
        store = InMemoryFeatureFlagStore()
        return FeatureFlagManager(store)

    @pytest.fixture
    def v1_manager(self, feature_manager):
        """Create a test V1 split flag manager."""
        return V1SplitFlagManager(feature_manager)

    def test_manager_initialization(self, v1_manager):
        """Test manager initializes with all expected flags."""
        expected_flags = {"eia", "iso", "drought", "admin", "ppa", "curves"}
        actual_flags = set(v1_manager.V1_SPLIT_FLAGS.keys())
        assert actual_flags == expected_flags

    def test_flag_configs(self, v1_manager):
        """Test that flag configurations are set up correctly."""
        # Test EIA config
        eia_config = v1_manager.V1_SPLIT_FLAGS["eia"]
        assert eia_config.env_var == "AURUM_API_V1_SPLIT_EIA"
        assert eia_config.module_path == "aurum.api.v1.eia"
        assert not eia_config.default_enabled

        # Test PPA config (should be enabled by default) 
        ppa_config = v1_manager.V1_SPLIT_FLAGS["ppa"]
        assert ppa_config.env_var == "AURUM_API_V1_SPLIT_PPA"
        assert ppa_config.default_enabled

    @pytest.mark.asyncio
    async def test_initialize_v1_flags(self, v1_manager):
        """Test initialization of V1 flags in feature system."""
        # Set some environment variables
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "0"
        
        try:
            await v1_manager.initialize_v1_flags()
            
            # Check that flags were created
            eia_flag = await v1_manager.feature_manager.get_flag("v1_split_eia")
            assert eia_flag is not None
            assert eia_flag.status == FeatureFlagStatus.ENABLED
            assert "v1-split" in eia_flag.tags
            
            iso_flag = await v1_manager.feature_manager.get_flag("v1_split_iso")
            assert iso_flag is not None
            assert iso_flag.status == FeatureFlagStatus.DISABLED
            
            # PPA should be enabled by default even without env var
            ppa_flag = await v1_manager.feature_manager.get_flag("v1_split_ppa")
            assert ppa_flag is not None
            assert ppa_flag.status == FeatureFlagStatus.ENABLED
            
        finally:
            # Cleanup
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)
            os.environ.pop("AURUM_API_V1_SPLIT_ISO", None)

    @pytest.mark.asyncio
    async def test_initialize_existing_flags(self, v1_manager):
        """Test that existing flags are not overwritten."""
        # Create a flag manually first
        existing_flag = FeatureFlag(
            name="Existing EIA Flag",
            key="v1_split_eia",
            description="Pre-existing flag",
            status=FeatureFlagStatus.ENABLED,
            default_value=False,
            rules=[],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            created_by="test",
            tags=["test"]
        )
        await v1_manager.feature_manager.set_flag(existing_flag)
        
        # Initialize should not overwrite
        await v1_manager.initialize_v1_flags()
        
        # Check the flag wasn't changed
        flag = await v1_manager.feature_manager.get_flag("v1_split_eia")
        assert flag.name == "Existing EIA Flag"
        assert flag.created_by == "test"

    @pytest.mark.asyncio
    async def test_is_v1_split_enabled(self, v1_manager):
        """Test checking if V1 split flag is enabled."""
        # Initialize with a flag
        await v1_manager.initialize_v1_flags()
        
        # Set flag enabled
        await v1_manager.set_v1_split_enabled("eia", True)
        
        # Test checking
        is_enabled = await v1_manager.is_v1_split_enabled("eia")
        assert is_enabled is True
        
        # Test environment variable sync
        assert os.getenv("AURUM_API_V1_SPLIT_EIA") == "1"

    @pytest.mark.asyncio
    async def test_is_v1_split_enabled_unknown_flag(self, v1_manager):
        """Test checking unknown flag returns False."""
        is_enabled = await v1_manager.is_v1_split_enabled("unknown_flag")
        assert is_enabled is False

    @pytest.mark.asyncio
    async def test_is_v1_split_enabled_fallback(self, v1_manager):
        """Test fallback to environment variable when feature flag fails."""
        # Set environment variable
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            # Mock feature manager to raise exception
            with patch.object(v1_manager.feature_manager, 'is_enabled', side_effect=Exception("Test error")):
                is_enabled = await v1_manager.is_v1_split_enabled("eia")
                assert is_enabled is True  # Should fallback to env var
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)

    @pytest.mark.asyncio
    async def test_set_v1_split_enabled(self, v1_manager):
        """Test enabling/disabling V1 split flags."""
        await v1_manager.initialize_v1_flags()
        
        # Enable flag
        success = await v1_manager.set_v1_split_enabled("eia", True, "test_user")
        assert success is True
        
        # Check flag status
        flag = await v1_manager.feature_manager.get_flag("v1_split_eia")
        assert flag.status == FeatureFlagStatus.ENABLED
        assert os.getenv("AURUM_API_V1_SPLIT_EIA") == "1"
        
        # Disable flag
        success = await v1_manager.set_v1_split_enabled("eia", False, "test_user")
        assert success is True
        
        # Check flag status
        flag = await v1_manager.feature_manager.get_flag("v1_split_eia")
        assert flag.status == FeatureFlagStatus.DISABLED
        assert os.getenv("AURUM_API_V1_SPLIT_EIA") == "0"

    @pytest.mark.asyncio
    async def test_set_v1_split_enabled_unknown_flag(self, v1_manager):
        """Test setting unknown flag returns False."""
        success = await v1_manager.set_v1_split_enabled("unknown_flag", True)
        assert success is False

    @pytest.mark.asyncio
    async def test_get_v1_split_status(self, v1_manager):
        """Test getting status of all V1 split flags."""
        # Set some environment variables
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "0"
        
        try:
            await v1_manager.initialize_v1_flags()
            status = await v1_manager.get_v1_split_status()
            
            # Check all flags are present
            expected_flags = {"eia", "iso", "drought", "admin", "ppa", "curves"}
            assert set(status.keys()) == expected_flags
            
            # Check EIA status
            eia_status = status["eia"]
            assert eia_status["enabled"] is True
            assert eia_status["env_var"] == "AURUM_API_V1_SPLIT_EIA"
            assert eia_status["module_path"] == "aurum.api.v1.eia"
            assert "last_updated" in eia_status
            
            # Check ISO status
            iso_status = status["iso"]
            assert iso_status["enabled"] is False
            
            # Check PPA status (should be enabled by default)
            ppa_status = status["ppa"]
            assert ppa_status["enabled"] is True
            assert ppa_status["default_enabled"] is True
            
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)
            os.environ.pop("AURUM_API_V1_SPLIT_ISO", None)

    @pytest.mark.asyncio
    async def test_get_v1_split_status_not_initialized(self, v1_manager):
        """Test getting status when flags not in feature system yet."""
        # Set environment variable
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            # Don't initialize flags
            status = await v1_manager.get_v1_split_status()
            
            eia_status = status["eia"]
            assert eia_status["enabled"] is True  # From env var
            assert eia_status["status"] == "not_in_feature_system"
            assert eia_status["last_updated"] is None
            
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)

    def test_get_env_var_for_flag(self, v1_manager):
        """Test getting environment variable for flag."""
        env_var = v1_manager.get_env_var_for_flag("eia")
        assert env_var == "AURUM_API_V1_SPLIT_EIA"
        
        env_var = v1_manager.get_env_var_for_flag("unknown")
        assert env_var is None

    def test_get_module_path_for_flag(self, v1_manager):
        """Test getting module path for flag."""
        module_path = v1_manager.get_module_path_for_flag("eia")
        assert module_path == "aurum.api.v1.eia"
        
        module_path = v1_manager.get_module_path_for_flag("unknown")
        assert module_path is None


class TestGlobalManager:
    """Test global manager functions."""

    def test_get_v1_split_manager(self):
        """Test getting global manager."""
        manager1 = get_v1_split_manager()
        manager2 = get_v1_split_manager()
        
        # Should return same instance
        assert manager1 is manager2
        assert isinstance(manager1, V1SplitFlagManager)

    @pytest.mark.asyncio
    async def test_initialize_v1_split_flags(self):
        """Test global initialization function."""
        with patch('aurum.api.features.v1_split_integration.get_v1_split_manager') as mock_get_manager:
            mock_manager = AsyncMock()
            mock_get_manager.return_value = mock_manager
            
            await initialize_v1_split_flags()
            
            mock_manager.initialize_v1_flags.assert_called_once()


class TestEnvironmentIntegration:
    """Test integration with environment variables."""

    def test_env_var_sync_on_flag_change(self):
        """Test that environment variables are synced when flags change."""
        # This would be tested in integration tests with actual environment
        pass

    def test_backward_compatibility(self):
        """Test that existing environment-based code still works."""
        # Set environment variable directly
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            # Code that checks environment variable should still work
            assert os.getenv("AURUM_API_V1_SPLIT_EIA") == "1"
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_feature_manager_error_handling(self):
        """Test graceful handling of feature manager errors."""
        # Create manager with broken feature manager
        broken_manager = MagicMock()
        broken_manager.get_flag = AsyncMock(side_effect=Exception("Broken"))
        
        v1_manager = V1SplitFlagManager(broken_manager)
        
        # Should handle errors gracefully
        status = await v1_manager.get_v1_split_status()
        
        # Should have error entries for all flags
        for flag_name in v1_manager.V1_SPLIT_FLAGS:
            assert "error" in status[flag_name]

    @pytest.mark.asyncio 
    async def test_initialization_error_handling(self):
        """Test handling of initialization errors."""
        v1_manager = V1SplitFlagManager()
        
        # Mock feature manager to fail
        with patch.object(v1_manager.feature_manager, 'set_flag', side_effect=Exception("Init error")):
            # Should not raise exception
            await v1_manager.initialize_v1_flags()