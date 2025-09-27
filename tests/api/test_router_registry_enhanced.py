"""Tests for enhanced router registry with V1 split flag integration."""

from __future__ import annotations

import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from aurum.api.router_registry import (
    get_v1_router_specs,
    get_v1_router_specs_async,
    _check_v1_flag_sync,
    RouterSpec
)
from aurum.core import AurumSettings


class TestEnhancedRouterRegistry:
    """Test enhanced router registry functionality."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock settings."""
        return MagicMock(spec=AurumSettings)

    @pytest.fixture
    def mock_v1_manager(self):
        """Create mock V1 split flag manager."""
        manager = MagicMock()
        manager.V1_SPLIT_FLAGS = {
            "eia": MagicMock(env_var="AURUM_API_V1_SPLIT_EIA", default_enabled=False),
            "iso": MagicMock(env_var="AURUM_API_V1_SPLIT_ISO", default_enabled=False),
            "ppa": MagicMock(env_var="AURUM_API_V1_SPLIT_PPA", default_enabled=True),
        }
        manager.initialize_v1_flags = AsyncMock()
        manager.is_v1_split_enabled = AsyncMock()
        return manager

    def test_get_v1_router_specs_backward_compatibility(self, mock_settings):
        """Test that original router specs function still works with environment variables."""
        # Set some environment variables
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "0"
        
        try:
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = get_v1_router_specs(mock_settings)
                
                # Should have loaded EIA router but not ISO
                module_names = [spec.name for spec in specs if spec.name]
                assert any("eia" in name for name in module_names)
                assert not any("iso" in name for name in module_names)
                
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)
            os.environ.pop("AURUM_API_V1_SPLIT_ISO", None)

    def test_check_v1_flag_sync_with_feature_manager(self):
        """Test sync flag checking with feature manager."""
        # Create mock feature manager with store
        mock_flag = MagicMock()
        mock_flag.status.value = "enabled"
        
        mock_store = MagicMock()
        mock_store._flags = {"v1_split_eia": mock_flag}
        
        mock_feature_manager = MagicMock()
        mock_feature_manager.store = mock_store
        
        mock_v1_manager = MagicMock()
        mock_v1_manager.feature_manager = mock_feature_manager
        
        result = _check_v1_flag_sync(mock_v1_manager, "eia", "AURUM_API_V1_SPLIT_EIA")
        assert result is True

    def test_check_v1_flag_sync_fallback_to_env(self):
        """Test sync flag checking falls back to environment variable."""
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            # Mock manager without proper feature manager
            mock_v1_manager = MagicMock()
            mock_v1_manager.feature_manager = None
            mock_v1_manager.V1_SPLIT_FLAGS = {
                "eia": MagicMock(env_var="AURUM_API_V1_SPLIT_EIA", default_enabled=False)
            }
            
            result = _check_v1_flag_sync(mock_v1_manager, "eia", "AURUM_API_V1_SPLIT_EIA")
            assert result is True
            
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)

    def test_check_v1_flag_sync_default_enabled(self):
        """Test sync flag checking with default enabled flag."""
        # No environment variable set, but flag has default_enabled=True
        mock_v1_manager = MagicMock()
        mock_v1_manager.feature_manager = None
        mock_v1_manager.V1_SPLIT_FLAGS = {
            "ppa": MagicMock(env_var="AURUM_API_V1_SPLIT_PPA", default_enabled=True)
        }
        
        result = _check_v1_flag_sync(mock_v1_manager, "ppa", "AURUM_API_V1_SPLIT_PPA")
        assert result is True

    def test_check_v1_flag_sync_error_handling(self):
        """Test sync flag checking handles errors gracefully."""
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            # Mock manager that raises exception
            mock_v1_manager = MagicMock()
            mock_v1_manager.feature_manager = MagicMock()
            mock_v1_manager.feature_manager.store = MagicMock(side_effect=Exception("Test error"))
            
            result = _check_v1_flag_sync(mock_v1_manager, "eia", "AURUM_API_V1_SPLIT_EIA")
            assert result is True  # Should fallback to env var
            
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)

    @pytest.mark.asyncio
    async def test_get_v1_router_specs_async(self, mock_settings, mock_v1_manager):
        """Test async router specs function."""
        # Mock the manager responses
        mock_v1_manager.is_v1_split_enabled.side_effect = lambda flag: {
            "eia": True,
            "iso": False, 
            "drought": False,
            "admin": True,
            "ppa": True,
        }.get(flag, False)
        
        with patch('aurum.api.router_registry.get_v1_split_manager', return_value=mock_v1_manager):
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = await get_v1_router_specs_async(mock_settings)
                
                # Should have initialized flags
                mock_v1_manager.initialize_v1_flags.assert_called_once()
                
                # Should have checked enabled flags
                mock_v1_manager.is_v1_split_enabled.assert_called()
                
                # Should have loaded enabled routers
                module_names = [spec.name for spec in specs if spec.name]
                assert any("eia" in name for name in module_names)
                assert any("admin" in name for name in module_names)
                assert any("ppa" in name for name in module_names)

    @pytest.mark.asyncio
    async def test_get_v1_router_specs_async_with_errors(self, mock_settings, mock_v1_manager):
        """Test async router specs function handles errors gracefully."""
        # Mock initialization to fail
        mock_v1_manager.initialize_v1_flags.side_effect = Exception("Init failed")
        
        # Mock flag checking to fail for some flags
        def mock_is_enabled(flag):
            if flag == "eia":
                raise Exception("Check failed")
            return flag == "ppa"  # Only PPA enabled
        
        mock_v1_manager.is_v1_split_enabled.side_effect = mock_is_enabled
        
        # Mock fallback config
        mock_v1_manager.V1_SPLIT_FLAGS = {
            "eia": MagicMock(env_var="AURUM_API_V1_SPLIT_EIA", default_enabled=False),
            "ppa": MagicMock(env_var="AURUM_API_V1_SPLIT_PPA", default_enabled=True),
        }
        
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"  # Should fallback to this
        
        try:
            with patch('aurum.api.router_registry.get_v1_split_manager', return_value=mock_v1_manager):
                with patch('aurum.api.router_registry._try_import_router') as mock_import:
                    mock_router = MagicMock()
                    mock_import.return_value = mock_router
                    
                    specs = await get_v1_router_specs_async(mock_settings)
                    
                    # Should have attempted initialization despite error
                    mock_v1_manager.initialize_v1_flags.assert_called_once()
                    
                    # Should have loaded routers based on fallback logic
                    module_names = [spec.name for spec in specs if spec.name]
                    assert any("eia" in name for name in module_names)  # From env fallback
                    assert any("ppa" in name for name in module_names)  # From successful check
                    
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)

    @pytest.mark.asyncio
    async def test_get_v1_router_specs_async_supplemental_modules(self, mock_settings, mock_v1_manager):
        """Test that supplemental modules are still loaded correctly."""
        mock_v1_manager.is_v1_split_enabled.return_value = False  # No V1 splits enabled
        
        with patch('aurum.api.router_registry.get_v1_split_manager', return_value=mock_v1_manager):
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = await get_v1_router_specs_async(mock_settings)
                
                # Should still have mandatory and supplemental modules
                module_names = [spec.name for spec in specs if spec.name]
                
                # Check for mandatory modules
                assert any("scenarios" in name for name in module_names)
                assert any("metadata" in name for name in module_names)
                
                # Check for supplemental modules
                assert any("external" in name for name in module_names)
                assert any("performance" in name for name in module_names)

    def test_enhanced_router_loading_logging(self, mock_settings, caplog):
        """Test that enhanced router loading includes proper logging."""
        with patch('aurum.api.router_registry.get_v1_split_manager') as mock_get_manager:
            mock_v1_manager = MagicMock()
            mock_v1_manager.V1_SPLIT_FLAGS = {
                "eia": MagicMock(env_var="AURUM_API_V1_SPLIT_EIA", default_enabled=False)
            }
            mock_get_manager.return_value = mock_v1_manager
            
            with patch('aurum.api.router_registry._check_v1_flag_sync', return_value=True):
                with patch('aurum.api.router_registry._try_import_router') as mock_import:
                    mock_router = MagicMock()
                    mock_import.return_value = mock_router
                    
                    specs = get_v1_router_specs(mock_settings)
                    
                    # Should have logged router loading
                    assert any("Loaded V1 split router" in record.message for record in caplog.records)

    def test_router_spec_structure(self, mock_settings):
        """Test that RouterSpec objects are created correctly."""
        with patch('aurum.api.router_registry.get_v1_split_manager') as mock_get_manager:
            mock_v1_manager = MagicMock()
            mock_get_manager.return_value = mock_v1_manager
            
            with patch('aurum.api.router_registry._check_v1_flag_sync', return_value=True):
                with patch('aurum.api.router_registry._try_import_router') as mock_import:
                    mock_router = MagicMock()
                    mock_import.return_value = mock_router
                    
                    specs = get_v1_router_specs(mock_settings)
                    
                    # Check that specs are RouterSpec instances
                    for spec in specs:
                        assert isinstance(spec, RouterSpec)
                        assert hasattr(spec, 'router')
                        assert hasattr(spec, 'name')
                        assert hasattr(spec, 'include_kwargs')


class TestIntegrationWithExistingCode:
    """Test integration with existing codebase."""

    def test_backward_compatibility_environment_vars(self, mock_settings):
        """Test that existing environment variable usage still works."""
        # This ensures existing deployment scripts and configs continue to work
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "0"
        
        try:
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = get_v1_router_specs(mock_settings)
                
                # EIA should be loaded, ISO should not
                module_names = [spec.name for spec in specs if spec.name]
                eia_loaded = any("eia" in name for name in module_names)
                iso_loaded = any("iso" in name for name in module_names)
                
                assert eia_loaded is True
                assert iso_loaded is False
                
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)
            os.environ.pop("AURUM_API_V1_SPLIT_ISO", None)

    def test_no_double_loading_of_modules(self, mock_settings):
        """Test that modules are not loaded twice when using both methods."""
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        
        try:
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = get_v1_router_specs(mock_settings)
                
                # Count how many times EIA module appears
                eia_specs = [spec for spec in specs if spec.name and "eia" in spec.name]
                assert len(eia_specs) <= 1  # Should not be loaded more than once
                
        finally:
            os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)


class TestErrorScenarios:
    """Test various error scenarios."""

    def test_import_error_handling(self, mock_settings):
        """Test graceful handling of module import errors."""
        with patch('aurum.api.router_registry.get_v1_split_manager') as mock_get_manager:
            mock_v1_manager = MagicMock()
            mock_get_manager.return_value = mock_v1_manager
            
            with patch('aurum.api.router_registry._check_v1_flag_sync', return_value=True):
                with patch('aurum.api.router_registry._try_import_router', return_value=None):
                    # Should not raise exception when import fails
                    specs = get_v1_router_specs(mock_settings)
                    
                    # Should still return valid specs list
                    assert isinstance(specs, list)

    def test_v1_manager_error_handling(self, mock_settings):
        """Test handling when V1 manager has issues."""
        with patch('aurum.api.router_registry.get_v1_split_manager', side_effect=Exception("Manager error")):
            # Should fall back to environment variables
            os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
            
            try:
                with patch('aurum.api.router_registry._try_import_router') as mock_import:
                    mock_router = MagicMock()
                    mock_import.return_value = mock_router
                    
                    specs = get_v1_router_specs(mock_settings)
                    
                    # Should still work with env var fallback
                    module_names = [spec.name for spec in specs if spec.name]
                    assert any("eia" in name for name in module_names)
                    
            finally:
                os.environ.pop("AURUM_API_V1_SPLIT_EIA", None)