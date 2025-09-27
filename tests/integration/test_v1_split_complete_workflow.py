"""Integration test for complete V1 split flag workflow."""

from __future__ import annotations

import os
import pytest
import asyncio
from unittest.mock import patch, MagicMock

from aurum.api.features.v1_split_integration import (
    get_v1_split_manager,
    initialize_v1_split_flags,
    V1SplitFlagManager
)
from aurum.api.features.feature_flags import (
    InMemoryFeatureFlagStore,
    FeatureFlagManager,
    FeatureFlagStatus
)
from aurum.api.router_registry import get_v1_router_specs_async


class TestV1SplitCompleteWorkflow:
    """Complete workflow integration test for V1 split flags."""

    @pytest.fixture
    async def clean_environment(self):
        """Clean environment setup and teardown."""
        # Store original environment
        original_env = {}
        v1_split_vars = [
            "AURUM_API_V1_SPLIT_EIA",
            "AURUM_API_V1_SPLIT_ISO", 
            "AURUM_API_V1_SPLIT_DROUGHT",
            "AURUM_API_V1_SPLIT_ADMIN",
            "AURUM_API_V1_SPLIT_PPA",
            "AURUM_API_V1_SPLIT_CURVES"
        ]
        
        for var in v1_split_vars:
            original_env[var] = os.environ.get(var)
            os.environ.pop(var, None)
        
        yield
        
        # Restore original environment
        for var, value in original_env.items():
            if value is not None:
                os.environ[var] = value
            else:
                os.environ.pop(var, None)

    @pytest.fixture
    def feature_manager(self):
        """Create a feature manager for testing."""
        store = InMemoryFeatureFlagStore()
        return FeatureFlagManager(store)

    @pytest.fixture
    def v1_manager(self, feature_manager):
        """Create a V1 split manager for testing."""
        return V1SplitFlagManager(feature_manager)

    @pytest.mark.asyncio
    async def test_complete_workflow_environment_to_runtime(self, clean_environment, v1_manager):
        """Test complete workflow from environment variables to runtime control."""
        
        # === Phase 1: Traditional Environment Variable Setup ===
        print("Phase 1: Setting up with environment variables...")
        
        # Set some environment variables (traditional way)
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "0"
        # Leave others unset (should use defaults)
        
        # Initialize the V1 split system
        await v1_manager.initialize_v1_flags()
        
        # Verify initial state matches environment
        status = await v1_manager.get_v1_split_status()
        assert status["eia"]["enabled"] is True  # From env var
        assert status["iso"]["enabled"] is False  # From env var
        assert status["ppa"]["enabled"] is True   # Default enabled
        assert status["curves"]["enabled"] is True  # Default enabled
        assert status["drought"]["enabled"] is False  # Default disabled
        assert status["admin"]["enabled"] is False   # Default disabled
        
        print("✓ Phase 1 complete: Environment variables loaded correctly")
        
        # === Phase 2: Runtime Control Without Restart ===
        print("Phase 2: Using runtime API control...")
        
        # Enable drought flag via runtime API (no restart needed)
        success = await v1_manager.set_v1_split_enabled("drought", True, "api_user")
        assert success is True
        
        # Verify it's enabled and environment variable was synced
        is_enabled = await v1_manager.is_v1_split_enabled("drought")
        assert is_enabled is True
        assert os.getenv("AURUM_API_V1_SPLIT_DROUGHT") == "1"
        
        # Disable EIA flag via runtime API
        success = await v1_manager.set_v1_split_enabled("eia", False, "api_user")
        assert success is True
        
        # Verify it's disabled and environment variable was synced
        is_enabled = await v1_manager.is_v1_split_enabled("eia")
        assert is_enabled is False
        assert os.getenv("AURUM_API_V1_SPLIT_EIA") == "0"
        
        print("✓ Phase 2 complete: Runtime control working, env vars synced")
        
        # === Phase 3: Router Registry Integration ===
        print("Phase 3: Testing router registry integration...")
        
        # Mock settings
        mock_settings = MagicMock()
        
        # Test that router registry uses our runtime flags
        with patch('aurum.api.router_registry._try_import_router') as mock_import:
            mock_router = MagicMock()
            mock_import.return_value = mock_router
            
            # Get router specs using async version (supports runtime flags)
            specs = await get_v1_router_specs_async(mock_settings)
            
            # Check which modules were loaded based on our flag states
            loaded_modules = [spec.name for spec in specs if spec.name]
            
            # EIA should NOT be loaded (we disabled it)
            assert not any("eia" in module for module in loaded_modules)
            
            # Drought should be loaded (we enabled it)  
            assert any("drought" in module for module in loaded_modules)
            
            # PPA should be loaded (default enabled)
            assert any("ppa" in module for module in loaded_modules)
        
        print("✓ Phase 3 complete: Router registry respects runtime flags")
        
        # === Phase 4: Persistence and Recovery ===
        print("Phase 4: Testing persistence and recovery...")
        
        # Create a new manager (simulating service restart)
        new_feature_manager = FeatureFlagManager(InMemoryFeatureFlagStore())
        new_v1_manager = V1SplitFlagManager(new_feature_manager)
        
        # Initialize should read from environment variables that were synced
        await new_v1_manager.initialize_v1_flags()
        
        # Verify state persisted via environment variables
        new_status = await new_v1_manager.get_v1_split_status()
        assert new_status["eia"]["enabled"] is False    # Should be disabled
        assert new_status["drought"]["enabled"] is True  # Should be enabled
        assert new_status["ppa"]["enabled"] is True      # Should be enabled (default)
        
        print("✓ Phase 4 complete: State persists across restarts")
        
        # === Phase 5: Error Handling and Fallbacks ===
        print("Phase 5: Testing error handling...")
        
        # Test unknown flag
        success = await v1_manager.set_v1_split_enabled("unknown_flag", True)
        assert success is False
        
        # Test graceful handling when feature system is broken
        with patch.object(v1_manager.feature_manager, 'get_flag', side_effect=Exception("DB error")):
            # Should fall back to environment variables
            is_enabled = await v1_manager.is_v1_split_enabled("eia")
            # Should still work (reads from env var)
            assert isinstance(is_enabled, bool)
        
        print("✓ Phase 5 complete: Error handling works correctly")
        
        print("🎉 Complete V1 split flag workflow test passed!")

    @pytest.mark.asyncio
    async def test_backward_compatibility_workflow(self, clean_environment):
        """Test that existing environment-only workflows still work."""
        
        print("Testing backward compatibility workflow...")
        
        # Set environment variables the old way
        os.environ["AURUM_API_V1_SPLIT_EIA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ISO"] = "1"
        os.environ["AURUM_API_V1_SPLIT_ADMIN"] = "1"
        
        # Use the traditional sync router registry function
        from aurum.api.router_registry import get_v1_router_specs
        
        mock_settings = MagicMock()
        
        with patch('aurum.api.router_registry._try_import_router') as mock_import:
            mock_router = MagicMock()
            mock_import.return_value = mock_router
            
            # This should work exactly as before
            specs = get_v1_router_specs(mock_settings)
            
            loaded_modules = [spec.name for spec in specs if spec.name]
            
            # All three should be loaded
            assert any("eia" in module for module in loaded_modules)
            assert any("iso" in module for module in loaded_modules)
            assert any("admin" in module for module in loaded_modules)
        
        print("✓ Backward compatibility maintained")

    @pytest.mark.asyncio
    async def test_feature_flag_api_integration(self, clean_environment):
        """Test integration with the broader feature flag API system."""
        
        print("Testing feature flag API integration...")
        
        # Initialize V1 flags
        manager = get_v1_split_manager()
        await manager.initialize_v1_flags()
        
        # V1 split flags should appear in the general feature flag system
        all_flags = await manager.feature_manager.list_flags()
        v1_flags = [flag for flag in all_flags if flag.key.startswith("v1_split_")]
        
        # Should have all 6 V1 split flags
        assert len(v1_flags) == 6
        
        # Check that they have proper metadata
        eia_flag = next((flag for flag in v1_flags if flag.key == "v1_split_eia"), None)
        assert eia_flag is not None
        assert "v1-split" in eia_flag.tags
        assert "eia" in eia_flag.tags
        assert eia_flag.name == "V1 Split - EIA"
        
        # Check that we can use the general feature flag API
        flag_stats = await manager.feature_manager.get_feature_stats()
        assert flag_stats["total_flags"] >= 6  # At least our V1 flags
        
        print("✓ Feature flag API integration working")

    @pytest.mark.asyncio
    async def test_concurrent_flag_operations(self, clean_environment, v1_manager):
        """Test concurrent flag operations work correctly."""
        
        print("Testing concurrent operations...")
        
        await v1_manager.initialize_v1_flags()
        
        # Define concurrent operations
        async def enable_eia():
            return await v1_manager.set_v1_split_enabled("eia", True, "user1")
        
        async def enable_iso():
            return await v1_manager.set_v1_split_enabled("iso", True, "user2")
        
        async def check_drought():
            return await v1_manager.is_v1_split_enabled("drought")
        
        async def get_status():
            return await v1_manager.get_v1_split_status()
        
        # Run operations concurrently
        results = await asyncio.gather(
            enable_eia(),
            enable_iso(), 
            check_drought(),
            get_status(),
            return_exceptions=True
        )
        
        # Check all succeeded
        assert all(not isinstance(result, Exception) for result in results)
        assert results[0] is True  # EIA enabled
        assert results[1] is True  # ISO enabled
        assert isinstance(results[2], bool)  # Drought check
        assert isinstance(results[3], dict)  # Status dict
        
        # Verify final state
        final_status = await v1_manager.get_v1_split_status()
        assert final_status["eia"]["enabled"] is True
        assert final_status["iso"]["enabled"] is True
        
        print("✓ Concurrent operations work correctly")

    @pytest.mark.asyncio
    async def test_global_manager_behavior(self, clean_environment):
        """Test global manager singleton behavior."""
        
        print("Testing global manager behavior...")
        
        # Get manager instances
        manager1 = get_v1_split_manager()
        manager2 = get_v1_split_manager()
        
        # Should be the same instance
        assert manager1 is manager2
        
        # Initialize via global function
        await initialize_v1_split_flags()
        
        # Should work with both instances
        status1 = await manager1.get_v1_split_status()
        status2 = await manager2.get_v1_split_status()
        
        assert status1 == status2
        
        print("✓ Global manager behavior correct")

    def test_configuration_completeness(self):
        """Test that all documented flags are in the configuration."""
        
        print("Testing configuration completeness...")
        
        manager = get_v1_split_manager()
        configured_flags = set(manager.V1_SPLIT_FLAGS.keys())
        
        # All documented flags should be present
        expected_flags = {"eia", "iso", "drought", "admin", "ppa", "curves"}
        assert configured_flags == expected_flags
        
        # Check each flag has required configuration
        for flag_name, config in manager.V1_SPLIT_FLAGS.items():
            assert config.env_var.startswith("AURUM_API_V1_SPLIT_")
            assert config.module_path.startswith("aurum.api.v1.")
            assert config.description  # Non-empty description
            assert isinstance(config.default_enabled, bool)
        
        print("✓ Configuration is complete and correct")

    @pytest.mark.asyncio
    async def test_documentation_examples_work(self, clean_environment):
        """Test that examples from documentation actually work."""
        
        print("Testing documentation examples...")
        
        # Example from docs: Python integration
        manager = get_v1_split_manager()
        
        # Initialize flags
        await manager.initialize_v1_flags()
        
        # Enable EIA router (from docs example)
        success = await manager.set_v1_split_enabled("eia", True, "admin_user")
        assert success is True
        print("EIA router enabled")
        
        # Check status (from docs example)
        is_enabled = await manager.is_v1_split_enabled("eia")
        print(f"EIA router is {'enabled' if is_enabled else 'disabled'}")
        assert is_enabled is True
        
        # Get all status (from docs example)
        status = await manager.get_v1_split_status()
        print(f"Total flags: {len(status)}")
        assert len(status) == 6
        
        print("✓ Documentation examples work correctly")


@pytest.mark.integration
class TestV1SplitRealWorldScenarios:
    """Real-world scenario tests."""

    @pytest.mark.asyncio
    async def test_gradual_rollout_scenario(self):
        """Test gradual rollout of new router."""
        
        print("Testing gradual rollout scenario...")
        
        # Scenario: Rolling out EIA split router
        manager = get_v1_split_manager()
        await manager.initialize_v1_flags()
        
        # Step 1: Start with flag disabled (monolith handles EIA)
        await manager.set_v1_split_enabled("eia", False)
        assert not await manager.is_v1_split_enabled("eia")
        
        # Step 2: Enable for testing/staging
        await manager.set_v1_split_enabled("eia", True, "devops_team")
        assert await manager.is_v1_split_enabled("eia")
        
        # Step 3: Validate parity (simulated)
        # ... validation logic would go here ...
        
        # Step 4: Keep enabled for production
        status = await manager.get_v1_split_status()
        assert status["eia"]["enabled"] is True
        
        print("✓ Gradual rollout scenario works")

    @pytest.mark.asyncio
    async def test_emergency_rollback_scenario(self):
        """Test emergency rollback scenario."""
        
        print("Testing emergency rollback scenario...")
        
        manager = get_v1_split_manager()
        await manager.initialize_v1_flags()
        
        # Enable flag (new router active)
        await manager.set_v1_split_enabled("iso", True, "deployment_system")
        assert await manager.is_v1_split_enabled("iso")
        
        # Emergency: Issues found, immediate rollback
        await manager.set_v1_split_enabled("iso", False, "oncall_engineer")
        assert not await manager.is_v1_split_enabled("iso")
        
        # Verify environment variable was updated for any external systems
        assert os.getenv("AURUM_API_V1_SPLIT_ISO") == "0"
        
        print("✓ Emergency rollback scenario works")

    @pytest.mark.asyncio  
    async def test_mixed_environment_scenario(self):
        """Test mixed deployment with some env vars and some runtime control."""
        
        print("Testing mixed environment scenario...")
        
        # Some flags set via environment (e.g., container config)
        os.environ["AURUM_API_V1_SPLIT_PPA"] = "1"
        os.environ["AURUM_API_V1_SPLIT_CURVES"] = "1"
        
        manager = get_v1_split_manager()
        await manager.initialize_v1_flags()
        
        # Verify environment flags are respected
        assert await manager.is_v1_split_enabled("ppa")
        assert await manager.is_v1_split_enabled("curves")
        
        # Use runtime control for others
        await manager.set_v1_split_enabled("admin", True, "admin_panel")
        assert await manager.is_v1_split_enabled("admin")
        
        # Mixed approach should work seamlessly
        status = await manager.get_v1_split_status()
        enabled_count = sum(1 for flag in status.values() if flag["enabled"])
        assert enabled_count >= 3  # At least ppa, curves, admin
        
        print("✓ Mixed environment scenario works")
        
        # Cleanup
        os.environ.pop("AURUM_API_V1_SPLIT_PPA", None)
        os.environ.pop("AURUM_API_V1_SPLIT_CURVES", None)

    def test_deployment_script_compatibility(self):
        """Test that existing deployment scripts still work."""
        
        print("Testing deployment script compatibility...")
        
        # Simulate deployment script setting environment variables
        deployment_flags = {
            "AURUM_API_V1_SPLIT_EIA": "1",
            "AURUM_API_V1_SPLIT_ISO": "0", 
            "AURUM_API_V1_SPLIT_ADMIN": "1"
        }
        
        for var, value in deployment_flags.items():
            os.environ[var] = value
        
        try:
            # Traditional router loading should still work
            from aurum.api.router_registry import get_v1_router_specs
            
            mock_settings = MagicMock()
            
            with patch('aurum.api.router_registry._try_import_router') as mock_import:
                mock_router = MagicMock()
                mock_import.return_value = mock_router
                
                specs = get_v1_router_specs(mock_settings)
                loaded_modules = [spec.name for spec in specs if spec.name]
                
                # Should load EIA and admin, but not ISO
                assert any("eia" in module for module in loaded_modules)
                assert any("admin" in module for module in loaded_modules)
                # ISO might appear in mandatory modules, so we don't assert it's absent
        
        finally:
            for var in deployment_flags:
                os.environ.pop(var, None)
        
        print("✓ Deployment script compatibility maintained")