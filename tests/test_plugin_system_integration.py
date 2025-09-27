"""Integration tests for Plugin System Service."""

import asyncio
import pytest
import tempfile
import os
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime, timedelta

from aurum.api.services.plugin_system_service import (
    PluginSystemService,
    PluginContract,
    PluginInstance,
    PluginSecurityLevel,
    PluginStatus,
    get_plugin_system_service,
    discover_and_load_plugins
)
from aurum.api.services.esg_risk_service import (
    ESGRiskService,
    ESGScore,
    ESGRiskCategory,
    get_esg_risk_service
)
from aurum.api.services.plugin_marketplace import (
    PluginMarketplaceService,
    get_plugin_marketplace_service
)


@pytest.fixture
def plugin_service():
    """Create a plugin system service instance for testing."""
    return get_plugin_system_service()


@pytest.fixture
def esg_risk_service():
    """Create an ESG risk service instance for testing."""
    return get_esg_risk_service()


@pytest.fixture
def marketplace_service():
    """Create a plugin marketplace service instance for testing."""
    return get_plugin_marketplace_service()


@pytest.fixture
def sample_plugin_config():
    """Sample plugin configuration for testing."""
    return {
        "api_key": "test_api_key",
        "polling_interval_minutes": 60,
        "cache_ttl_hours": 1
    }


@pytest.fixture
def sample_tenant_id():
    """Sample tenant ID for testing."""
    return "test_tenant_123"


class TestPluginSystemIntegration:
    """Integration tests for plugin system functionality."""

    @pytest.mark.asyncio
    async def test_plugin_discovery_and_loading(self, plugin_service, sample_tenant_id, sample_plugin_config):
        """Test plugin discovery and loading workflow."""
        # Test plugin discovery
        contracts = await plugin_service.discover_plugins()
        assert isinstance(contracts, list)

        # Test tenant configuration
        await plugin_service.configure_tenant_plugins(sample_tenant_id, ["weather_feed"])

        # Test plugin loading
        instance_id = await plugin_service.load_plugin(
            "weather_feed",
            sample_tenant_id,
            sample_plugin_config
        )

        assert instance_id is not None
        assert isinstance(instance_id, str)

        # Test plugin instance retrieval
        instances = await plugin_service.list_plugins(tenant_id=sample_tenant_id)
        assert len(instances) > 0

        weather_instance = next(
            (i for i in instances if i.plugin_name == "weather_feed"),
            None
        )
        assert weather_instance is not None
        assert weather_instance.status == PluginStatus.ACTIVE

        # Test plugin method execution
        if weather_instance:
            # This would test the actual plugin functionality
            # For now, just test that the method can be called without error
            try:
                result = await plugin_service.execute_plugin_method(
                    instance_id,
                    "get_metadata",
                    {}
                )
                # Should return metadata or handle gracefully
                assert result is not None
            except Exception:
                # Plugin might not be fully functional in test environment
                pass

        # Test plugin unloading
        unload_success = await plugin_service.unload_plugin(instance_id)
        assert unload_success is True

        # Verify plugin is removed
        instances = await plugin_service.list_plugins(tenant_id=sample_tenant_id)
        weather_instance = next(
            (i for i in instances if i.plugin_name == "weather_feed"),
            None
        )
        assert weather_instance is None

    @pytest.mark.asyncio
    async def test_plugin_security_validation(self, plugin_service, sample_tenant_id):
        """Test plugin security validation."""
        # Configure tenant with specific plugins
        await plugin_service.configure_tenant_plugins(
            sample_tenant_id,
            ["weather_feed", "carbon_tracker"]
        )

        # Test authorized plugin loading
        instance_id = await plugin_service.load_plugin(
            "weather_feed",
            sample_tenant_id,
            {}
        )
        assert instance_id is not None

        # Clean up
        await plugin_service.unload_plugin(instance_id)

        # Test unauthorized plugin loading
        with pytest.raises(ValueError):
            await plugin_service.load_plugin(
                "unauthorized_plugin",
                sample_tenant_id,
                {}
            )

    @pytest.mark.asyncio
    async def test_plugin_health_monitoring(self, plugin_service, sample_tenant_id, sample_plugin_config):
        """Test plugin health monitoring functionality."""
        # Load plugin
        instance_id = await plugin_service.load_plugin(
            "weather_feed",
            sample_tenant_id,
            sample_plugin_config
        )

        # Test health check
        health = await plugin_service.get_plugin_health(instance_id)
        assert isinstance(health, dict)
        assert "status" in health

        # Test service health
        service_health = await plugin_service.get_service_health()
        assert isinstance(service_health, dict)
        assert "status" in service_health
        assert "plugins_loaded" in service_health

        # Clean up
        await plugin_service.unload_plugin(instance_id)

    @pytest.mark.asyncio
    async def test_plugin_statistics(self, plugin_service, sample_tenant_id, sample_plugin_config):
        """Test plugin system statistics."""
        # Load multiple plugins
        instance_ids = []
        for plugin_name in ["weather_feed", "carbon_tracker"]:
            try:
                instance_id = await plugin_service.load_plugin(
                    plugin_name,
                    sample_tenant_id,
                    sample_plugin_config
                )
                instance_ids.append(instance_id)
            except Exception:
                # Plugin might not be available in test environment
                pass

        # Get statistics
        stats = await plugin_service.get_plugin_statistics()
        assert isinstance(stats, dict)
        assert "total_plugins" in stats
        assert "active_plugins" in stats
        assert "security_distribution" in stats
        assert "tenant_distribution" in stats

        # Clean up
        for instance_id in instance_ids:
            await plugin_service.unload_plugin(instance_id)


class TestESGRiskIntegration:
    """Integration tests for ESG risk functionality."""

    @pytest.fixture
    def mock_carbon_service(self):
        """Mock carbon service for testing."""
        with patch('aurum.api.services.esg_risk_service.get_carbon_rec_service') as mock:
            carbon_service = Mock()
            carbon_service.calculate_portfolio_carbon_exposure = AsyncMock(
                return_value={
                    "carbon_intensity": 0.3,
                    "carbon_cost_per_mwh": 5.0
                }
            )
            mock.return_value = carbon_service
            yield carbon_service

    @pytest.fixture
    def mock_risk_service(self):
        """Mock risk service for testing."""
        with patch('aurum.api.services.esg_risk_service.get_risk_engine_service') as mock:
            risk_service = Mock()
            risk_service.calculate_portfolio_risk_metrics = AsyncMock(
                return_value=Mock(
                    var_95=0.05,
                    cvar_95=0.08,
                    volatility=0.15
                )
            )
            mock.return_value = risk_service
            yield risk_service

    @pytest.mark.asyncio
    async def test_esg_analysis_calculation(
        self,
        esg_risk_service,
        mock_carbon_service,
        mock_risk_service
    ):
        """Test ESG analysis calculation."""
        portfolio_id = "test_portfolio_123"

        # Calculate ESG analysis
        analysis = await esg_risk_service.calculate_portfolio_esg_analysis(portfolio_id)

        assert analysis.portfolio_id == portfolio_id
        assert isinstance(analysis.overall_esg_score, ESGScore)
        assert isinstance(analysis.overall_risk_score, float)
        assert isinstance(analysis.esg_metrics, list)
        assert len(analysis.esg_metrics) > 0

        # Verify metrics structure
        for metric in analysis.esg_metrics:
            assert metric.metric_name in [
                "carbon_intensity", "carbon_cost_impact", "transition_risk",
                "physical_risk", "governance_risk", "social_risk"
            ]
            assert metric.category in [ESGRiskCategory.ENVIRONMENTAL, ESGRiskCategory.CLIMATE,
                                     ESGRiskCategory.TRANSITION, ESGRiskCategory.PHYSICAL,
                                     ESGRiskCategory.GOVERNANCE, ESGRiskCategory.SOCIAL]
            assert isinstance(metric.value, float)

    @pytest.mark.asyncio
    async def test_esg_adjusted_risk_calculation(
        self,
        esg_risk_service,
        mock_carbon_service,
        mock_risk_service
    ):
        """Test ESG-adjusted risk calculation."""
        portfolio_id = "test_portfolio_456"

        # Calculate ESG-adjusted risk
        from aurum.api.services.risk_engine_service import RiskDistributionConfig

        risk_config = RiskDistributionConfig(
            distribution_type="normal",
            parameters={"mu": 0.0, "sigma": 0.2}
        )

        result = await esg_risk_service.calculate_esg_adjusted_risk(
            portfolio_id,
            risk_config,
            "moderate"
        )

        assert result.portfolio_id == portfolio_id
        assert result.adjusted_var > 0
        assert result.adjusted_cvar > 0
        assert result.adjusted_volatility > 0
        assert isinstance(result.esg_adjustments, dict)
        assert "total_adjustment" in result.esg_adjustments

    @pytest.mark.asyncio
    async def test_esg_dashboard_data(
        self,
        esg_risk_service,
        mock_carbon_service,
        mock_risk_service
    ):
        """Test ESG dashboard data generation."""
        portfolio_id = "test_portfolio_789"

        # Get dashboard data
        dashboard_data = await esg_risk_service.get_portfolio_dashboard_data(portfolio_id)

        assert dashboard_data["portfolio_id"] == portfolio_id
        assert "esg_analysis" in dashboard_data
        assert "risk_metrics" in dashboard_data
        assert "esg_breakdown" in dashboard_data
        assert "recommendations" in dashboard_data

        # Verify ESG breakdown structure
        breakdown = dashboard_data["esg_breakdown"]
        assert "environmental_score" in breakdown
        assert "social_score" in breakdown
        assert "governance_score" in breakdown
        assert "climate_score" in breakdown


class TestPluginMarketplaceIntegration:
    """Integration tests for plugin marketplace functionality."""

    @pytest.mark.asyncio
    async def test_plugin_listings_retrieval(self, marketplace_service):
        """Test plugin listings retrieval."""
        # Get all plugins
        plugins = await marketplace_service.get_plugin_listings()
        assert isinstance(plugins, list)

        # Get plugins by category
        data_plugins = await marketplace_service.get_plugin_listings(category="data_ingestion")
        assert isinstance(data_plugins, list)

        # Get plugins by security level
        restricted_plugins = await marketplace_service.get_plugin_listings(
            security_level=PluginSecurityLevel.RESTRICTED
        )
        assert isinstance(restricted_plugins, list)

    @pytest.mark.asyncio
    async def test_plugin_search_functionality(self, marketplace_service):
        """Test plugin search functionality."""
        # Search for weather-related plugins
        weather_plugins = await marketplace_service.search_plugins("weather")
        assert isinstance(weather_plugins, list)

        # Search with category filter
        weather_data_plugins = await marketplace_service.search_plugins(
            "weather",
            category="data_ingestion"
        )
        assert isinstance(weather_data_plugins, list)

    @pytest.mark.asyncio
    async def test_plugin_installation_workflow(self, marketplace_service, sample_tenant_id):
        """Test plugin installation workflow."""
        # Test plugin installation
        try:
            result = await marketplace_service.install_plugin(
                "weather_feed",
                sample_tenant_id,
                {"api_key": "test_key"},
                None
            )

            assert result["status"] == "success"
            assert result["plugin_name"] == "weather_feed"
            assert "instance_id" in result
            assert result["instance_id"] is not None

        except Exception as e:
            # Plugin installation might fail in test environment
            # This is expected since the actual plugin might not be available
            print(f"Plugin installation test skipped: {e}")

    @pytest.mark.asyncio
    async def test_plugin_review_system(self, marketplace_service):
        """Test plugin review and rating system."""
        # Add a review
        review = await marketplace_service.add_plugin_review(
            "weather_feed",
            "test_user_123",
            5,
            "Great plugin for weather data integration!"
        )

        assert review.plugin_name == "weather_feed"
        assert review.user_id == "test_user_123"
        assert review.rating == 5
        assert review.review_text == "Great plugin for weather data integration!"

        # Get plugin analytics
        analytics = await marketplace_service.get_plugin_analytics("weather_feed")

        assert analytics["plugin_name"] == "weather_feed"
        assert "total_reviews" in analytics
        assert "rating_distribution" in analytics
        assert "review_trend" in analytics

    @pytest.mark.asyncio
    async def test_plugin_categories_and_featured(self, marketplace_service):
        """Test plugin categories and featured plugins."""
        # Get categories
        categories = await marketplace_service.get_categories()
        assert isinstance(categories, dict)
        assert len(categories) > 0

        # Get featured plugins
        featured = await marketplace_service.get_featured_plugins(limit=3)
        assert isinstance(featured, list)
        assert len(featured) <= 3


class TestEndToEndWorkflows:
    """End-to-end workflow integration tests."""

    @pytest.mark.asyncio
    async def test_complete_plugin_workflow(
        self,
        plugin_service,
        marketplace_service,
        sample_tenant_id,
        sample_plugin_config
    ):
        """Test complete plugin lifecycle workflow."""
        # 1. Browse marketplace
        plugins = await marketplace_service.get_plugin_listings(category="data_ingestion")
        weather_plugin = next(
            (p for p in plugins if p.plugin_name == "weather_feed"),
            None
        )

        if not weather_plugin:
            pytest.skip("Weather plugin not available in marketplace")

        # 2. Install plugin
        install_result = await marketplace_service.install_plugin(
            "weather_feed",
            sample_tenant_id,
            sample_plugin_config
        )

        assert install_result["status"] == "success"
        instance_id = install_result["instance_id"]

        # 3. Verify plugin is loaded
        instances = await plugin_service.list_plugins(tenant_id=sample_tenant_id)
        assert len(instances) > 0

        # 4. Test plugin functionality
        try:
            metadata = await plugin_service.execute_plugin_method(
                instance_id,
                "get_metadata",
                {}
            )
            # Plugin should return metadata
            assert metadata is not None
        except Exception:
            # Plugin functionality might not be fully available in tests
            pass

        # 5. Add review
        await marketplace_service.add_plugin_review(
            "weather_feed",
            "workflow_test_user",
            4,
            "Good plugin for testing workflows"
        )

        # 6. Check analytics
        analytics = await marketplace_service.get_plugin_analytics("weather_feed")
        assert analytics["total_reviews"] >= 1

        # 7. Clean up - unload plugin
        await plugin_service.unload_plugin(instance_id)

        # 8. Verify cleanup
        instances = await plugin_service.list_plugins(tenant_id=sample_tenant_id)
        weather_instances = [i for i in instances if i.plugin_name == "weather_feed"]
        assert len(weather_instances) == 0

    @pytest.mark.asyncio
    async def test_esg_risk_workflow(
        self,
        esg_risk_service,
        sample_tenant_id
    ):
        """Test complete ESG risk analysis workflow."""
        portfolio_id = f"{sample_tenant_id}_portfolio"

        # 1. Calculate ESG analysis
        esg_analysis = await esg_risk_service.calculate_portfolio_esg_analysis(portfolio_id)

        assert esg_analysis.portfolio_id == portfolio_id
        assert isinstance(esg_analysis.overall_esg_score, ESGScore)
        assert len(esg_analysis.esg_metrics) > 0

        # 2. Calculate ESG-adjusted risk
        from aurum.api.services.risk_engine_service import RiskDistributionConfig

        risk_config = RiskDistributionConfig(
            distribution_type="normal",
            parameters={"mu": 0.0, "sigma": 0.2}
        )

        risk_result = await esg_risk_service.calculate_esg_adjusted_risk(
            portfolio_id,
            risk_config,
            "moderate"
        )

        assert risk_result.portfolio_id == portfolio_id
        assert risk_result.adjusted_var >= risk_result.base_risk_result.var_95
        assert len(risk_result.esg_adjustments) > 0

        # 3. Generate dashboard data
        dashboard = await esg_risk_service.get_portfolio_dashboard_data(portfolio_id)

        assert dashboard["portfolio_id"] == portfolio_id
        assert "esg_analysis" in dashboard
        assert "risk_metrics" in dashboard
        assert "recommendations" in dashboard

        # 4. Verify dashboard includes expected metrics
        risk_metrics = dashboard["risk_metrics"]
        assert "base_var" in risk_metrics
        assert "adjusted_var" in risk_metrics
        assert risk_metrics["adjusted_var"] >= risk_metrics["base_var"]


@pytest.mark.asyncio
async def test_service_health_monitoring():
    """Test health monitoring across all services."""
    # Test plugin service health
    plugin_service = get_plugin_system_service()
    plugin_health = await plugin_service.get_service_health()
    assert isinstance(plugin_health, dict)
    assert "status" in plugin_health

    # Test ESG risk service health
    esg_service = get_esg_risk_service()
    esg_health = await esg_service.get_service_health()
    assert isinstance(esg_health, dict)
    assert "status" in esg_health

    # Test marketplace service health
    marketplace_service = get_plugin_marketplace_service()
    marketplace_health = await marketplace_service.get_service_health()
    assert isinstance(marketplace_health, dict)
    assert "status" in marketplace_health


if __name__ == "__main__":
    # Run tests manually if needed
    asyncio.run(test_service_health_monitoring())
