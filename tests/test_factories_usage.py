"""Example tests demonstrating factory usage."""

import pytest
from tests.factories import (
    ScenarioFactory,
    ScenarioRunFactory,
    CurveFactory,
    TenantFactory,
    UserFactory,
    ApiPayloadFactory,
)


@pytest.mark.unit
class TestFactoryUsage:
    """Demonstrate how factories eliminate payload construction duplication."""

    def test_scenario_factory_creates_realistic_data(self):
        """Test that scenario factory creates realistic test data."""
        scenario = ScenarioFactory()

        # Verify required fields are present
        assert "name" in scenario
        assert "description" in scenario
        assert "scenario_type" in scenario
        assert "assumptions" in scenario
        assert "parameters" in scenario

        # Verify data types and ranges
        assert isinstance(scenario["name"], str)
        assert len(scenario["name"]) > 0
        assert scenario["scenario_type"] in [
            "monte_carlo", "forecasting", "stress_test", "sensitivity_analysis"
        ]

        # Verify assumptions structure
        assumptions = scenario["assumptions"]
        assert isinstance(assumptions, list)
        assert len(assumptions) > 0
        for assumption in assumptions:
            assert "type" in assumption
            assert "value" in assumption
            assert isinstance(assumption["value"], (int, float))

    def test_scenario_run_factory_creates_complete_data(self):
        """Test that scenario run factory creates complete test data."""
        run = ScenarioRunFactory()

        # Verify required fields
        assert "scenario_id" in run
        assert "run_id" in run
        assert "status" in run
        assert "inputs" in run
        assert "outputs" in run

        # Verify status is valid
        assert run["status"] in ["pending", "running", "completed", "failed", "cancelled"]

        # Verify inputs structure
        inputs = run["inputs"]
        assert "curve_data" in inputs
        assert "market_data" in inputs
        assert "assumptions" in inputs

    def test_curve_factory_creates_time_series_data(self):
        """Test that curve factory creates realistic time series data."""
        curve = CurveFactory()

        # Verify required fields
        assert "curve_id" in curve
        assert "name" in curve
        assert "curve_type" in curve
        assert "commodity" in curve
        assert "data_points" in curve

        # Verify data types
        assert isinstance(curve["data_points"], list)
        assert len(curve["data_points"]) > 0

        # Verify data point structure
        for point in curve["data_points"]:
            assert "timestamp" in point
            assert "value" in point
            assert "confidence" in point
            assert isinstance(point["value"], (int, float))
            assert 0.0 <= point["confidence"] <= 1.0

    def test_tenant_factory_creates_comprehensive_data(self):
        """Test that tenant factory creates comprehensive test data."""
        tenant = TenantFactory()

        # Verify required fields
        assert "tenant_id" in tenant
        assert "name" in tenant
        assert "settings" in tenant
        assert "contact_info" in tenant
        assert "subscription" in tenant

        # Verify settings structure
        settings = tenant["settings"]
        assert "max_users" in settings
        assert "max_scenarios" in settings
        assert "feature_flags" in settings
        assert "rate_limits" in settings

        # Verify subscription structure
        subscription = tenant["subscription"]
        assert "plan" in subscription
        assert "billing_cycle" in subscription

    def test_api_payload_factory_creates_request_data(self):
        """Test that API payload factory creates realistic request data."""
        # Test scenario payload
        scenario_payload = ApiPayloadFactory.create_scenario_payload()
        assert "name" in scenario_payload
        assert "scenario_type" in scenario_payload
        assert "assumptions" in scenario_payload
        assert "parameters" in scenario_payload

        # Test with overrides
        overrides = {"name": "Custom Scenario Name"}
        custom_payload = ApiPayloadFactory.create_scenario_payload(overrides)
        assert custom_payload["name"] == "Custom Scenario Name"

    def test_multiple_factories_can_be_combined(self):
        """Test that multiple factories can be combined for complex scenarios."""
        # Create a tenant
        tenant = TenantFactory()

        # Create scenarios for that tenant
        scenarios = [ScenarioFactory() for _ in range(3)]

        # Create users for the tenant
        users = [UserFactory() for _ in range(2)]

        # Verify all data is consistent and realistic
        assert len(scenarios) == 3
        assert len(users) == 2
        assert "tenant_id" in tenant

        # All scenarios should have realistic structure
        for scenario in scenarios:
            assert "scenario_type" in scenario
            assert "assumptions" in scenario
            assert isinstance(scenario["assumptions"], list)

    def test_factory_data_is_deterministic_when_needed(self):
        """Test that factories can create deterministic data when required."""
        # Create factory with specific seed for deterministic data
        scenario1 = ScenarioFactory(name="Deterministic Scenario")
        scenario2 = ScenarioFactory(name="Deterministic Scenario")

        # These should be identical since we set the name explicitly
        assert scenario1["name"] == "Deterministic Scenario"
        assert scenario2["name"] == "Deterministic Scenario"

        # But other fields should still be generated
        assert "scenario_type" in scenario1
        assert "assumptions" in scenario1

    def test_factory_overrides_work_correctly(self):
        """Test that factory overrides work as expected."""
        # Create a base scenario
        base_scenario = ScenarioFactory()

        # Create a custom scenario with overrides
        custom_scenario = ScenarioFactory(
            name="Custom Name",
            scenario_type="forecasting",
            assumptions=[{"type": "custom", "value": 0.1}]
        )

        # Verify overrides are applied
        assert custom_scenario["name"] == "Custom Name"
        assert custom_scenario["scenario_type"] == "forecasting"
        assert custom_scenario["assumptions"] == [{"type": "custom", "value": 0.1}]

        # But other fields should still be generated
        assert "parameters" in custom_scenario
        assert "metadata" in custom_scenario
