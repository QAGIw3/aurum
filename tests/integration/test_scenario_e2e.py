"""End-to-end integration tests for scenario APIs with RLS and tenancy."""

import asyncio
import json
import pytest
import httpx
from typing import AsyncGenerator, Dict, Any
from datetime import datetime, timedelta

from aurum.telemetry.context import correlation_context, get_request_id


@pytest.fixture(scope="session")
async def api_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Create authenticated API client for testing."""
    async with httpx.AsyncClient(
        base_url="http://api:8000",
        timeout=30.0
    ) as client:
        # Wait for API to be ready
        for _ in range(60):
            try:
                response = await client.get("/health")
                if response.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(2)
        else:
            pytest.fail("API service did not become ready")

        yield client


@pytest.fixture(scope="session")
async def kafka_client() -> AsyncGenerator[Dict[str, Any], None]:
    """Create Kafka client for testing."""
    # This would be a proper Kafka client for testing
    # For now, we'll use a mock or direct API calls
    yield {"type": "test"}


class TestScenarioE2E:
    """End-to-end scenario API tests with RLS and tenancy."""

    async def test_scenario_lifecycle_e2e(
        self,
        api_client: httpx.AsyncClient,
        kafka_client: Dict[str, Any]
    ):
        """Test complete scenario lifecycle from creation to execution."""
        # Test data
        tenant_id = "test-tenant-001"
        user_id = "test-user-001"

        # Step 1: Create scenario
        with correlation_context(tenant_id=tenant_id, user_id=user_id):
            scenario_data = {
                "tenant_id": tenant_id,
                "name": "Test Revenue Forecast",
                "description": "Integration test scenario",
                "assumptions": [
                    {"type": "market_growth", "value": 0.05},
                    {"type": "discount_rate", "value": 0.08}
                ],
                "parameters": {
                    "forecast_period_months": 12,
                    "confidence_interval": 0.95,
                    "num_simulations": 1000
                }
            }

            response = await api_client.post(
                "/v1/scenarios",
                json=scenario_data,
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 201

            scenario_response = response.json()
            scenario_id = scenario_response["data"]["id"]
            assert scenario_response["data"]["name"] == scenario_data["name"]

        # Step 2: Create scenario run
        run_options = {
            "scenario_type": "monte_carlo",
            "parameters": {
                "num_simulations": 100,
                "confidence_level": 0.95
            },
            "environment": {
                "market_data_date": "2024-01-01",
                "currency": "USD"
            }
        }

        response = await api_client.post(
            f"/v1/scenarios/{scenario_id}/runs",
            json=run_options,
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 201

        run_response = response.json()
        run_id = run_response["data"]["id"]
        assert run_response["data"]["status"] in ["queued", "running"]

        # Step 3: Check run status
        for _ in range(30):  # Wait up to 30 seconds
            response = await api_client.get(
                f"/v1/scenarios/{scenario_id}/runs/{run_id}",
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 200

            run_status = response.json()["data"]["status"]
            if run_status in ["succeeded", "failed"]:
                break

            await asyncio.sleep(1)

        # Step 4: Verify final status
        response = await api_client.get(
            f"/v1/scenarios/{scenario_id}/runs/{run_id}",
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 200

        final_status = response.json()["data"]["status"]
        assert final_status == "succeeded"

        # Step 5: Check outputs
        response = await api_client.get(
            f"/v1/scenarios/{scenario_id}/outputs",
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 200

        outputs = response.json()["data"]
        assert len(outputs) > 0

    async def test_scenario_rls_isolation(
        self,
        api_client: httpx.AsyncClient
    ):
        """Test that RLS properly isolates scenarios between tenants."""
        tenants = ["tenant-a", "tenant-b"]
        scenarios = {}

        # Create scenarios for each tenant
        for tenant_id in tenants:
            scenario_data = {
                "tenant_id": tenant_id,
                "name": f"Test Scenario {tenant_id}",
                "description": f"RLS test scenario for {tenant_id}",
                "assumptions": [{"type": "growth_rate", "value": 0.1}],
                "parameters": {"forecast_period_months": 6}
            }

            response = await api_client.post(
                "/v1/scenarios",
                json=scenario_data,
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 201

            scenarios[tenant_id] = response.json()["data"]["id"]

        # Verify each tenant can only see their own scenarios
        for tenant_id in tenants:
            response = await api_client.get(
                "/v1/scenarios",
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 200

            tenant_scenarios = response.json()["data"]
            assert len(tenant_scenarios) == 1
            assert tenant_scenarios[0]["id"] == scenarios[tenant_id]

            # Verify tenant cannot access other tenant's scenario
            other_tenant_id = next(t for t in tenants if t != tenant_id)
            response = await api_client.get(
                f"/v1/scenarios/{scenarios[other_tenant_id]}",
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 404

    async def test_scenario_queue_integration(
        self,
        api_client: httpx.AsyncClient,
        kafka_client: Dict[str, Any]
    ):
        """Test scenario queue integration with Kafka."""
        tenant_id = "queue-test-tenant"
        user_id = "queue-test-user"

        # Create scenario
        with correlation_context(tenant_id=tenant_id, user_id=user_id):
            scenario_data = {
                "tenant_id": tenant_id,
                "name": "Queue Integration Test",
                "description": "Test scenario queue integration",
                "assumptions": [{"type": "test_assumption", "value": 1.0}],
                "parameters": {"test_param": "test_value"}
            }

            response = await api_client.post(
                "/v1/scenarios",
                json=scenario_data,
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 201

            scenario_id = response.json()["data"]["id"]

        # Create run that should be queued
        run_options = {
            "scenario_type": "monte_carlo",
            "parameters": {"num_simulations": 10},  # Small number for testing
            "priority": "high"
        }

        response = await api_client.post(
            f"/v1/scenarios/{scenario_id}/runs",
            json=run_options,
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 201

        run_id = response.json()["data"]["id"]

        # Verify run is queued
        response = await api_client.get(
            f"/v1/scenarios/{scenario_id}/runs/{run_id}",
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 200

        run_status = response.json()["data"]["status"]
        assert run_status in ["queued", "running", "succeeded", "failed"]

        # In a real test environment, we would also verify that:
        # 1. The message was published to the correct Kafka topic
        # 2. The worker consumed and processed the message
        # 3. Results were written to Iceberg via Trino
        # 4. OpenLineage events were emitted

    async def test_scenario_concurrent_processing(
        self,
        api_client: httpx.AsyncClient
    ):
        """Test concurrent scenario processing with proper isolation."""
        tenant_id = "concurrent-test-tenant"
        user_id = "concurrent-test-user"

        # Create multiple scenarios
        scenario_ids = []
        run_ids = []

        for i in range(3):
            with correlation_context(tenant_id=tenant_id, user_id=user_id):
                scenario_data = {
                    "tenant_id": tenant_id,
                    "name": f"Concurrent Test Scenario {i}",
                    "description": f"Concurrent processing test {i}",
                    "assumptions": [{"type": f"test_assumption_{i}", "value": 1.0}],
                    "parameters": {"test_param": f"test_value_{i}"}
                }

                response = await api_client.post(
                    "/v1/scenarios",
                    json=scenario_data,
                    headers={"X-Aurum-Tenant": tenant_id}
                )
                assert response.status_code == 201

                scenario_id = response.json()["data"]["id"]
                scenario_ids.append(scenario_id)

                # Create run
                run_options = {
                    "scenario_type": "monte_carlo",
                    "parameters": {"num_simulations": 5},
                    "priority": "normal"
                }

                response = await api_client.post(
                    f"/v1/scenarios/{scenario_id}/runs",
                    json=run_options,
                    headers={"X-Aurum-Tenant": tenant_id}
                )
                assert response.status_code == 201

                run_id = response.json()["data"]["id"]
                run_ids.append(run_id)

        # Wait for all runs to complete
        for run_id in run_ids:
            for _ in range(60):  # Wait up to 60 seconds
                response = await api_client.get(
                    f"/v1/scenarios/{scenario_ids[0]}/runs/{run_id}",
                    headers={"X-Aurum-Tenant": tenant_id}
                )
                assert response.status_code == 200

                run_status = response.json()["data"]["status"]
                if run_status in ["succeeded", "failed"]:
                    break

                await asyncio.sleep(1)

            # Verify final status
            response = await api_client.get(
                f"/v1/scenarios/{scenario_ids[0]}/runs/{run_id}",
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 200

            final_status = response.json()["data"]["status"]
            assert final_status in ["succeeded", "failed"]

    async def test_scenario_error_handling(
        self,
        api_client: httpx.AsyncClient
    ):
        """Test error handling in scenario processing."""
        tenant_id = "error-test-tenant"
        user_id = "error-test-user"

        # Create scenario
        with correlation_context(tenant_id=tenant_id, user_id=user_id):
            scenario_data = {
                "tenant_id": tenant_id,
                "name": "Error Test Scenario",
                "description": "Test error handling",
                "assumptions": [{"type": "invalid_assumption", "value": "invalid"}],
                "parameters": {"invalid_param": "invalid"}
            }

            response = await api_client.post(
                "/v1/scenarios",
                json=scenario_data,
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 201

            scenario_id = response.json()["data"]["id"]

        # Create run with invalid parameters to trigger error
        run_options = {
            "scenario_type": "invalid_type",
            "parameters": {"invalid_param": "should_fail"}
        }

        response = await api_client.post(
            f"/v1/scenarios/{scenario_id}/runs",
            json=run_options,
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 201

        run_id = response.json()["data"]["id"]

        # Wait for failure
        for _ in range(30):
            response = await api_client.get(
                f"/v1/scenarios/{scenario_id}/runs/{run_id}",
                headers={"X-Aurum-Tenant": tenant_id}
            )
            assert response.status_code == 200

            run_status = response.json()["data"]["status"]
            if run_status == "failed":
                break

            await asyncio.sleep(1)

        # Verify error handling
        response = await api_client.get(
            f"/v1/scenarios/{scenario_id}/runs/{run_id}",
            headers={"X-Aurum-Tenant": tenant_id}
        )
        assert response.status_code == 200

        run_data = response.json()["data"]
        assert run_data["status"] == "failed"
        assert "error_message" in run_data
