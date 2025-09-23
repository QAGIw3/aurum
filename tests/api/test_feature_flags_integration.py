from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app


@pytest.fixture
def client():
    """Create test client for the API."""
    app = create_app()
    return TestClient(app)


class TestFeatureFlagEndpoints:
    """Integration tests for feature flag endpoints."""

    def test_create_feature_flag(self, client):
        """Test creating a feature flag."""
        response = client.post(
            "/v1/admin/features",
            json={
                "name": "Test Feature",
                "key": "test_feature",
                "description": "A test feature flag",
                "default_value": False,
                "status": "enabled",
                "tags": ["test", "integration"]
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Feature flag 'Test Feature' created successfully"
        assert data["feature_key"] == "test_feature"
        assert data["status"] == "enabled"

    def test_list_feature_flags(self, client):
        """Test listing feature flags."""
        # Create a feature flag first
        client.post(
            "/v1/admin/features",
            json={
                "name": "Test Feature",
                "key": "test_feature",
                "description": "A test feature flag",
                "status": "enabled"
            }
        )

        response = client.get("/v1/admin/features")

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "meta" in data
        assert data["meta"]["total_flags"] >= 1

        # Check that our test feature is in the list
        feature_keys = [f["key"] for f in data["data"]]
        assert "test_feature" in feature_keys

    def test_get_feature_flag(self, client):
        """Test getting a specific feature flag."""
        # Create a feature flag first
        client.post(
            "/v1/admin/features",
            json={
                "name": "Test Feature",
                "key": "test_feature",
                "description": "A test feature flag",
                "status": "enabled"
            }
        )

        response = client.get("/v1/admin/features/test_feature")

        assert response.status_code == 200
        data = response.json()
        assert data["data"]["key"] == "test_feature"
        assert data["data"]["name"] == "Test Feature"
        assert data["data"]["status"] == "enabled"

    def test_get_nonexistent_feature_flag(self, client):
        """Test getting a non-existent feature flag."""
        response = client.get("/v1/admin/features/nonexistent")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"].lower()

    def test_update_feature_flag_status(self, client):
        """Test updating feature flag status."""
        # Create a feature flag first
        client.post(
            "/v1/admin/features",
            json={
                "name": "Test Feature",
                "key": "test_feature",
                "status": "disabled"
            }
        )

        # Update status
        response = client.put(
            "/v1/admin/features/test_feature/status",
            json={"status": "enabled"}
        )

        assert response.status_code == 200
        data = response.json()
        assert data["message"] == "Feature flag 'test_feature' status updated to 'enabled'"
        assert data["new_status"] == "enabled"

    def test_get_feature_stats(self, client):
        """Test getting feature statistics."""
        # Create some feature flags
        client.post(
            "/v1/admin/features",
            json={
                "name": "Feature 1",
                "key": "feature_1",
                "status": "enabled"
            }
        )
        client.post(
            "/v1/admin/features",
            json={
                "name": "Feature 2",
                "key": "feature_2",
                "status": "disabled"
            }
        )

        response = client.get("/v1/admin/features/stats")

        assert response.status_code == 200
        data = response.json()
        assert data["data"]["total_flags"] == 2
        assert data["data"]["status_distribution"]["enabled"] == 1
        assert data["data"]["status_distribution"]["disabled"] == 1

    def test_evaluate_feature_flags(self, client):
        """Test evaluating feature flags for a user."""
        # Create a feature flag first
        client.post(
            "/v1/admin/features",
            json={
                "name": "Test Feature",
                "key": "test_feature",
                "status": "enabled"
            }
        )

        response = client.get(
            "/v1/admin/features/evaluate",
            params={
                "user_id": "test_user_123",
                "user_segment": "all_users"
            }
        )

        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert "flag_evaluations" in data["data"]
        assert "test_feature" in data["data"]["flag_evaluations"]
