"""Tests for V1 split flag API endpoints."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
from fastapi import FastAPI

from aurum.api.features.feature_management import router
from aurum.api.features.v1_split_integration import V1SplitFlagManager


# Create a test FastAPI app with the router
app = FastAPI()
app.include_router(router)


class TestV1SplitAPIEndpoints:
    """Test V1 split flag API endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    @pytest.fixture
    def mock_principal(self):
        """Create mock authenticated principal."""
        return {"user_id": "test_admin", "groups": ["admin"]}

    @pytest.fixture
    def mock_v1_manager(self):
        """Create mock V1 split flag manager."""
        manager = AsyncMock(spec=V1SplitFlagManager)
        manager.initialize_v1_flags = AsyncMock()
        manager.get_v1_split_status = AsyncMock()
        manager.set_v1_split_enabled = AsyncMock()
        manager.get_env_var_for_flag = MagicMock()
        manager.get_module_path_for_flag = MagicMock()
        return manager

    def test_get_v1_split_flags_success(self, client, mock_principal, mock_v1_manager):
        """Test successful retrieval of V1 split flags."""
        # Mock the status response
        mock_status = {
            "eia": {
                "enabled": True,
                "env_var": "AURUM_API_V1_SPLIT_EIA",
                "module_path": "aurum.api.v1.eia",
                "description": "EIA API router",
                "default_enabled": False,
                "last_updated": "2024-01-01T12:00:00",
                "feature_key": "v1_split_eia"
            },
            "iso": {
                "enabled": False,
                "env_var": "AURUM_API_V1_SPLIT_ISO",
                "module_path": "aurum.api.v1.iso",
                "description": "ISO API router",
                "default_enabled": False,
                "last_updated": "2024-01-01T12:00:00",
                "feature_key": "v1_split_iso"
            }
        }
        mock_v1_manager.get_v1_split_status.return_value = mock_status

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits")

        assert response.status_code == 200
        data = response.json()
        
        assert "meta" in data
        assert "data" in data
        assert data["meta"]["total_flags"] == 2
        assert data["meta"]["enabled_flags"] == 1
        assert data["data"] == mock_status

        # Verify manager calls
        mock_v1_manager.initialize_v1_flags.assert_called_once()
        mock_v1_manager.get_v1_split_status.assert_called_once()

    def test_get_v1_split_flags_unauthorized(self, client):
        """Test unauthorized access to V1 split flags."""
        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=None):
            with patch('aurum.api.features.feature_management._routes._require_admin', side_effect=Exception("Unauthorized")):
                response = client.get("/v1/admin/features/v1-splits")

        assert response.status_code == 500  # FastAPI converts unhandled exceptions to 500

    def test_get_v1_split_flags_error(self, client, mock_principal, mock_v1_manager):
        """Test error handling in get V1 split flags."""
        mock_v1_manager.get_v1_split_status.side_effect = Exception("Database error")

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to get V1 split flags" in data["detail"]

    def test_toggle_v1_split_flag_enable_success(self, client, mock_principal, mock_v1_manager):
        """Test successful enabling of V1 split flag."""
        mock_v1_manager.set_v1_split_enabled.return_value = True
        mock_v1_manager.get_env_var_for_flag.return_value = "AURUM_API_V1_SPLIT_EIA"
        mock_v1_manager.get_module_path_for_flag.return_value = "aurum.api.v1.eia"

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.put("/v1/admin/features/v1-splits/eia", json=True)

        assert response.status_code == 200
        data = response.json()
        
        assert data["flag_name"] == "eia"
        assert data["enabled"] is True
        assert data["changed_by"] == "test_admin"
        assert data["env_var"] == "AURUM_API_V1_SPLIT_EIA"
        assert data["module_path"] == "aurum.api.v1.eia"
        assert "meta" in data

        # Verify manager call
        mock_v1_manager.set_v1_split_enabled.assert_called_once_with("eia", True, "test_admin")

    def test_toggle_v1_split_flag_disable_success(self, client, mock_principal, mock_v1_manager):
        """Test successful disabling of V1 split flag."""
        mock_v1_manager.set_v1_split_enabled.return_value = True
        mock_v1_manager.get_env_var_for_flag.return_value = "AURUM_API_V1_SPLIT_EIA"
        mock_v1_manager.get_module_path_for_flag.return_value = "aurum.api.v1.eia"

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.put("/v1/admin/features/v1-splits/eia", json=False)

        assert response.status_code == 200
        data = response.json()
        
        assert data["flag_name"] == "eia"
        assert data["enabled"] is False
        assert data["changed_by"] == "test_admin"

        # Verify manager call
        mock_v1_manager.set_v1_split_enabled.assert_called_once_with("eia", False, "test_admin")

    def test_toggle_v1_split_flag_failure(self, client, mock_principal, mock_v1_manager):
        """Test failure to toggle V1 split flag."""
        mock_v1_manager.set_v1_split_enabled.return_value = False  # Failure

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.put("/v1/admin/features/v1-splits/unknown_flag", json=True)

        assert response.status_code == 400
        data = response.json()
        assert "Failed to enable V1 split flag" in data["detail"]

    def test_toggle_v1_split_flag_error(self, client, mock_principal, mock_v1_manager):
        """Test error handling in toggle V1 split flag."""
        mock_v1_manager.set_v1_split_enabled.side_effect = Exception("Manager error")

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.put("/v1/admin/features/v1-splits/eia", json=True)

        assert response.status_code == 500
        data = response.json()
        assert "Failed to toggle V1 split flag" in data["detail"]

    def test_get_v1_split_flag_success(self, client, mock_principal, mock_v1_manager):
        """Test successful retrieval of specific V1 split flag."""
        mock_status = {
            "eia": {
                "enabled": True,
                "env_var": "AURUM_API_V1_SPLIT_EIA",
                "module_path": "aurum.api.v1.eia",
                "description": "EIA API router",
                "default_enabled": False,
                "last_updated": "2024-01-01T12:00:00",
                "feature_key": "v1_split_eia"
            }
        }
        mock_v1_manager.get_v1_split_status.return_value = mock_status

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits/eia")

        assert response.status_code == 200
        data = response.json()
        
        assert "meta" in data
        assert "data" in data
        assert data["data"] == mock_status["eia"]

    def test_get_v1_split_flag_not_found(self, client, mock_principal, mock_v1_manager):
        """Test retrieval of non-existent V1 split flag."""
        mock_v1_manager.get_v1_split_status.return_value = {}  # Empty status

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits/unknown_flag")

        assert response.status_code == 404
        data = response.json()
        assert "not found" in data["detail"]

    def test_get_v1_split_flag_error(self, client, mock_principal, mock_v1_manager):
        """Test error handling in get specific V1 split flag."""
        mock_v1_manager.get_v1_split_status.side_effect = Exception("Manager error")

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits/eia")

        assert response.status_code == 500
        data = response.json()
        assert "Failed to get V1 split flag" in data["detail"]

    def test_user_without_principal_in_toggle(self, client, mock_v1_manager):
        """Test toggle flag with no principal (should use 'admin' default)."""
        mock_v1_manager.set_v1_split_enabled.return_value = True
        mock_v1_manager.get_env_var_for_flag.return_value = "AURUM_API_V1_SPLIT_EIA"
        mock_v1_manager.get_module_path_for_flag.return_value = "aurum.api.v1.eia"

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=None):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.put("/v1/admin/features/v1-splits/eia", json=True)

        assert response.status_code == 200
        data = response.json()
        assert data["changed_by"] == "admin"  # Default when no principal

        # Verify manager call used default
        mock_v1_manager.set_v1_split_enabled.assert_called_once_with("eia", True, "admin")

    def test_response_includes_request_id(self, client, mock_principal, mock_v1_manager):
        """Test that responses include request ID in meta."""
        mock_v1_manager.get_v1_split_status.return_value = {}

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    with patch('aurum.api.features.feature_management.get_request_id', return_value="test-request-123"):
                        response = client.get("/v1/admin/features/v1-splits")

        assert response.status_code == 200
        data = response.json()
        assert data["meta"]["request_id"] == "test-request-123"

    def test_response_includes_query_time(self, client, mock_principal, mock_v1_manager):
        """Test that responses include query time in meta."""
        mock_v1_manager.get_v1_split_status.return_value = {}

        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager', return_value=mock_v1_manager):
                    response = client.get("/v1/admin/features/v1-splits")

        assert response.status_code == 200
        data = response.json()
        assert "query_time_ms" in data["meta"]
        assert isinstance(data["meta"]["query_time_ms"], (int, float))

    def test_invalid_json_body(self, client, mock_principal):
        """Test handling of invalid JSON in request body."""
        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                # Send invalid JSON (string instead of boolean)
                response = client.put("/v1/admin/features/v1-splits/eia", json="invalid")

        # FastAPI should handle JSON validation
        assert response.status_code == 422  # Unprocessable Entity


class TestV1SplitAPIIntegration:
    """Integration tests for V1 split flag API."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        return TestClient(app)

    def test_full_workflow(self, client):
        """Test complete workflow of managing V1 split flags."""
        mock_principal = {"user_id": "integration_test", "groups": ["admin"]}
        
        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager') as mock_get_manager:
                    mock_manager = AsyncMock()
                    mock_get_manager.return_value = mock_manager
                    
                    # Initial state: all flags disabled
                    mock_manager.get_v1_split_status.return_value = {
                        "eia": {"enabled": False, "feature_key": "v1_split_eia"},
                        "iso": {"enabled": False, "feature_key": "v1_split_iso"}
                    }
                    
                    # 1. Get initial status
                    response = client.get("/v1/admin/features/v1-splits")
                    assert response.status_code == 200
                    data = response.json()
                    assert data["meta"]["enabled_flags"] == 0
                    
                    # 2. Enable EIA flag
                    mock_manager.set_v1_split_enabled.return_value = True
                    mock_manager.get_env_var_for_flag.return_value = "AURUM_API_V1_SPLIT_EIA"
                    mock_manager.get_module_path_for_flag.return_value = "aurum.api.v1.eia"
                    
                    response = client.put("/v1/admin/features/v1-splits/eia", json=True)
                    assert response.status_code == 200
                    data = response.json()
                    assert data["enabled"] is True
                    
                    # 3. Verify flag was enabled
                    mock_manager.get_v1_split_status.return_value = {
                        "eia": {"enabled": True, "feature_key": "v1_split_eia"},
                        "iso": {"enabled": False, "feature_key": "v1_split_iso"}
                    }
                    
                    response = client.get("/v1/admin/features/v1-splits/eia")
                    assert response.status_code == 200
                    data = response.json()
                    assert data["data"]["enabled"] is True

    def test_error_recovery(self, client):
        """Test system recovers gracefully from errors."""
        mock_principal = {"user_id": "error_test", "groups": ["admin"]}
        
        with patch('aurum.api.features.feature_management._routes._get_principal', return_value=mock_principal):
            with patch('aurum.api.features.feature_management._routes._require_admin'):
                with patch('aurum.api.features.feature_management.get_v1_split_manager') as mock_get_manager:
                    # First call fails
                    mock_get_manager.side_effect = Exception("Temporary failure")
                    
                    response = client.get("/v1/admin/features/v1-splits")
                    assert response.status_code == 500
                    
                    # Second call succeeds
                    mock_manager = AsyncMock()
                    mock_manager.get_v1_split_status.return_value = {}
                    mock_get_manager.side_effect = None
                    mock_get_manager.return_value = mock_manager
                    
                    response = client.get("/v1/admin/features/v1-splits")
                    assert response.status_code == 200