"""Tests for comprehensive Vault secrets management."""

from __future__ import annotations

import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from scripts.secrets.vault_secrets_manager import (
    SourceConfig,
    VaultSecretsManager,
    DEFAULT_CONFIGS,
    VAULT_ADDR,
    VAULT_TOKEN,
)


class TestSourceConfig:
    def test_source_config_creation(self) -> None:
        """Test SourceConfig dataclass creation."""
        config = SourceConfig(
            name="eia",
            path="secret/data/aurum/eia",
            secrets={"api_key": "test-key"},
            description="EIA API credentials",
            env_mappings={"api_key": "EIA_API_KEY"}
        )

        assert config.name == "eia"
        assert config.path == "secret/data/aurum/eia"
        assert config.secrets == {"api_key": "test-key"}
        assert config.description == "EIA API credentials"
        assert config.env_mappings == {"api_key": "EIA_API_KEY"}

    def test_source_config_defaults(self) -> None:
        """Test SourceConfig with default values."""
        config = SourceConfig(
            name="test",
            path="secret/data/aurum/test",
            secrets={"key": "value"},
            description="Test source"
        )

        assert config.env_mappings == {}
        assert config.validation_func is None


class TestVaultSecretsManager:
    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_vault_secrets_manager_init(self, mock_client_class) -> None:
        """Test VaultSecretsManager initialization."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager("http://vault:8200", "token123")

        assert manager.vault_addr == "http://vault:8200"
        assert manager.vault_token == "token123"
        assert manager.client == mock_client
        mock_client_class.assert_called_once_with(url="http://vault:8200", token="token123")

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_vault_secrets_manager_auth_failure(self, mock_client_class) -> None:
        """Test VaultSecretsManager with authentication failure."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = False
        mock_client_class.return_value = mock_client

        with pytest.raises(RuntimeError, match="Failed to authenticate with Vault"):
            VaultSecretsManager("http://vault:8200", "token123")

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_build_source_configs(self, mock_client_class) -> None:
        """Test building source configurations."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        sources = manager.sources

        # Check that all expected sources are configured
        expected_sources = {"eia", "fred", "cpi", "noaa", "timescale", "pjm", "kafka"}
        assert set(sources.keys()) == expected_sources

        # Check EIA configuration
        eia_config = sources["eia"]
        assert eia_config.name == "eia"
        assert eia_config.path == "secret/data/aurum/eia"
        assert eia_config.secrets == DEFAULT_CONFIGS["eia"]
        assert eia_config.env_mappings["api_key"] == "EIA_API_KEY"

        # Check FRED configuration
        fred_config = sources["fred"]
        assert fred_config.env_mappings["api_key"] == "FRED_API_KEY"

        # Check CPI configuration (uses FRED API key)
        cpi_config = sources["cpi"]
        assert cpi_config.env_mappings["api_key"] == "FRED_API_KEY"

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_set_source_secrets(self, mock_client_class) -> None:
        """Test setting source secrets."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.create_or_update_secret.return_value = None
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        secrets = {"api_key": "test-key", "base_url": "https://api.example.com"}

        result = manager.set_source_secrets("eia", secrets)

        assert result is True
        mock_client.secrets.kv.v2.create_or_update_secret.assert_called_once_with(
            mount_point="secret",
            path="aurum/eia",
            secret=secrets
        )

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_set_source_secrets_unknown_source(self, mock_client_class) -> None:
        """Test setting secrets for unknown source."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        secrets = {"key": "value"}

        result = manager.set_source_secrets("unknown", secrets)

        assert result is False

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_get_source_secrets(self, mock_client_class) -> None:
        """Test getting source secrets."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {'data': {'api_key': 'test-key', 'base_url': 'https://api.example.com'}}
        }
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        secrets = manager.get_source_secrets("eia")

        assert secrets == {'api_key': 'test-key', 'base_url': 'https://api.example.com'}
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            mount_point="secret",
            path="aurum/eia"
        )

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_generate_env_exports(self, mock_client_class) -> None:
        """Test generating environment exports."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {'data': {'api_key': 'test-key', 'base_url': 'https://api.example.com'}}
        }
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        env_exports = manager.generate_env_exports(["eia"], format_="shell")

        expected = "export EIA_API_KEY='test-key'\nexport EIA_BASE_URL='https://api.example.com'"
        assert env_exports == expected

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_generate_env_exports_dotenv(self, mock_client_class) -> None:
        """Test generating dotenv format exports."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {'data': {'api_key': 'test-key'}}
        }
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        env_exports = manager.generate_env_exports(["eia"], format_="dotenv")

        expected = "EIA_API_KEY=test-key"
        assert env_exports == expected

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_list_sources(self, mock_client_class, capsys) -> None:
        """Test listing sources."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.side_effect = [
            {'data': {'data': {'api_key': 'eia-key'}}},
            {'data': {'data': {'api_key': 'fred-key'}}},
            {'data': {'data': {'token': 'noaa-token'}}},
        ]
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        manager.list_sources()

        captured = capsys.readouterr()
        assert "EIA" in captured.out
        assert "FRED" in captured.out
        assert "NOAA" in captured.out
        assert "TIMESCALE" in captured.out

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_show_source_details(self, mock_client_class, capsys) -> None:
        """Test showing source details."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {'data': {'api_key': 'test-key', 'base_url': 'https://api.example.com'}}
        }
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()
        manager.show_source_details("eia")

        captured = capsys.readouterr()
        assert "Source: eia" in captured.out
        assert "Vault path: secret/data/aurum/eia" in captured.out
        assert "api_key: test..." in captured.out  # Should be masked
        assert "base_url: https://api.example.com" in captured.out  # Should not be masked

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_validate_eia_key(self, mock_client_class) -> None:
        """Test EIA API key validation."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        with patch('scripts.secrets.vault_secrets_manager.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = manager._validate_eia_key("valid-key")

            assert result is True
            mock_get.assert_called_once()

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_validate_fred_key(self, mock_client_class) -> None:
        """Test FRED API key validation."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        with patch('scripts.secrets.vault_secrets_manager.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = manager._validate_fred_key("valid-key")

            assert result is True
            mock_get.assert_called_once()

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_validate_noaa_key(self, mock_client_class) -> None:
        """Test NOAA API token validation."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        with patch('scripts.secrets.vault_secrets_manager.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            result = manager._validate_noaa_key("valid-token")

            assert result is True
            mock_get.assert_called_once()


class TestVaultSecretsManagerIntegration:
    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_full_workflow(self, mock_client_class) -> None:
        """Test the complete Vault secrets management workflow."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.create_or_update_secret.return_value = None
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {'data': {'api_key': 'test-key'}}
        }
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        # Test initialization
        result = manager.set_source_secrets("eia", {"api_key": "test-key"})
        assert result is True

        # Test retrieval
        secrets = manager.get_source_secrets("eia")
        assert secrets == {"api_key": "test-key"}

        # Test environment export generation
        env_exports = manager.generate_env_exports(["eia"], format_="shell")
        assert "export EIA_API_KEY='test-key'" in env_exports

        # Test source listing
        manager.list_sources()

        # Test source details
        manager.show_source_details("eia")

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_validation_functions(self, mock_client_class) -> None:
        """Test all validation functions."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        with patch('scripts.secrets.vault_secrets_manager.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            # Test all validation functions
            assert manager._validate_eia_key("valid-key") is True
            assert manager._validate_fred_key("valid-key") is True
            assert manager._validate_noaa_key("valid-token") is True

    @patch('scripts.secrets.vault_secrets_manager.hvac.Client')
    def test_error_handling(self, mock_client_class) -> None:
        """Test error handling for various scenarios."""
        mock_client = MagicMock()
        mock_client.is_authenticated.return_value = True
        mock_client.secrets.kv.v2.read_secret_version.side_effect = Exception("Vault error")
        mock_client_class.return_value = mock_client

        manager = VaultSecretsManager()

        # Test getting non-existent secrets
        secrets = manager.get_source_secrets("nonexistent")
        assert secrets is None

        # Test getting secrets for unknown source
        result = manager.set_source_secrets("unknown", {})
        assert result is False

        # Test validation for unknown source
        result = manager.validate_source_key("unknown", "test-key")
        assert result is False
