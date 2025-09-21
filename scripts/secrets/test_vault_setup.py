#!/usr/bin/env python3
"""Test script to verify Vault secrets setup and generate local development environment."""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_vault_secrets_manager() -> bool:
    """Test the comprehensive Vault secrets manager."""
    try:
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from vault_secrets_manager import VaultSecretsManager, DEFAULT_CONFIGS
        print("✅ VaultSecretsManager import successful")

        # Test initialization with mock client
        with open("/dev/null", 'w') as devnull:
            import unittest.mock
            with unittest.mock.patch('vault_secrets_manager.hvac.Client') as mock_client_class:
                mock_client = unittest.mock.MagicMock()
                mock_client.is_authenticated.return_value = True
                mock_client_class.return_value = mock_client

                manager = VaultSecretsManager()
                print("✅ VaultSecretsManager initialization successful")

                # Test source configuration
                sources = manager.sources
                expected_sources = {"eia", "fred", "cpi", "noaa", "timescale", "pjm", "kafka"}
                assert set(sources.keys()) == expected_sources
                print("✅ All expected sources configured")

                # Test EIA configuration
                eia_config = sources["eia"]
                assert eia_config.env_mappings["api_key"] == "EIA_API_KEY"
                print("✅ EIA environment mappings correct")

                # Test FRED configuration
                fred_config = sources["fred"]
                assert fred_config.env_mappings["api_key"] == "FRED_API_KEY"
                print("✅ FRED environment mappings correct")

                # Test CPI configuration (uses FRED API key)
                cpi_config = sources["cpi"]
                assert cpi_config.env_mappings["api_key"] == "FRED_API_KEY"
                print("✅ CPI environment mappings correct")

                return True

    except Exception as e:
        print(f"❌ VaultSecretsManager test failed: {e}")
        return False


def test_local_env_generator() -> bool:
    """Test the local environment generator."""
    try:
        sys.path.insert(0, str(REPO_ROOT / "scripts"))
        from generate_local_env import pull_all_secrets, generate_env_file, DEFAULT_LOCAL_ENV
        print("✅ generate_local_env import successful")

        # Test with mock Vault client
        with open("/dev/null", 'w') as devnull:
            import unittest.mock
            with unittest.mock.patch('generate_local_env.hvac.Client') as mock_client_class:
                mock_client = unittest.mock.MagicMock()
                mock_client.is_authenticated.return_value = True
                mock_client.secrets.kv.v2.read_secret_version.side_effect = [
                    {'data': {'data': {'api_key': 'eia-key'}}},
                    {'data': {'data': {'api_key': 'fred-key'}}},
                    {'data': {'data': {'token': 'noaa-token'}}},
                    {'data': {'data': {'user': 'aurum', 'password': 'pass'}}},
                    {'data': {'data': {'token': 'pjm-token'}}},
                    {'data': {'data': {'bootstrap_servers': 'localhost:9092'}}}
                ]
                mock_client_class.return_value = mock_client

                # Test pulling secrets
                env_vars = pull_all_secrets("http://vault:8200", "token123")
                assert "EIA_API_KEY" in env_vars
                assert "FRED_API_KEY" in env_vars
                assert "NOAA_GHCND_TOKEN" in env_vars
                assert "TIMESCALE_USER" in env_vars
                assert "PJM_TOKEN" in env_vars
                print("✅ Secret pulling successful")

                # Test environment variable generation
                env_vars.update(DEFAULT_LOCAL_ENV)
                assert "AURUM_KAFKA_BOOTSTRAP_SERVERS" in env_vars
                assert "AURUM_TIMESCALE_JDBC_URL" in env_vars
                print("✅ Environment variable generation successful")

                return True

    except Exception as e:
        print(f"❌ generate_local_env test failed: {e}")
        return False


def test_vault_scripts_exist() -> bool:
    """Test that all Vault scripts exist."""
    script_dir = REPO_ROOT / "scripts" / "secrets"

    required_scripts = [
        "vault_secrets_manager.py",
        "generate_local_env.py",
        "pull_vault_env.py",
        "push_vault_env.py",
        "cpi_vault_manager.py",
        "fred_vault_manager.py",
        "noaa_vault_manager.py",
        "pull_cpi_env.py",
        "pull_fred_env.py",
        "pull_noaa_env.py"
    ]

    missing_scripts = []
    for script in required_scripts:
        script_path = script_dir / script
        if not script_path.exists():
            missing_scripts.append(script)

    if missing_scripts:
        print(f"❌ Missing Vault scripts: {missing_scripts}")
        return False

    print("✅ All Vault scripts exist")
    return True


def main() -> int:
    print("🧪 Testing Vault Secrets Management Setup...\n")

    tests = [
        ("Vault Scripts", test_vault_scripts_exist),
        ("Vault Secrets Manager", test_vault_secrets_manager),
        ("Local Environment Generator", test_local_env_generator),
    ]

    all_passed = True
    for test_name, test_func in tests:
        print(f"🧪 Testing {test_name}...")
        try:
            if not test_func():
                all_passed = False
        except Exception as e:
            print(f"❌ {test_name} test failed: {e}")
            all_passed = False

    if all_passed:
        print("\n✅ All Vault secrets management tests passed!")
        print("\n🚀 Next Steps:")
        print("1. Initialize Vault: python3 scripts/secrets/vault_secrets_manager.py --init")
        print("2. Set API keys: python3 scripts/secrets/vault_secrets_manager.py --set 'eia:your-eia-key'")
        print("3. Generate local env: python3 scripts/secrets/generate_local_env.py")
        print("4. Test with: python3 scripts/secrets/vault_secrets_manager.py --list")
        print("5. Run dry-run: python3 scripts/seatunnel/dry_run_renderer.py --template eia_series_to_kafka")
        return 0
    else:
        print("\n❌ Some Vault secrets management tests failed!")
        return 1


if __name__ == "__main__":
    sys.exit(main())
