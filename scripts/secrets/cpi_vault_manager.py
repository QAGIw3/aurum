#!/usr/bin/env python3
"""Vault secret management for CPI (FRED) API keys and configuration."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Import hvac for Vault operations
try:
    import hvac
except ImportError:
    print("Error: hvac library is required. Install with: pip install hvac", file=sys.stderr)
    sys.exit(1)

REPO_ROOT = Path(__file__).resolve().parents[2]
VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "")
VAULT_MOUNT = "secret"
VAULT_PATH = "aurum/cpi"

DEFAULT_CONFIG = {
    "api_key": "your-fred-api-key-here",
    "base_url": "https://api.stlouisfed.org/fred",
    "series_id": "CPIAUCSL",
    "frequency": "MONTHLY",
    "seasonal_adj": "SA",
    "area": "US",
    "units": "Index 1982-1984=100",
    "source": "FRED"
}


class CpiVaultManager:
    """Manage CPI secrets in Vault."""

    def __init__(self, vault_addr: str = VAULT_ADDR, vault_token: str = VAULT_TOKEN):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.client = hvac.Client(url=vault_addr, token=vault_token)

        if not self.client.is_authenticated():
            raise RuntimeError("Failed to authenticate with Vault. Check VAULT_ADDR and VAULT_TOKEN.")

    def set_cpi_secrets(self, secrets: dict[str, str]) -> bool:
        """Store CPI secrets in Vault."""
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                mount_point=VAULT_MOUNT,
                path=VAULT_PATH,
                secret=secrets
            )
            print(f"Successfully stored CPI secrets in Vault at {VAULT_MOUNT}/{VAULT_PATH}")
            return True
        except Exception as e:
            print(f"Failed to store CPI secrets: {e}", file=sys.stderr)
            return False

    def get_cpi_secrets(self) -> dict[str, str] | None:
        """Retrieve CPI secrets from Vault."""
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                mount_point=VAULT_MOUNT,
                path=VAULT_PATH
            )
            return response['data']['data']
        except Exception as e:
            print(f"Failed to retrieve CPI secrets: {e}", file=sys.stderr)
            return None

    def generate_env_exports(self, secrets: dict[str, str]) -> str:
        """Generate shell export statements for CPI environment variables."""
        env_exports = []
        for key, value in secrets.items():
            env_key = f"CPI_{key.upper()}"
            env_exports.append(f"export {env_key}='{value}'")

        return "\n".join(env_exports)

    def init_default_secrets(self) -> bool:
        """Initialize Vault with default CPI configuration."""
        print("Initializing Vault with default CPI configuration...")
        return self.set_cpi_secrets(DEFAULT_CONFIG)

    def validate_api_key(self, api_key: str) -> bool:
        """Validate FRED API key by making a test request."""
        import requests

        try:
            params = {
                "series_id": "CPIAUCSL",
                "api_key": api_key,
                "file_type": "json",
                "limit": 1
            }

            response = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                print("✅ CPI (FRED) API key is valid")
                return True
            elif response.status_code == 400:
                print("❌ CPI (FRED) API key is invalid (400 Bad Request)")
                return False
            else:
                print(f"⚠️ CPI (FRED) API key validation inconclusive (status: {response.status_code})")
                return True  # Assume valid for non-auth errors

        except requests.RequestException as e:
            print(f"⚠️ Could not validate CPI (FRED) API key: {e}")
            return True  # Assume valid on network errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage CPI secrets in Vault")
    parser.add_argument("--init", action="store_true", help="Initialize with default configuration")
    parser.add_argument("--set", type=str, help="Set FRED API key (e.g., --set 'your-api-key-here')")
    parser.add_argument("--get", action="store_true", help="Get current CPI secrets")
    parser.add_argument("--validate", action="store_true", help="Validate current FRED API key")
    parser.add_argument("--env", action="store_true", help="Generate shell environment exports")
    parser.add_argument("--vault-addr", default=VAULT_ADDR, help="Vault server address")
    parser.add_argument("--vault-token", default=VAULT_TOKEN, help="Vault authentication token")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()

    try:
        manager = CpiVaultManager(args.vault_addr, args.vault_token)

        if args.init:
            if args.dry_run:
                print("Would initialize Vault with default CPI configuration")
                print("Default config:", json.dumps(DEFAULT_CONFIG, indent=2))
            else:
                if manager.init_default_secrets():
                    print("✅ CPI secrets initialized in Vault")
                    return 0
                else:
                    return 1

        elif args.set:
            current_secrets = manager.get_cpi_secrets() or {}
            current_secrets["api_key"] = args.set

            if args.validate:
                if not manager.validate_api_key(args.set):
                    print("❌ API key validation failed", file=sys.stderr)
                    return 1

            if args.dry_run:
                print("Would update CPI API key in Vault")
                print("Updated secrets:", json.dumps(current_secrets, indent=2))
            else:
                if manager.set_cpi_secrets(current_secrets):
                    print("✅ CPI API key updated in Vault")
                    return 0
                else:
                    return 1

        elif args.get:
            secrets = manager.get_cpi_secrets()
            if secrets:
                print("Current CPI secrets:")
                print(json.dumps(secrets, indent=2))
                return 0
            else:
                print("No CPI secrets found in Vault", file=sys.stderr)
                return 1

        elif args.validate:
            secrets = manager.get_cpi_secrets()
            if secrets and "api_key" in secrets:
                if manager.validate_api_key(secrets["api_key"]):
                    print("✅ CPI (FRED) API key is valid")
                    return 0
                else:
                    print("❌ CPI (FRED) API key is invalid", file=sys.stderr)
                    return 1
            else:
                print("No CPI API key found to validate", file=sys.stderr)
                return 1

        elif args.env:
            secrets = manager.get_cpi_secrets()
            if secrets:
                env_exports = manager.generate_env_exports(secrets)
                print(env_exports)
                return 0
            else:
                print("No CPI secrets found", file=sys.stderr)
                return 1

        else:
            parser.print_help()
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
