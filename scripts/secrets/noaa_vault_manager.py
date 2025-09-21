#!/usr/bin/env python3
"""Vault secret management for NOAA GHCND tokens and configuration."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional

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
VAULT_PATH = "aurum/noaa"

DEFAULT_CONFIG = {
    "token": "your-noaa-api-token-here",
    "base_url": "https://www.ncdc.noaa.gov/cdo-web/api/v2",
    "dataset": "GHCND",
    "limit": 1000,
    "timeout": 30000,
    "station_limit": 1000,
    "unit_code": "unknown",
    "units": "metric"
}


class NOAAVaultManager:
    """Manage NOAA secrets in Vault."""

    def __init__(self, vault_addr: str = VAULT_ADDR, vault_token: str = VAULT_TOKEN):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.client = hvac.Client(url=vault_addr, token=vault_token)

        if not self.client.is_authenticated():
            raise RuntimeError("Failed to authenticate with Vault. Check VAULT_ADDR and VAULT_TOKEN.")

    def set_noaa_secrets(self, secrets: Dict[str, str]) -> bool:
        """Store NOAA secrets in Vault."""
        try:
            self.client.secrets.kv.v2.create_or_update_secret(
                mount_point=VAULT_MOUNT,
                path=VAULT_PATH,
                secret=secrets
            )
            print(f"Successfully stored NOAA secrets in Vault at {VAULT_MOUNT}/{VAULT_PATH}")
            return True
        except Exception as e:
            print(f"Failed to store NOAA secrets: {e}", file=sys.stderr)
            return False

    def get_noaa_secrets(self) -> Optional[Dict[str, str]]:
        """Retrieve NOAA secrets from Vault."""
        try:
            response = self.client.secrets.kv.v2.read_secret_version(
                mount_point=VAULT_MOUNT,
                path=VAULT_PATH
            )
            return response['data']['data']
        except Exception as e:
            print(f"Failed to retrieve NOAA secrets: {e}", file=sys.stderr)
            return None

    def generate_env_exports(self, secrets: Dict[str, str]) -> str:
        """Generate shell export statements for NOAA environment variables."""
        env_exports = []
        for key, value in secrets.items():
            env_key = f"NOAA_GHCND_{key.upper()}"
            env_exports.append(f"export {env_key}='{value}'")

        return "\n".join(env_exports)

    def init_default_secrets(self) -> bool:
        """Initialize Vault with default NOAA configuration."""
        print("Initializing Vault with default NOAA configuration...")
        return self.set_noaa_secrets(DEFAULT_CONFIG)

    def validate_token(self, token: str) -> bool:
        """Validate NOAA API token by making a test request."""
        import requests

        try:
            headers = {"token": token}
            params = {
                "datasetid": "GHCND",
                "startdate": "2024-01-01",
                "enddate": "2024-01-01",
                "limit": 1
            }

            response = requests.get(
                f"{DEFAULT_CONFIG['base_url']}/data",
                headers=headers,
                params=params,
                timeout=30
            )

            if response.status_code == 200:
                print("✅ NOAA API token is valid")
                return True
            elif response.status_code == 401:
                print("❌ NOAA API token is invalid (401 Unauthorized)")
                return False
            else:
                print(f"⚠️ NOAA API token validation inconclusive (status: {response.status_code})")
                return True  # Assume valid for non-auth errors

        except requests.RequestException as e:
            print(f"⚠️ Could not validate NOAA API token: {e}")
            return True  # Assume valid on network errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage NOAA secrets in Vault")
    parser.add_argument("--init", action="store_true", help="Initialize with default configuration")
    parser.add_argument("--set", type=str, help="Set NOAA token (e.g., --set 'your-token-here')")
    parser.add_argument("--get", action="store_true", help="Get current NOAA secrets")
    parser.add_argument("--validate", action="store_true", help="Validate current NOAA token")
    parser.add_argument("--env", action="store_true", help="Generate shell environment exports")
    parser.add_argument("--vault-addr", default=VAULT_ADDR, help="Vault server address")
    parser.add_argument("--vault-token", default=VAULT_TOKEN, help="Vault authentication token")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()

    try:
        manager = NOAAVaultManager(args.vault_addr, args.vault_token)

        if args.init:
            if args.dry_run:
                print("Would initialize Vault with default NOAA configuration")
                print("Default config:", json.dumps(DEFAULT_CONFIG, indent=2))
            else:
                if manager.init_default_secrets():
                    print("✅ NOAA secrets initialized in Vault")
                    return 0
                else:
                    return 1

        elif args.set:
            current_secrets = manager.get_noaa_secrets() or {}
            current_secrets["token"] = args.set

            if args.validate:
                if not manager.validate_token(args.set):
                    print("❌ Token validation failed", file=sys.stderr)
                    return 1

            if args.dry_run:
                print("Would update NOAA token in Vault")
                print("Updated secrets:", json.dumps(current_secrets, indent=2))
            else:
                if manager.set_noaa_secrets(current_secrets):
                    print("✅ NOAA token updated in Vault")
                    return 0
                else:
                    return 1

        elif args.get:
            secrets = manager.get_noaa_secrets()
            if secrets:
                print("Current NOAA secrets:")
                print(json.dumps(secrets, indent=2))
                return 0
            else:
                print("No NOAA secrets found in Vault", file=sys.stderr)
                return 1

        elif args.validate:
            secrets = manager.get_noaa_secrets()
            if secrets and "token" in secrets:
                if manager.validate_token(secrets["token"]):
                    print("✅ NOAA token is valid")
                    return 0
                else:
                    print("❌ NOAA token is invalid", file=sys.stderr)
                    return 1
            else:
                print("No NOAA token found to validate", file=sys.stderr)
                return 1

        elif args.env:
            secrets = manager.get_noaa_secrets()
            if secrets:
                env_exports = manager.generate_env_exports(secrets)
                print(env_exports)
                return 0
            else:
                print("No NOAA secrets found", file=sys.stderr)
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
