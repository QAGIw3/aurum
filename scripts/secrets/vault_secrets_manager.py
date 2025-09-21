#!/usr/bin/env python3
"""Comprehensive Vault secret management CLI for all Aurum data sources."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

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
VAULT_BASE_PATH = "aurum"

# Default configurations for all sources
DEFAULT_CONFIGS = {
    "eia": {
        "api_key": "your-eia-api-key-here",
        "base_url": "https://api.eia.gov/v2",
        "series_path": "electricity/wholesale/prices/data",
        "timeout": "30000"
    },
    "fred": {
        "api_key": "your-fred-api-key-here",
        "base_url": "https://api.stlouisfed.org/fred",
        "series_id": "DGS10",
        "frequency": "DAILY"
    },
    "cpi": {
        "api_key": "your-fred-api-key-here",  # Same as FRED
        "base_url": "https://api.stlouisfed.org/fred",
        "series_id": "CPIAUCSL",
        "frequency": "MONTHLY",
        "area": "US_CITY_AVERAGE"
    },
    "noaa": {
        "token": "your-noaa-api-token-here",
        "base_url": "https://www.ncdc.noaa.gov/cdo-web/api/v2",
        "dataset": "GHCND",
        "timeout": "30000"
    },
    "timescale": {
        "user": "aurum",
        "password": "aurum-dev-password",
        "host": "localhost",
        "port": "5432",
        "database": "aurum"
    },
    "pjm": {
        "token": "your-pjm-api-token-here",
        "endpoint": "https://api.pjm.com/api/v1",
        "timeout": "30000"
    },
    "kafka": {
        "bootstrap_servers": "localhost:9092",
        "schema_registry": "http://localhost:8081"
    }
}


@dataclass
class SourceConfig:
    """Configuration for a data source."""
    name: str
    path: str
    secrets: Dict[str, str]
    description: str
    env_mappings: Dict[str, str] = field(default_factory=dict)
    validation_func: Optional[callable] = None


class VaultSecretsManager:
    """Comprehensive Vault secret management for all Aurum sources."""

    def __init__(self, vault_addr: str = VAULT_ADDR, vault_token: str = VAULT_TOKEN):
        self.vault_addr = vault_addr
        self.vault_token = vault_token
        self.client = hvac.Client(url=vault_addr, token=vault_token)

        if not self.client.is_authenticated():
            raise RuntimeError("Failed to authenticate with Vault. Check VAULT_ADDR and VAULT_TOKEN.")

        self.sources = self._build_source_configs()

    def _build_source_configs(self) -> Dict[str, SourceConfig]:
        """Build source configurations from defaults."""
        configs = {}

        for source_name, secrets in DEFAULT_CONFIGS.items():
            path = f"{VAULT_MOUNT}/data/{VAULT_BASE_PATH}/{source_name}"
            description = f"{source_name.upper()} API credentials and configuration"

            # Define environment variable mappings
            env_mappings = {}
            for key in secrets.keys():
                env_mappings[key] = f"{source_name.upper()}_{key.upper()}"
            if source_name == "cpi":
                env_mappings["api_key"] = "FRED_API_KEY"  # CPI uses FRED API key
            elif source_name == "fred":
                env_mappings["api_key"] = "FRED_API_KEY"

            # Add validation functions where applicable
            validation_func = None
            if source_name in ["eia", "fred", "cpi", "noaa"]:
                validation_func = getattr(self, f"_validate_{source_name}_key", None)

            configs[source_name] = SourceConfig(
                name=source_name,
                path=path,
                secrets=secrets,
                description=description,
                env_mappings=env_mappings,
                validation_func=validation_func
            )

        return configs

    def set_source_secrets(self, source_name: str, secrets: Dict[str, str]) -> bool:
        """Store secrets for a specific source in Vault."""
        if source_name not in self.sources:
            print(f"Unknown source: {source_name}", file=sys.stderr)
            return False

        source = self.sources[source_name]
        try:
            # Ensure path exists for KV v2
            kv_path = source.path.replace(f"{VAULT_MOUNT}/data/", "")
            self.client.secrets.kv.v2.create_or_update_secret(
                mount_point=VAULT_MOUNT,
                path=kv_path,
                secret=secrets
            )
            print(f"Successfully stored {source_name} secrets in Vault at {source.path}")
            return True
        except Exception as e:
            print(f"Failed to store {source_name} secrets: {e}", file=sys.stderr)
            return False

    def get_source_secrets(self, source_name: str) -> Optional[Dict[str, str]]:
        """Retrieve secrets for a specific source from Vault."""
        if source_name not in self.sources:
            print(f"Unknown source: {source_name}", file=sys.stderr)
            return None

        source = self.sources[source_name]
        try:
            kv_path = source.path.replace(f"{VAULT_MOUNT}/data/", "")
            response = self.client.secrets.kv.v2.read_secret_version(
                mount_point=VAULT_MOUNT,
                path=kv_path
            )
            return response['data']['data']
        except Exception as e:
            print(f"Failed to retrieve {source_name} secrets: {e}", file=sys.stderr)
            return None

    def generate_env_exports(self, sources: List[str], format_: str = "shell") -> str:
        """Generate shell export statements for specified sources."""
        env_exports = []

        for source_name in sources:
            if source_name not in self.sources:
                print(f"Warning: Unknown source {source_name}", file=sys.stderr)
                continue

            secrets = self.get_source_secrets(source_name)
            if not secrets:
                print(f"Warning: No secrets found for {source_name}", file=sys.stderr)
                continue

            source = self.sources[source_name]
            for vault_key, env_var in source.env_mappings.items():
                if vault_key in secrets:
                    value = secrets[vault_key]
                    if format_ == "shell":
                        # Shell-escape values that contain special characters
                        escaped_value = value.replace("'", "'\"'\"'")
                        env_exports.append(f"export {env_var}='{escaped_value}'")
                    elif format_ == "dotenv":
                        env_exports.append(f"{env_var}={value}")

        return "\n".join(env_exports)

    def init_all_defaults(self) -> bool:
        """Initialize Vault with default configurations for all sources."""
        success_count = 0
        for source_name, source in self.sources.items():
            if self.set_source_secrets(source_name, source.secrets):
                success_count += 1
            else:
                print(f"Failed to initialize {source_name}", file=sys.stderr)

        if success_count == len(self.sources):
            print(f"✅ Initialized {success_count} sources in Vault")
            return True
        else:
            print(f"⚠️ Initialized {success_count}/{len(self.sources)} sources", file=sys.stderr)
            return False

    def validate_source_key(self, source_name: str, api_key: str) -> bool:
        """Validate API key for a specific source."""
        if source_name not in self.sources:
            print(f"Unknown source: {source_name}", file=sys.stderr)
            return False

        source = self.sources[source_name]
        if source.validation_func:
            return source.validation_func(api_key)
        else:
            print(f"No validation available for {source_name}")
            return True  # Assume valid if no validation function

    # Validation functions for different sources
    def _validate_eia_key(self, api_key: str) -> bool:
        """Validate EIA API key."""
        import requests
        try:
            response = requests.get(
                "https://api.eia.gov/v2/electricity/wholesale/prices/data",
                params={"api_key": api_key, "length": "1"},
                timeout=10
            )
            if response.status_code == 200:
                print("✅ EIA API key is valid")
                return True
            elif response.status_code == 400:
                print("❌ EIA API key is invalid")
                return False
            else:
                print(f"⚠️ EIA API key validation inconclusive (status: {response.status_code})")
                return True
        except requests.RequestException as e:
            print(f"⚠️ Could not validate EIA API key: {e}")
            return True

    def _validate_fred_key(self, api_key: str) -> bool:
        """Validate FRED API key."""
        import requests
        try:
            response = requests.get(
                "https://api.stlouisfed.org/fred/series/observations",
                params={"series_id": "DGS10", "api_key": api_key, "limit": "1"},
                timeout=10
            )
            if response.status_code == 200:
                print("✅ FRED API key is valid")
                return True
            elif response.status_code == 400:
                print("❌ FRED API key is invalid")
                return False
            else:
                print(f"⚠️ FRED API key validation inconclusive (status: {response.status_code})")
                return True
        except requests.RequestException as e:
            print(f"⚠️ Could not validate FRED API key: {e}")
            return True

    def _validate_noaa_key(self, token: str) -> bool:
        """Validate NOAA API token."""
        import requests
        try:
            headers = {"token": token}
            params = {"datasetid": "GHCND", "startdate": "2024-01-01", "enddate": "2024-01-01", "limit": "1"}
            response = requests.get(
                "https://www.ncdc.noaa.gov/cdo-web/api/v2/data",
                headers=headers,
                params=params,
                timeout=10
            )
            if response.status_code == 200:
                print("✅ NOAA API token is valid")
                return True
            elif response.status_code == 401:
                print("❌ NOAA API token is invalid")
                return False
            else:
                print(f"⚠️ NOAA API token validation inconclusive (status: {response.status_code})")
                return True
        except requests.RequestException as e:
            print(f"⚠️ Could not validate NOAA API token: {e}")
            return True

    def list_sources(self) -> None:
        """List all configured sources."""
        print("Available sources:")
        for source_name, source in self.sources.items():
            secrets = self.get_source_secrets(source_name)
            status = "✅" if secrets else "❌"
            print(f"  {status} {source_name:15} - {source.description}")

    def show_source_details(self, source_name: str) -> None:
        """Show detailed information for a specific source."""
        if source_name not in self.sources:
            print(f"Unknown source: {source_name}", file=sys.stderr)
            return

        source = self.sources[source_name]
        secrets = self.get_source_secrets(source_name)

        print(f"Source: {source_name}")
        print(f"Description: {source.description}")
        print(f"Vault path: {source.path}")
        print(f"Environment mappings: {source.env_mappings}")

        if secrets:
            print("Secrets:")
            for key, value in secrets.items():
                # Mask sensitive values
                if any(sensitive in key.lower() for sensitive in ['key', 'token', 'password']):
                    masked_value = value[:4] + "..." if len(value) > 4 else "***"
                    print(f"  {key}: {masked_value}")
                else:
                    print(f"  {key}: {value}")
        else:
            print("No secrets stored")


def main() -> int:
    parser = argparse.ArgumentParser(description="Comprehensive Vault secret management for all Aurum sources")
    parser.add_argument("--init", action="store_true", help="Initialize all sources with defaults")
    parser.add_argument("--init-source", type=str, help="Initialize specific source with defaults")
    parser.add_argument("--set", type=str, help="Set API key/token for source (e.g., 'eia:your-key')")
    parser.add_argument("--get", action="store_true", help="Get current secrets for all sources")
    parser.add_argument("--get-source", type=str, help="Get secrets for specific source")
    parser.add_argument("--validate", type=str, help="Validate API key/token for source")
    parser.add_argument("--env", action="store_true", help="Generate shell environment exports for all sources")
    parser.add_argument("--env-sources", type=str, help="Comma-separated list of sources for env exports")
    parser.add_argument("--list", action="store_true", help="List all configured sources")
    parser.add_argument("--details", type=str, help="Show details for specific source")
    parser.add_argument("--vault-addr", default=VAULT_ADDR, help="Vault server address")
    parser.add_argument("--vault-token", default=VAULT_TOKEN, help="Vault authentication token")
    parser.add_argument("--format", choices=["shell", "dotenv"], default="shell",
                       help="Output format for env exports")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()

    try:
        manager = VaultSecretsManager(args.vault_addr, args.vault_token)

        if args.init:
            if args.dry_run:
                print("Would initialize all sources with default configurations")
                for source_name, source in manager.sources.items():
                    print(f"  {source_name}: {json.dumps(source.secrets, indent=2)}")
            else:
                if manager.init_all_defaults():
                    print("✅ All sources initialized in Vault")
                    return 0
                else:
                    return 1

        elif args.init_source:
            source_name = args.init_source.lower()
            if source_name not in manager.sources:
                print(f"Unknown source: {source_name}", file=sys.stderr)
                return 1

            if args.dry_run:
                print(f"Would initialize {source_name} with defaults")
                print(json.dumps(manager.sources[source_name].secrets, indent=2))
            else:
                if manager.set_source_secrets(source_name, manager.sources[source_name].secrets):
                    print(f"✅ {source_name} initialized in Vault")
                    return 0
                else:
                    return 1

        elif args.set:
            try:
                source_name, api_value = args.set.split(":", 1)
                source_name = source_name.lower()
            except ValueError:
                print("Error: --set must be in format 'source:value'", file=sys.stderr)
                return 1

            if source_name not in manager.sources:
                print(f"Unknown source: {source_name}", file=sys.stderr)
                return 1

            current_secrets = manager.get_source_secrets(source_name) or {}
            source = manager.sources[source_name]

            # Determine which key to update based on source
            if source_name == "eia":
                current_secrets["api_key"] = api_value
            elif source_name in ["fred", "cpi"]:
                current_secrets["api_key"] = api_value
            elif source_name == "noaa":
                current_secrets["token"] = api_value
            elif source_name == "pjm":
                current_secrets["token"] = api_value

            if args.validate and source.validation_func:
                if not manager.validate_source_key(source_name, api_value):
                    print("❌ Validation failed", file=sys.stderr)
                    return 1

            if args.dry_run:
                print(f"Would update {source_name} secret in Vault")
                print("Updated secrets:", json.dumps(current_secrets, indent=2))
            else:
                if manager.set_source_secrets(source_name, current_secrets):
                    print(f"✅ {source_name} secret updated in Vault")
                    return 0
                else:
                    return 1

        elif args.get:
            for source_name, source in manager.sources.items():
                secrets = manager.get_source_secrets(source_name)
                if secrets:
                    print(f"\n{source_name.upper()} secrets:")
                    for key, value in secrets.items():
                        if any(sensitive in key.lower() for sensitive in ['key', 'token', 'password']):
                            masked = value[:4] + "..." if len(value) > 4 else "***"
                            print(f"  {key}: {masked}")
                        else:
                            print(f"  {key}: {value}")
                else:
                    print(f"\n{source_name.upper()}: No secrets found")

        elif args.get_source:
            manager.show_source_details(args.get_source)

        elif args.validate:
            source_name = args.validate.lower()
            if source_name not in manager.sources:
                print(f"Unknown source: {source_name}", file=sys.stderr)
                return 1

            secrets = manager.get_source_secrets(source_name)
            if secrets:
                source = manager.sources[source_name]
                api_key = None
                if source_name == "eia" and "api_key" in secrets:
                    api_key = secrets["api_key"]
                elif source_name in ["fred", "cpi"] and "api_key" in secrets:
                    api_key = secrets["api_key"]
                elif source_name == "noaa" and "token" in secrets:
                    api_key = secrets["token"]
                elif source_name == "pjm" and "token" in secrets:
                    api_key = secrets["token"]

                if api_key and source.validation_func:
                    if manager.validate_source_key(source_name, api_key):
                        print(f"✅ {source_name} API credentials are valid")
                        return 0
                    else:
                        print(f"❌ {source_name} API credentials are invalid", file=sys.stderr)
                        return 1
                else:
                    print(f"No API credentials found for {source_name}", file=sys.stderr)
                    return 1
            else:
                print(f"No secrets found for {source_name}", file=sys.stderr)
                return 1

        elif args.env:
            sources = list(manager.sources.keys()) if not args.env_sources else args.env_sources.split(",")
            env_exports = manager.generate_env_exports(sources, format_=args.format)
            if env_exports:
                print(env_exports)
                return 0
            else:
                print("No environment variables generated", file=sys.stderr)
                return 1

        elif args.list:
            manager.list_sources()

        elif args.details:
            manager.show_source_details(args.details)

        else:
            parser.print_help()
            return 1

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
