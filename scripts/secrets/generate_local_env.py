#!/usr/bin/env python3
"""Generate comprehensive .env file for local development from Vault secrets."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List

# Import hvac for Vault operations
try:
    import hvac
except ImportError:
    print("Error: hvac library is required. Install with: pip install hvac", file=sys.stderr)
    sys.exit(1)

VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "")
VAULT_MOUNT = "secret"
VAULT_BASE_PATH = "aurum"

# Default .env configuration for local development
DEFAULT_LOCAL_ENV = {
    # Kafka Configuration
    "AURUM_KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "AURUM_SCHEMA_REGISTRY_URL": "http://localhost:8081",

    # Timescale Configuration
    "AURUM_TIMESCALE_JDBC_URL": "jdbc:postgresql://localhost:5432/aurum",
    "AURUM_TIMESCALE_USER": "aurum",
    "AURUM_TIMESCALE_PASSWORD": "aurum",

    # Airflow Configuration
    "AURUM_BIN_PATH": ".venv/bin:$PATH",
    "AURUM_PYTHONPATH_ENTRY": "/opt/airflow/src",
    "AURUM_VAULT_ADDR": "http://127.0.0.1:8200",
    "AURUM_VAULT_TOKEN": "aurum-dev-token",

    # Development Flags
    "AURUM_DEBUG": "0",
    "AURUM_EXECUTE_SEATUNNEL": "0",
}


def pull_all_secrets(vault_addr: str, vault_token: str) -> Dict[str, str]:
    """Pull secrets from all configured Vault paths."""
    try:
        client = hvac.Client(url=vault_addr, token=vault_token)

        if not client.is_authenticated():
            raise RuntimeError("Failed to authenticate with Vault")

        all_secrets = {}

        # Define all known Vault paths
        vault_paths = [
            f"{VAULT_BASE_PATH}/eia",
            f"{VAULT_BASE_PATH}/fred",
            f"{VAULT_BASE_PATH}/cpi",
            f"{VAULT_BASE_PATH}/noaa",
            f"{VAULT_BASE_PATH}/timescale",
            f"{VAULT_BASE_PATH}/pjm",
            f"{VAULT_BASE_PATH}/kafka"
        ]

        for path in vault_paths:
            try:
                full_path = f"{VAULT_MOUNT}/data/{path}"
                response = client.secrets.kv.v2.read_secret_version(
                    mount_point=VAULT_MOUNT,
                    path=path
                )
                secrets = response['data']['data']

                # Add secrets with appropriate prefixes
                for key, value in secrets.items():
                    if path == f"{VAULT_BASE_PATH}/eia":
                        all_secrets[f"EIA_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/fred":
                        all_secrets[f"FRED_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/cpi":
                        if key == "api_key":
                            all_secrets["FRED_API_KEY"] = value  # CPI uses FRED API key
                        else:
                            all_secrets[f"CPI_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/noaa":
                        all_secrets[f"NOAA_GHCND_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/timescale":
                        all_secrets[f"TIMESCALE_{key.upper()}"] = value
                        all_secrets[f"AURUM_TIMESCALE_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/pjm":
                        all_secrets[f"PJM_{key.upper()}"] = value
                    elif path == f"{VAULT_BASE_PATH}/kafka":
                        all_secrets[f"AURUM_KAFKA_BOOTSTRAP_SERVERS"] = secrets.get("bootstrap_servers", "localhost:9092")
                        all_secrets[f"AURUM_SCHEMA_REGISTRY_URL"] = secrets.get("schema_registry", "http://localhost:8081")

            except Exception as e:
                print(f"Warning: Could not read secrets from {path}: {e}", file=sys.stderr)

        return all_secrets

    except Exception as e:
        raise RuntimeError(f"Failed to pull secrets from Vault: {e}")


def generate_env_file(env_vars: Dict[str, str], filepath: str) -> None:
    """Generate .env file with all environment variables."""
    with open(filepath, 'w') as f:
        f.write("# Aurum Local Development Environment\n")
        f.write("# Generated from Vault secrets - DO NOT EDIT MANUALLY\n")
        f.write("#\n")
        f.write("# To regenerate:\n")
        f.write("#   python3 scripts/secrets/generate_local_env.py\n")
        f.write("#\n")
        f.write("# Last generated: $(date)\n")
        f.write("\n")

        # Write sections in logical order
        sections = {
            "Core Infrastructure": [
                "AURUM_KAFKA_BOOTSTRAP_SERVERS",
                "AURUM_SCHEMA_REGISTRY_URL",
                "AURUM_TIMESCALE_JDBC_URL",
                "AURUM_TIMESCALE_USER",
                "AURUM_TIMESCALE_PASSWORD"
            ],
            "Data Source APIs": [
                "EIA_API_KEY",
                "FRED_API_KEY",
                "NOAA_GHCND_TOKEN",
                "PJM_TOKEN"
            ],
            "Airflow Configuration": [
                "AURUM_BIN_PATH",
                "AURUM_PYTHONPATH_ENTRY",
                "AURUM_VAULT_ADDR",
                "AURUM_VAULT_TOKEN"
            ],
            "Development Settings": [
                "AURUM_DEBUG",
                "AURUM_EXECUTE_SEATUNNEL"
            ]
        }

        for section, vars_in_section in sections.items():
            f.write(f"# {section}\n")
            for var in vars_in_section:
                if var in env_vars:
                    f.write(f"{var}={env_vars[var]}\n")
            f.write("\n")

        # Write any remaining variables
        written_vars = set()
        for section_vars in sections.values():
            written_vars.update(section_vars)

        remaining_vars = set(env_vars.keys()) - written_vars
        if remaining_vars:
            f.write("# Additional Variables\n")
            for var in sorted(remaining_vars):
                f.write(f"{var}={env_vars[var]}\n")

    print(f"Generated .env file: {filepath}")
    print(f"Contains {len(env_vars)} environment variables")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate comprehensive .env file for local development")
    parser.add_argument("--vault-addr", default=VAULT_ADDR, help="Vault server address")
    parser.add_argument("--vault-token", default=VAULT_TOKEN, help="Vault authentication token")
    parser.add_argument("--output", type=str, default=".env.local",
                       help="Output .env file path (default: .env.local)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be generated without writing file")

    args = parser.parse_args()

    try:
        # Pull secrets from Vault
        env_vars = pull_all_secrets(args.vault_addr, args.vault_token)

        # Add default local environment configuration
        env_vars.update(DEFAULT_LOCAL_ENV)

        if args.dry_run:
            print("# Local Development Environment Variables (dry run)")
            print(f"# Would generate {len(env_vars)} variables")
            print("#")

            for key, value in sorted(env_vars.items()):
                if any(sensitive in key.lower() for sensitive in ['key', 'token', 'password']):
                    masked = value[:4] + "..." if len(value) > 4 else "***"
                    print(f"# {key}={masked}")
                else:
                    print(f"# {key}={value}")
            return 0

        # Generate .env file
        generate_env_file(env_vars, args.output)

        # Show summary
        print("📋 Environment Summary:")
📋 Environment Summary:"        print(f"  • Kafka: {env_vars.get('AURUM_KAFKA_BOOTSTRAP_SERVERS', 'Not configured')}")
        print(f"  • Schema Registry: {env_vars.get('AURUM_SCHEMA_REGISTRY_URL', 'Not configured')}")
        print(f"  • Timescale: {env_vars.get('AURUM_TIMESCALE_JDBC_URL', 'Not configured')}")
        print(f"  • EIA API: {'✅' if 'EIA_API_KEY' in env_vars else '❌'}")
        print(f"  • FRED API: {'✅' if 'FRED_API_KEY' in env_vars else '❌'}")
        print(f"  • NOAA API: {'✅' if 'NOAA_GHCND_TOKEN' in env_vars else '❌'}")
        print(f"  • PJM API: {'✅' if 'PJM_TOKEN' in env_vars else '❌'}")

        print("
🚀 Next Steps:"        print("  1. Source the environment: source .env.local")
        print("  2. Start local services: docker-compose up")
        print("  3. Test with: python3 scripts/secrets/vault_secrets_manager.py --list")
        print("  4. Run dry-run: python3 scripts/seatunnel/dry_run_renderer.py --template eia_series_to_kafka")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
