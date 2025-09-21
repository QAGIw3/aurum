#!/usr/bin/env python3
"""Pull FRED environment variables from Vault for local development."""

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

VAULT_ADDR = os.environ.get("VAULT_ADDR", "http://127.0.0.1:8200")
VAULT_TOKEN = os.environ.get("VAULT_TOKEN", "")
VAULT_MOUNT = "secret"
VAULT_PATH = "aurum/fred"

DEFAULT_ENV_MAPPING = {
    "api_key": "FRED_API_KEY",
    "base_url": "FRED_BASE_URL",
    "series_id": "FRED_SERIES_ID",
    "frequency": "FRED_FREQUENCY",
    "seasonal_adj": "FRED_SEASONAL_ADJ",
    "units": "FRED_UNITS",
    "title": "FRED_TITLE",
    "notes": "FRED_NOTES"
}


def pull_fred_env_from_vault(
    env_mapping: dict[str, str] | None = None,
    vault_addr: str = VAULT_ADDR,
    vault_token: str = VAULT_TOKEN
) -> dict[str, str]:
    """Pull FRED environment variables from Vault."""
    if env_mapping is None:
        env_mapping = DEFAULT_ENV_MAPPING

    try:
        client = hvac.Client(url=vault_addr, token=vault_token)

        if not client.is_authenticated():
            raise RuntimeError("Failed to authenticate with Vault")

        response = client.secrets.kv.v2.read_secret_version(
            mount_point=VAULT_MOUNT,
            path=VAULT_PATH
        )

        secrets = response['data']['data']

        env_vars = {}
        for vault_key, env_var in env_mapping.items():
            if vault_key in secrets:
                env_vars[env_var] = secrets[vault_key]

        return env_vars

    except Exception as e:
        raise RuntimeError(f"Failed to pull FRED secrets from Vault: {e}")


def generate_env_exports(env_vars: dict[str, str], format: str = "shell") -> str:
    """Generate environment variable export statements."""
    if format == "shell":
        exports = []
        for key, value in env_vars.items():
            # Shell-escape values that contain special characters
            escaped_value = value.replace("'", "'\"'\"'")
            exports.append(f"export {key}='{escaped_value}'")
        return "\n".join(exports)

    elif format == "json":
        return json.dumps(env_vars, indent=2)

    elif format == "dotenv":
        exports = []
        for key, value in env_vars.items():
            exports.append(f"{key}={value}")
        return "\n".join(exports)

    else:
        raise ValueError(f"Unsupported format: {format}")


def save_to_env_file(env_vars: dict[str, str], filepath: str) -> None:
    """Save environment variables to a .env file."""
    with open(filepath, 'w') as f:
        f.write("# FRED Environment Variables\n")
        f.write("# Generated from Vault - DO NOT EDIT MANUALLY\n\n")
        for key, value in env_vars.items():
            f.write(f"{key}={value}\n")
    print(f"Environment variables saved to {filepath}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Pull FRED environment variables from Vault")
    parser.add_argument("--format", choices=["shell", "json", "dotenv"], default="shell",
                       help="Output format (default: shell)")
    parser.add_argument("--env-file", type=str, help="Save to .env file")
    parser.add_argument("--vault-addr", default=VAULT_ADDR, help="Vault server address")
    parser.add_argument("--vault-token", default=VAULT_TOKEN, help="Vault authentication token")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be exported without setting")

    args = parser.parse_args()

    try:
        env_vars = pull_fred_env_from_vault(
            env_mapping=DEFAULT_ENV_MAPPING,
            vault_addr=args.vault_addr,
            vault_token=args.vault_token
        )

        if not env_vars:
            print("No FRED environment variables found in Vault", file=sys.stderr)
            return 1

        if args.dry_run:
            print("# FRED Environment Variables (dry run)")
            for key, value in env_vars.items():
                print(f"# {key}={value}")
            return 0

        output = generate_env_exports(env_vars, format=args.format)

        if args.env_file:
            save_to_env_file(env_vars, args.env_file)
        else:
            print(output)

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
