#!/usr/bin/env python
"""Fetch secrets from Vault and emit environment exports."""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

from aurum.compat import requests


@dataclass(frozen=True)
class Mapping:
    path: str
    key: str
    env: str


def parse_mapping(raw: str) -> Mapping:
    try:
        path_and_key, env = raw.split("=", 1)
        path, key = path_and_key.split(":", 1)
    except ValueError as exc:  # pragma: no cover - defensive
        raise argparse.ArgumentTypeError(
            "Mapping must be in the form 'path:key=ENV_NAME'"
        ) from exc
    if not path or not key or not env:
        raise argparse.ArgumentTypeError("Mapping components cannot be empty")
    return Mapping(path=path, key=key, env=env)


def fetch_secret(vault_addr: str, token: str, path: str) -> Dict[str, object]:
    url = f"{vault_addr.rstrip('/')}/v1/{path}".replace("//v1", "/v1")
    response = requests.get(url, headers={"X-Vault-Token": token}, timeout=10)
    if response.status_code == 404:
        raise RuntimeError(f"Vault path not found: {path}")
    response.raise_for_status()
    payload = response.json()
    data = payload.get("data")
    if isinstance(data, dict) and "data" in data:
        nested = data["data"]
        if isinstance(nested, dict):
            return nested
    if not isinstance(data, dict):
        raise RuntimeError(f"Unexpected Vault payload for {path}: missing data field")
    return data


def build_exports(
    vault_addr: str,
    token: str,
    mappings: Iterable[Mapping],
    *,
    format_: str,
) -> List[str]:
    grouped: Dict[str, List[Mapping]] = {}
    for mapping in mappings:
        grouped.setdefault(mapping.path, []).append(mapping)

    lines: List[str] = []
    for path, path_mappings in grouped.items():
        secret = fetch_secret(vault_addr, token, path)
        for mapping in path_mappings:
            if mapping.key not in secret:
                raise RuntimeError(f"Key '{mapping.key}' not found at Vault path {path}")
            value = secret[mapping.key]
            if value is None:
                raise RuntimeError(f"Key '{mapping.key}' at Vault path {path} is null")
            rendered = str(value)
            if format_ == "shell":
                lines.append(f"export {mapping.env}={rendered}")
            elif format_ == "env":
                lines.append(f"{mapping.env}={rendered}")
            else:  # pragma: no cover - safeguarded by argparse
                raise RuntimeError(f"Unsupported format: {format_}")
    return lines


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export Vault secrets as shell env assignments")
    parser.add_argument(
        "--vault-addr",
        default=os.environ.get("VAULT_ADDR"),
        help="Vault address (defaults to VAULT_ADDR environment variable)",
    )
    parser.add_argument(
        "--vault-token",
        default=os.environ.get("VAULT_TOKEN"),
        help="Vault token (defaults to VAULT_TOKEN environment variable)",
    )
    parser.add_argument(
        "--mapping",
        action="append",
        type=parse_mapping,
        required=True,
        help="Mapping in the form path:key=ENV_NAME (repeatable)",
    )
    parser.add_argument(
        "--format",
        choices=["shell", "env"],
        default="shell",
        help="Output format (shell exports or env file)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.vault_addr:
        parser.error("Vault address must be provided via --vault-addr or VAULT_ADDR")
    if not args.vault_token:
        parser.error("Vault token must be provided via --vault-token or VAULT_TOKEN")

    exports = build_exports(args.vault_addr, args.vault_token, args.mapping, format_=args.format)
    for line in exports:
        print(line)
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
