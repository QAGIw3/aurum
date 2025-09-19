#!/usr/bin/env python
"""Write environment variables into Vault KV secrets."""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List

from aurum.compat import requests


@dataclass(frozen=True)
class Mapping:
    env: str
    path: str
    key: str


def parse_mapping(raw: str) -> Mapping:
    try:
        env, path_key = raw.split("=", 1)
        path, key = path_key.split(":", 1)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Mapping must be in the form ENV_NAME=path:key"
        ) from exc
    if not env or not path or not key:
        raise argparse.ArgumentTypeError("Mapping components cannot be empty")
    return Mapping(env=env, path=path, key=key)


def kv_write(vault_addr: str, token: str, path: str, data: Dict[str, str]) -> None:
    url = f"{vault_addr.rstrip('/')}/v1/{path}".replace("//v1", "/v1")
    # Support KV v2 by posting into data wrapper when path contains /data/
    payload: Dict[str, Dict[str, str]]
    if "/data/" in path:
        payload = {"data": data}
    else:
        payload = data
    response = requests.post(url, headers={"X-Vault-Token": token}, json=payload, timeout=10)
    if response.status_code >= 400:
        raise RuntimeError(f"Vault write failed for {path}: {response.status_code} {response.text}")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Push environment variables into Vault KV")
    parser.add_argument(
        "--vault-addr",
        default=os.environ.get("VAULT_ADDR"),
        help="Vault address (defaults to VAULT_ADDR)",
    )
    parser.add_argument(
        "--vault-token",
        default=os.environ.get("VAULT_TOKEN"),
        help="Vault token (defaults to VAULT_TOKEN)",
    )
    parser.add_argument(
        "--mapping",
        action="append",
        type=parse_mapping,
        required=True,
        help="Mapping in the form ENV_NAME=path:key (repeatable)",
    )

    args = parser.parse_args(list(argv) if argv is not None else None)

    if not args.vault_addr:
        parser.error("Vault address must be provided via --vault-addr or VAULT_ADDR")
    if not args.vault_token:
        parser.error("Vault token must be provided via --vault-token or VAULT_TOKEN")

    grouped: Dict[str, Dict[str, str]] = {}
    missing: List[str] = []
    for mapping in args.mapping:
        value = os.environ.get(mapping.env)
        if value is None:
            missing.append(mapping.env)
            continue
        grouped.setdefault(mapping.path, {})[mapping.key] = value

    if missing:
        raise RuntimeError(f"Missing environment variables: {', '.join(sorted(missing))}")

    for path, data in grouped.items():
        kv_write(args.vault_addr, args.vault_token, path, data)
        print(f"Wrote {len(data)} key(s) to Vault path {path}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
