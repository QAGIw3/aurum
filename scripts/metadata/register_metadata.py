"""Register datasets/tables in OpenMetadata."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Iterable

import requests


class MetadataError(RuntimeError):
    """Raised when metadata registration fails."""


DEFAULT_SERVER = "http://openmetadata:8585/api"
DATASET_TYPE = "dataset"
TABLE_TYPE = "table"


class OpenMetadataClient:
    def __init__(self, server: str, auth_token: str | None = None) -> None:
        self._server = server.rstrip("/")
        self._session = requests.Session()
        if auth_token:
            self._session.headers.update({"Authorization": f"Bearer {auth_token}"})
        self._session.headers.update({"Content-Type": "application/json"})

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        url = f"{self._server}/{path.lstrip('/')}"
        resp = self._session.request(method, url, json=payload, timeout=30)
        if resp.status_code >= 300:
            raise MetadataError(f"{method} {url} failed: {resp.status_code} {resp.text}")
        if resp.text:
            return resp.json()
        return {}

    def ensure_entity(self, entity_type: str, body: dict) -> dict:
        # Try to create; if already exists, we update
        try:
            return self._request("POST", f"v1/{entity_type}s", body)  # type: ignore[str-format]
        except MetadataError as exc:
            if "already exists" not in str(exc):
                raise
        # fetch existing entity id by fullyQualifiedName
        fqn = body.get("fullyQualifiedName")
        if not fqn:
            raise MetadataError("Missing fullyQualifiedName for update")
        existing = self._request("GET", f"v1/{entity_type}s/name/{fqn}")  # type: ignore[str-format]
        entity_id = existing.get("id")
        if not entity_id:
            raise MetadataError(f"Failed to resolve existing {entity_type} for {fqn}")
        body["id"] = entity_id
        return self._request("PUT", f"v1/{entity_type}s", body)  # type: ignore[str-format]


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register datasets and tables in OpenMetadata")
    parser.add_argument("--server", default=DEFAULT_SERVER, help="OpenMetadata API base URL")
    parser.add_argument("--token", help="Bearer token for authentication")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to JSON file describing datasets/tables to register",
    )
    return parser.parse_args(argv)


def load_entities(path: str) -> dict:
    raw_text = Path(path).read_text(encoding="utf-8")
    expanded = os.path.expandvars(raw_text)
    content = json.loads(expanded)
    datasets = content.get("datasets", [])
    tables = content.get("tables", [])
    return {"datasets": datasets, "tables": tables}


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    entities = load_entities(args.input)
    client = OpenMetadataClient(args.server, args.token)

    for dataset in entities["datasets"]:
        client.ensure_entity(DATASET_TYPE, dataset)
    for table in entities["tables"]:
        client.ensure_entity(TABLE_TYPE, table)
    print(
        f"Registered {len(entities['datasets'])} datasets and {len(entities['tables'])} tables in OpenMetadata"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
