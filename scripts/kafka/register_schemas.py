#!/usr/bin/env python
"""Register Avro schemas in the Kafka Schema Registry."""
from __future__ import annotations

import argparse
import json as json_module
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from typing import TYPE_CHECKING

try:  # pragma: no cover - exercised via CLI runtime
    import requests
except ImportError:  # pragma: no cover - handled by create_session
    requests = None  # type: ignore[assignment]

if TYPE_CHECKING:  # pragma: no cover
    import requests as _requests


DEFAULT_SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "kafka" / "schemas"
DEFAULT_SUBJECT_FILE = DEFAULT_SCHEMA_ROOT / "subjects.json"


class SchemaRegistryError(RuntimeError):
    """Raised when a schema registry operation fails."""


@dataclass
class _Response:
    status_code: int
    text: str
    _json: dict | None = None

    def json(self) -> dict:
        if self._json is None:
            raise SchemaRegistryError("Response did not include JSON body")
        return self._json


class _UrllibSession:
    """Minimal drop-in replacement for requests.Session used in tests/CLI."""

    def __init__(self, timeout: int = 10) -> None:
        self._default_timeout = timeout

    def _request(self, method: str, url: str, json_data: dict, timeout: int) -> _Response:
        data = json_data and json_module.dumps(json_data).encode("utf-8")
        request = Request(url, data=data, method=method)
        request.add_header("Content-Type", "application/json")
        try:
            with urlopen(request, timeout=timeout or self._default_timeout) as resp:
                body = resp.read()
                text = body.decode("utf-8")
                try:
                    parsed = json_module.loads(text) if text else None
                except json_module.JSONDecodeError:
                    parsed = None
                return _Response(status_code=resp.status, text=text, _json=parsed)
        except HTTPError as exc:
            text = exc.read().decode("utf-8", errors="replace")
            return _Response(status_code=exc.code, text=text)
        except URLError as exc:
            raise SchemaRegistryError(f"Failed to reach Schema Registry: {exc}") from exc

    def put(self, url: str, json: dict, timeout: int) -> _Response:  # type: ignore[override]
        return self._request("PUT", url, json, timeout)

    def post(self, url: str, json: dict, timeout: int) -> _Response:  # type: ignore[override]
        return self._request("POST", url, json, timeout)


def load_subject_mapping(path: Path) -> dict[str, str]:
    try:
        mapping = json_module.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:  # pragma: no cover - handled by CLI
        raise SchemaRegistryError(f"Subject mapping file not found: {path}") from exc
    except json_module.JSONDecodeError as exc:  # pragma: no cover - handled by CLI
        raise SchemaRegistryError(f"Invalid JSON in subject mapping: {path}") from exc

    if not isinstance(mapping, dict):
        raise SchemaRegistryError("Subject mapping must be a JSON object of subject -> schema")
    return {str(subject): str(schema) for subject, schema in mapping.items()}


def load_schema(schema_root: Path, relative_path: str) -> dict:
    schema_path = schema_root / relative_path
    try:
        return json_module.loads(schema_path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SchemaRegistryError(f"Schema file not found: {schema_path}") from exc
    except json_module.JSONDecodeError as exc:
        raise SchemaRegistryError(f"Invalid JSON in schema file: {schema_path}") from exc


def _timeout(env_var: str, default_seconds: int) -> int:
    try:
        return int(os.getenv(env_var, str(default_seconds)))
    except Exception:
        return default_seconds


def put_compatibility(registry_url: str, subject: str, level: str, *, session: requests.Session) -> None:
    endpoint = f"{registry_url.rstrip('/')}/config/{subject}"
    response = session.put(
        endpoint,
        json={"compatibility": level},
        timeout=_timeout("SCHEMA_REGISTRY_PUT_TIMEOUT", 30),
    )
    if response.status_code not in {200, 204}:
        raise SchemaRegistryError(
            f"Failed to set compatibility for {subject}: {response.status_code} {response.text}"
        )


def register_schema(registry_url: str, subject: str, schema: dict, *, session: requests.Session) -> int:
    endpoint = f"{registry_url.rstrip('/')}/subjects/{subject}/versions"
    payload = {"schema": json_module.dumps(schema), "schemaType": "AVRO"}
    response = session.post(
        endpoint,
        json=payload,
        timeout=_timeout("SCHEMA_REGISTRY_POST_TIMEOUT", 45),
    )
    if response.status_code not in {200, 201}:
        raise SchemaRegistryError(
            f"Failed to register schema for {subject}: {response.status_code} {response.text}"
        )
    data = response.json()
    return int(data.get("id") or data.get("version", -1))


def iter_subjects(selected: Iterable[str] | None, available: dict[str, str]) -> dict[str, str]:
    if not selected:
        return available
    missing = sorted(set(selected) - available.keys())
    if missing:
        raise SchemaRegistryError(f"Unknown subjects requested: {', '.join(missing)}")
    return {subject: available[subject] for subject in selected}


def create_session() -> "_requests.Session | _UrllibSession":
    if requests is not None:  # pragma: no branch
        return requests.Session()
    return _UrllibSession()


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register Avro schemas for Aurum topics")
    parser.add_argument(
        "--schema-registry-url",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        help="Schema Registry base URL (default: env SCHEMA_REGISTRY_URL or http://localhost:8081)",
    )
    parser.add_argument(
        "--subjects-file",
        type=Path,
        default=DEFAULT_SUBJECT_FILE,
        help=f"Path to JSON map of subject -> schema (default: {DEFAULT_SUBJECT_FILE})",
    )
    parser.add_argument(
        "--schema-root",
        type=Path,
        default=DEFAULT_SCHEMA_ROOT,
        help=f"Directory containing .avsc files (default: {DEFAULT_SCHEMA_ROOT})",
    )
    parser.add_argument(
        "--compatibility",
        default="BACKWARD",
        help="Compatibility level to enforce per subject (default: BACKWARD)",
    )
    parser.add_argument(
        "--subject",
        action="append",
        help="Register only the specified subject(s); can be provided multiple times",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without calling the Schema Registry",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    subjects = load_subject_mapping(args.subjects_file)
    selected_subjects = iter_subjects(args.subject, subjects)

    session = create_session()

    for subject, schema_file in selected_subjects.items():
        schema = load_schema(args.schema_root, schema_file)
        if args.dry_run:
            print(f"[dry-run] would set compatibility {args.compatibility} for {subject}")
            print(f"[dry-run] would register {schema_file} to {subject}")
            continue

        put_compatibility(args.schema_registry_url, subject, args.compatibility, session=session)
        version = register_schema(args.schema_registry_url, subject, schema, session=session)
        print(f"Registered {subject} (schema: {schema_file}) -> version/id {version}")

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    try:
        raise SystemExit(main())
    except SchemaRegistryError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
