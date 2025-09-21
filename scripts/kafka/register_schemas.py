#!/usr/bin/env python
"""Register Avro schemas in the Kafka Schema Registry."""
from __future__ import annotations

import argparse
import json as json_module
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence
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
DEFAULT_EIA_CONFIG = Path(__file__).resolve().parents[2] / "config" / "eia_ingest_datasets.json"

SUBJECT_NAME_RE = re.compile(r"^aurum\.[a-z0-9_.]+\.v\d+(?:-(?:key|value))?$")
ALLOWED_COMPATIBILITY_LEVELS = {
    "NONE",
    "BACKWARD",
    "BACKWARD_TRANSITIVE",
    "FORWARD",
    "FORWARD_TRANSITIVE",
    "FULL",
    "FULL_TRANSITIVE",
}


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


@dataclass(frozen=True)
class SchemaContract:
    """Schema + subject contract enforced before registry interactions."""

    name: str
    schema_pattern: re.Pattern[str]
    subject_patterns: tuple[re.Pattern[str], ...]
    required_fields: frozenset[str]
    expected_compatibility: str | None = None

    def matches_schema(self, schema_name: str) -> bool:
        return bool(self.schema_pattern.fullmatch(schema_name))

    def matches_subject(self, subject: str) -> bool:
        return any(pattern.fullmatch(subject) for pattern in self.subject_patterns)


@dataclass(frozen=True)
class ValidatedSubject:
    """Result of validating a subject against its schema contract."""

    subject: str
    schema_path: Path
    schema: dict
    contract: SchemaContract


SCHEMA_CONTRACTS: tuple[SchemaContract, ...] = (
    SchemaContract(
        name="Curve Observation",
        schema_pattern=re.compile(r"^curve\.observation\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.curve\.observation\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"asof_date", "curve_key", "tenor_label", "version_hash", "_ingest_ts"}),
    ),
    SchemaContract(
        name="Drought Payloads",
        schema_pattern=re.compile(r"^aurum\.drought\.[a-z0-9_]+\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.drought\.[a-z0-9_.]+\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"tenant_id", "schema_version", "ingest_ts"}),
    ),
    SchemaContract(
        name="CPI Series",
        schema_pattern=re.compile(r"^cpi\.series\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.cpi\.series\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"series_id", "period", "value", "ingest_ts"}),
    ),
    SchemaContract(
        name="EIA Series",
        schema_pattern=re.compile(r"^eia\.series\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.eia\.[a-z0-9_.]+\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"series_id", "period", "value", "ingest_ts"}),
    ),
    SchemaContract(
        name="FRED Series",
        schema_pattern=re.compile(r"^fred\.series\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.fred\.series\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"series_id", "date", "value", "ingest_ts"}),
    ),
    SchemaContract(
        name="Fuel Curves",
        schema_pattern=re.compile(r"^fuel\.curve\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.fuel\.[a-z0-9_.]+\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"series_id", "fuel_type", "period", "value", "ingest_ts"}),
    ),
    SchemaContract(
        name="FX Rates",
        schema_pattern=re.compile(r"^fx\.rate\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.fx\.rate\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"base_currency", "quote_currency", "rate", "ingest_ts"}),
    ),
    SchemaContract(
        name="Ingest Error",
        schema_pattern=re.compile(r"^ingest\.error\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ingest\.error\.v\d+(?:-(?:key|value))?$"),
            re.compile(r"^aurum\.ref\.eia\.series\.dlq\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"source", "error_message", "severity", "ingest_ts"}),
    ),
    SchemaContract(
        name="ISO Metrics",
        schema_pattern=re.compile(r"^iso\.[a-z0-9_]+\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.iso\.[a-z0-9_.]+\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"iso_code", "ingest_ts"}),
    ),
    SchemaContract(
        name="NOAA Weather",
        schema_pattern=re.compile(r"^noaa\.weather\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.ref\.noaa\.weather\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"station_id", "date", "element", "ingest_ts"}),
    ),
    SchemaContract(
        name="QA Result",
        schema_pattern=re.compile(r"^qa\.result\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.qa\.result\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"run_id", "check_name", "status", "created_ts"}),
    ),
    SchemaContract(
        name="Scenario Output",
        schema_pattern=re.compile(r"^scenario\.output\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.scenario\.output\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"scenario_id", "tenant_id", "run_id", "metric", "computed_ts"}),
    ),
    SchemaContract(
        name="Scenario Request",
        schema_pattern=re.compile(r"^scenario\.request\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.scenario\.request\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"scenario_id", "tenant_id", "submitted_ts"}),
    ),
    SchemaContract(
        name="Alert",
        schema_pattern=re.compile(r"^alert\.v\d+$"),
        subject_patterns=(
            re.compile(r"^aurum\.alert\.v\d+(?:-(?:key|value))?$"),
        ),
        required_fields=frozenset({"alert_id", "tenant_id", "created_ts"}),
    ),
)


__all__ = [
    "SchemaRegistryError",
    "SchemaContract",
    "ValidatedSubject",
    "SCHEMA_CONTRACTS",
    "normalize_compatibility",
    "validate_subject_name",
    "find_contract_for_schema",
    "enforce_contract",
    "validate_contracts",
    "load_subject_mapping",
    "load_schema",
    "load_eia_subjects",
    "iter_subjects",
]


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


def load_eia_subjects(config_path: Path) -> dict[str, str]:
    if not config_path.exists():
        raise SchemaRegistryError(f"EIA config not found at {config_path}")
    raw = json_module.loads(config_path.read_text(encoding="utf-8"))
    datasets = raw.get("datasets", [])
    if not isinstance(datasets, list):
        raise SchemaRegistryError("EIA config must contain a 'datasets' array")
    subjects: dict[str, str] = {}
    for entry in datasets:
        if not isinstance(entry, dict):
            continue
        topic = entry.get("default_topic")
        if not topic:
            continue
        subject = f"{topic}-value"
        subjects[subject] = "eia.series.v1.avsc"
    return subjects


def normalize_compatibility(level: str) -> str:
    normalized = (level or "").strip().upper()
    if normalized not in ALLOWED_COMPATIBILITY_LEVELS:
        raise SchemaRegistryError(
            f"Unsupported compatibility level '{level}'. Allowed values: {sorted(ALLOWED_COMPATIBILITY_LEVELS)}"
        )
    return normalized


def validate_subject_name(subject: str) -> None:
    if not SUBJECT_NAME_RE.fullmatch(subject):
        raise SchemaRegistryError(
            f"Subject '{subject}' does not match required pattern {SUBJECT_NAME_RE.pattern}"
        )


def _schema_name_from_path(schema_path: Path) -> str:
    return schema_path.stem


def find_contract_for_schema(schema_path: Path) -> SchemaContract:
    schema_name = _schema_name_from_path(schema_path)
    for contract in SCHEMA_CONTRACTS:
        if contract.matches_schema(schema_name):
            return contract
    raise SchemaRegistryError(f"No schema contract declared for '{schema_name}'")


def _collect_field_names(schema: dict) -> frozenset[str]:
    if schema.get("type") != "record":
        raise SchemaRegistryError("Schemas must define a top-level Avro record")
    fields = schema.get("fields")
    if not isinstance(fields, list):
        raise SchemaRegistryError("Schema is missing a 'fields' array")
    names: set[str] = set()
    for field in fields:
        name = field.get("name") if isinstance(field, dict) else None
        if not isinstance(name, str):
            raise SchemaRegistryError("Encountered field without a valid name")
        names.add(name)
    return frozenset(names)


def enforce_contract(
    subject: str,
    schema_path: Path,
    schema: dict,
    contract: SchemaContract,
    compatibility: str,
) -> None:
    validate_subject_name(subject)
    if not contract.matches_schema(_schema_name_from_path(schema_path)):
        raise SchemaRegistryError(
            f"Schema '{schema_path.stem}' does not satisfy contract '{contract.name}'"
        )
    if not contract.matches_subject(subject):
        expected = " | ".join(pattern.pattern for pattern in contract.subject_patterns)
        raise SchemaRegistryError(
            f"Subject '{subject}' does not match expected pattern(s) {expected} for contract '{contract.name}'"
        )
    missing = contract.required_fields - _collect_field_names(schema)
    if missing:
        raise SchemaRegistryError(
            f"Schema '{schema_path.name}' is missing required field(s) {sorted(missing)} for contract '{contract.name}'"
        )
    if contract.expected_compatibility and contract.expected_compatibility != compatibility:
        raise SchemaRegistryError(
            f"Contract '{contract.name}' expects compatibility '{contract.expected_compatibility}' but received '{compatibility}'"
        )


def validate_contracts(
    subjects: Mapping[str, str],
    schema_root: Path,
    compatibility: str,
) -> dict[str, ValidatedSubject]:
    errors: list[str] = []
    validated: dict[str, ValidatedSubject] = {}

    for subject, schema_file in sorted(subjects.items()):
        try:
            validate_subject_name(subject)
        except SchemaRegistryError as exc:
            errors.append(f"{subject}: {exc}")
            continue

        schema_path = (schema_root / schema_file).resolve()
        try:
            schema = load_schema(schema_root, schema_file)
        except SchemaRegistryError as exc:
            errors.append(f"{subject}: {exc}")
            continue

        try:
            contract = find_contract_for_schema(schema_path)
        except SchemaRegistryError as exc:
            errors.append(f"{subject}: {exc}")
            continue

        try:
            enforce_contract(subject, schema_path, schema, contract, compatibility)
        except SchemaRegistryError as exc:
            errors.append(f"{subject}: {exc}")
            continue

        validated[subject] = ValidatedSubject(
            subject=subject,
            schema_path=schema_path,
            schema=schema,
            contract=contract,
        )

    if errors:
        raise SchemaRegistryError("Schema contract validation failed:\n - " + "\n - ".join(errors))
    return validated


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
        "--include-eia",
        action="store_true",
        help="Merge subjects derived from config/eia_ingest_datasets.json",
    )
    parser.add_argument(
        "--eia-config",
        type=Path,
        default=DEFAULT_EIA_CONFIG,
        help=f"Path to eia_ingest_datasets.json (default: {DEFAULT_EIA_CONFIG})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without calling the Schema Registry",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate subject naming and schema contracts without hitting the registry",
    )
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)

    compatibility = normalize_compatibility(args.compatibility)

    subjects = load_subject_mapping(args.subjects_file)
    if args.include_eia:
        subjects.update(load_eia_subjects(args.eia_config))
    selected_subjects = iter_subjects(args.subject, subjects)

    validated = validate_contracts(selected_subjects, args.schema_root, compatibility)

    if args.validate_only:
        print(f"Validated {len(validated)} schema contract(s) (compatibility={compatibility})")
        return 0

    if args.dry_run:
        for subject, details in validated.items():
            print(
                f"[dry-run] subject={subject} contract={details.contract.name} "
                f"compatibility={compatibility}"
            )
            print(
                f"[dry-run] would register {details.schema_path.name} -> {subject}"
            )
        return 0

    session = create_session()

    for subject, details in validated.items():
        put_compatibility(
            args.schema_registry_url,
            subject,
            compatibility,
            session=session,
        )
        version = register_schema(
            args.schema_registry_url,
            subject,
            details.schema,
            session=session,
        )
        print(
            f"Registered {subject} (schema: {details.schema_path.name}) -> version/id {version}"
        )

    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    try:
        raise SystemExit(main())
    except SchemaRegistryError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(1)
