"""Subject contract catalog for Avro schema governance."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence

import yaml


class ContractError(Exception):
    """Base exception for contract catalog issues."""


class SubjectNotDefinedError(ContractError):
    """Raised when a subject is not present in the contract catalog."""


class SubjectPatternError(ContractError):
    """Raised when a subject does not satisfy the naming contract."""


class SchemaContractMismatch(ContractError):
    """Raised when schema content does not match the frozen contract file."""


class SchemaNotFoundError(ContractError):
    """Raised when the referenced schema file cannot be located."""


@dataclass(frozen=True)
class SubjectContract:
    """Contract metadata for a single Avro subject."""

    subject: str
    schema: str
    topic: str
    compatibility: str

    def schema_path(self, search_paths: Sequence[Path]) -> Path:
        """Resolve the schema path within the provided search paths."""
        for root in search_paths:
            candidate = root / self.schema
            if candidate.exists():
                return candidate
        raise SchemaNotFoundError(
            f"Schema file '{self.schema}' for subject '{self.subject}' not found in {list(search_paths)}"
        )


class SubjectContracts:
    """Immutable catalog describing all supported Avro subjects."""

    def __init__(
        self,
        contracts_path: Path,
        *,
        schema_search_paths: Optional[Sequence[Path]] = None,
    ) -> None:
        if not contracts_path.exists():
            default_catalog = Path(__file__).resolve().parents[3] / "kafka" / "schemas" / "contracts.yml"
            if default_catalog.exists():
                contracts_path = default_catalog
            else:
                raise ContractError(f"Contracts catalog not found: {contracts_path}")

        self.contracts_path = contracts_path
        self._schema_dirs: List[Path] = list(schema_search_paths or [contracts_path.parent])
        self._raw = yaml.safe_load(contracts_path.read_text()) or {}

        defaults = self._raw.get("defaults", {})
        naming_defaults = defaults.get("naming", {})

        pattern_str = naming_defaults.get("pattern", r"^aurum\\.[a-z0-9_.]+\\.v[0-9]+(-key|-value)?$")
        self._pattern = re.compile(pattern_str)
        self._pattern_exceptions = set(naming_defaults.get("exceptions", []))
        self._default_compatibility = defaults.get("compatibility", "BACKWARD")

        self._contracts: Dict[str, SubjectContract] = {}
        self._schema_index: Dict[str, List[SubjectContract]] = {}

        for item in self._raw.get("subjects", []):
            subject = item.get("subject")
            schema = item.get("schema")
            topic = item.get("topic")
            compatibility = item.get("compatibility", self._default_compatibility)

            if not subject or not schema:
                raise ContractError(f"Invalid contract entry: {item}")

            contract = SubjectContract(
                subject=subject,
                schema=schema,
                topic=topic or subject,
                compatibility=compatibility,
            )
            if subject in self._contracts:
                raise ContractError(f"Duplicate subject defined in contracts: {subject}")

            self._contracts[subject] = contract
            self._schema_index.setdefault(schema, []).append(contract)

    def list_subjects(self) -> List[SubjectContract]:
        """Return all subject contracts."""
        return list(self._contracts.values())

    def get(self, subject: str) -> SubjectContract:
        """Get the contract for a specific subject."""
        try:
            return self._contracts[subject]
        except KeyError as exc:
            raise SubjectNotDefinedError(f"Subject '{subject}' is not defined in the contract catalog") from exc

    def subjects_for_schema(self, schema_filename: str) -> List[SubjectContract]:
        """Return all subjects that use the provided schema filename."""
        return self._schema_index.get(schema_filename, [])

    def validate_subject_name(self, subject: str) -> None:
        """Ensure the subject adheres to the naming contract."""
        if subject in self._pattern_exceptions:
            return
        if not self._pattern.match(subject):
            raise SubjectPatternError(
                f"Subject '{subject}' does not satisfy naming pattern '{self._pattern.pattern}'"
            )

    def load_schema(self, subject: str) -> Dict[str, object]:
        """Load the frozen schema JSON for a subject."""
        contract = self.get(subject)
        schema_path = contract.schema_path(self._schema_dirs)
        return json.loads(schema_path.read_text())

    def validate_schema_payload(self, subject: str, payload: Dict[str, object]) -> None:
        """Compare supplied schema payload with frozen contract file."""
        contract = self.get(subject)
        expected = self.load_schema(subject)
        if expected != payload:
            raise SchemaContractMismatch(
                f"Schema payload for subject '{subject}' does not match frozen contract {contract.schema}"
            )

    def schema_paths(self) -> Iterable[Path]:
        """Yield schema file paths referenced by the catalog."""
        for contract in self._contracts.values():
            yield contract.schema_path(self._schema_dirs)

    def get_default_compatibility(self) -> str:
        """Return default compatibility value from catalog."""
        return self._default_compatibility

    def naming_pattern(self) -> re.Pattern[str]:
        """Return compiled naming pattern."""
        return self._pattern

    def naming_exceptions(self) -> List[str]:
        """Return the set of naming exceptions."""
        return sorted(self._pattern_exceptions)

    def schema_search_paths(self) -> List[Path]:
        """Return configured schema search paths."""
        return list(self._schema_dirs)
