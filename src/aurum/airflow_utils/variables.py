"""Utilities for working with Airflow Variable definitions."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping

__all__ = [
    "VariableFileError",
    "VariableValidationError",
    "load_variable_mapping",
    "validate_variable_mapping",
]


class VariableFileError(RuntimeError):
    """Raised when the variables file cannot be parsed."""


@dataclass(frozen=True)
class VariableValidationError(ValueError):
    """Raised when the variable mapping fails validation."""

    errors: tuple[str, ...]

    def __str__(self) -> str:  # pragma: no cover - simple override
        return "; ".join(self.errors)


def load_variable_mapping(path: Path) -> dict[str, str]:
    """Load an Airflow variable mapping from ``path``.

    Values are normalised to strings so downstream consumers can rely on
    consistent types even if the JSON file contains integers or booleans.
    """

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:  # pragma: no cover - defensive
        raise VariableFileError(f"Variables file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise VariableFileError(f"Invalid JSON in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise VariableFileError(f"Expected top-level object mapping in {path}")

    mapping: dict[str, str] = {}
    for raw_key, raw_value in data.items():
        key = str(raw_key)
        value = "" if raw_value is None else str(raw_value)
        mapping[key] = value
    return mapping


def validate_variable_mapping(mapping: Mapping[str, str]) -> list[str]:
    """Return a list of validation errors for ``mapping``."""

    errors: list[str] = []
    for key, value in mapping.items():
        if not key:
            errors.append("Variable keys must be non-empty strings")
            continue
        if not key.startswith("aurum_"):
            errors.append(f"Variable '{key}' must start with 'aurum_'")
        if " " in key:
            errors.append(f"Variable '{key}' must not contain whitespace")
        if value is None:
            errors.append(f"Variable '{key}' has null value; use empty string instead")
        elif value != value.strip() and value.strip():
            errors.append(f"Variable '{key}' has leading/trailing whitespace in value")
    if not mapping:
        errors.append("Variable mapping is empty")
    return errors
