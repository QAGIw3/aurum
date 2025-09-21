from __future__ import annotations

import pathlib

import yaml
from openapi_spec_validator import validate_spec
from openapi_spec_validator.validation.exceptions import OpenAPIValidationError


SPEC_PATH = pathlib.Path(__file__).resolve().parents[2] / "openapi" / "aurum.yaml"


def test_openapi_spec_is_valid() -> None:
    raw = SPEC_PATH.read_text(encoding="utf-8")
    document = yaml.safe_load(raw)
    try:
        validate_spec(document)
    except OpenAPIValidationError as exc:  # pragma: no cover - failure path surfaces validation issues
        raise AssertionError(f"OpenAPI specification validation failed: {exc}") from exc
