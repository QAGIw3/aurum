"""Runtime model generation from Avro contracts."""

from __future__ import annotations

from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Type, Union

from pydantic import BaseModel, Field, create_model

from .contracts import SubjectContracts

_PY_PRIMITIVES: Dict[str, Any] = {
    "string": str,
    "int": int,
    "long": int,
    "float": float,
    "double": float,
    "boolean": bool,
    "bytes": bytes,
    "null": type(None),
}


class ContractModelError(RuntimeError):
    """Raised when a Pydantic contract model cannot be generated."""


@lru_cache(maxsize=1)
def _default_contracts() -> SubjectContracts:
    """Load the contract catalog from the repository default location."""
    contracts_path = Path(__file__).resolve().parents[3] / "kafka" / "schemas" / "contracts.yml"
    return SubjectContracts(contracts_path)


def _ensure_contracts(catalog: Optional[SubjectContracts]) -> SubjectContracts:
    return catalog or _default_contracts()


def _resolve_logical_type(schema: Dict[str, Any]) -> Optional[Any]:
    logical = schema.get("logicalType")
    if not logical:
        return None

    if logical == "timestamp-micros":
        return datetime
    if logical == "timestamp-millis":
        return datetime
    if logical == "time-micros":
        return datetime
    if logical == "time-millis":
        return datetime
    if logical == "date":
        return date
    return None


def _avro_type_to_annotation(avro_type: Any) -> Tuple[Any, bool]:
    """Convert an Avro type declaration to a Python typing annotation.

    Returns a tuple of (annotation, optional_flag).
    """
    if isinstance(avro_type, list):
        non_null_types = [t for t in avro_type if t != "null"]
        optional = len(non_null_types) != len(avro_type)

        if not non_null_types:
            return type(None), True

        if len(non_null_types) == 1:
            annotation, nested_optional = _avro_type_to_annotation(non_null_types[0])
            return annotation, optional or nested_optional

        annotations = []
        optional_nested = optional
        for subtype in non_null_types:
            resolved, nested_optional = _avro_type_to_annotation(subtype)
            annotations.append(resolved)
            optional_nested = optional_nested or nested_optional

        return Union[tuple(annotations)], optional_nested  # type: ignore[arg-type]

    if isinstance(avro_type, dict):
        logical = _resolve_logical_type(avro_type)
        if logical is not None:
            return logical, False

        avro_type_name = avro_type.get("type")
        if avro_type_name == "enum":
            symbols = tuple(avro_type.get("symbols", []))
            if not symbols:
                raise ContractModelError("Enum type defined without symbols")
            return Literal.__getitem__(symbols), False  # type: ignore[arg-type]

        if avro_type_name == "array":
            item_type = avro_type.get("items", "string")
            resolved, _ = _avro_type_to_annotation(item_type)
            return List[resolved], False

        if avro_type_name == "map":
            value_type = avro_type.get("values", "string")
            resolved, _ = _avro_type_to_annotation(value_type)
            return Dict[str, resolved], False

        if avro_type_name == "record":
            # Nested records are represented as generic dicts
            return Dict[str, Any], False

        if avro_type_name == "fixed":
            return bytes, False

        if avro_type_name:
            return _avro_type_to_annotation(avro_type_name)

        raise ContractModelError(f"Unsupported Avro type declaration: {avro_type}")

    if isinstance(avro_type, str):
        python_type = _PY_PRIMITIVES.get(avro_type)
        if python_type is None:
            raise ContractModelError(f"Unsupported Avro primitive: {avro_type}")
        return python_type, avro_type == "null"

    raise ContractModelError(f"Unrecognized Avro type declaration: {avro_type}")


def _build_model_fields(schema: Dict[str, Any]) -> Dict[str, Tuple[Any, Field]]:
    fields: Dict[str, Tuple[Any, Field]] = {}
    for field in schema.get("fields", []):
        field_type = field.get("type")
        if field_type is None:
            raise ContractModelError(f"Field '{field.get('name')}' is missing a type")

        annotation, is_optional = _avro_type_to_annotation(field_type)
        if is_optional and annotation is not type(None):
            annotation = Optional[annotation]

        has_default = "default" in field
        default = field.get("default") if has_default else None
        if has_default and default == "null":
            default = None

        metadata: Dict[str, Any] = {}
        if field.get("doc"):
            metadata["description"] = field["doc"]

        if not has_default and not is_optional:
            field_info = Field(..., **metadata)
        elif not has_default and is_optional:
            field_info = Field(default=None, **metadata)
        elif has_default:
            field_info = Field(default=default, **metadata)
        else:
            field_info = Field(..., **metadata)

        fields[field["name"]] = (annotation, field_info)
    return fields


@lru_cache(maxsize=None)
def generate_model(subject: str, *, contracts: Optional[SubjectContracts] = None) -> Type[BaseModel]:
    """Generate (or fetch cached) Pydantic model for a subject."""
    catalog = _ensure_contracts(contracts)
    catalog.validate_subject_name(subject)

    schema = catalog.load_schema(subject)
    model_fields = _build_model_fields(schema)

    if not model_fields:
        raise ContractModelError(f"Schema for subject '{subject}' has no fields")

    model_name = schema.get("name", subject.split(".")[-1].title())
    model_doc = schema.get("doc")

    model = create_model(
        model_name,
        __base__=BaseModel,
        __module__=__name__,
        **model_fields,
    )

    if model_doc:
        model.__doc__ = model_doc

    return model


def get_model(subject: str, *, contracts: Optional[SubjectContracts] = None) -> Type[BaseModel]:
    """Public helper that returns the cached Pydantic model for a subject."""
    return generate_model(subject, contracts=contracts)


def payload_from_contract(subject: str, payload: Dict[str, Any]) -> BaseModel:
    """Validate and coerce a payload using the contract-generated model."""
    model = get_model(subject)
    return model.model_validate(payload)
