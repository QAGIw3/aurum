"""Pagination utilities for the Aurum API.

Provides cursor-based pagination primitives (encoding/decoding, schema validation,
and metadata helpers) and legacy offset helpers.

See also: docs/pagination.md for behavior guarantees and examples.
"""

from __future__ import annotations

import base64
import hashlib
import json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from enum import Enum
from dataclasses import dataclass
from datetime import date, datetime

from fastapi import HTTPException

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500
MAX_CURSOR_LENGTH = 1000


class SortDirection(Enum):
    """Sort direction for cursor pagination."""
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True)
class SortKey:
    """A frozen sort key definition."""
    name: str
    direction: SortDirection = SortDirection.ASC
    nullable: bool = False

    def __hash__(self) -> int:
        return hash((self.name, self.direction.value, self.nullable))

    def __eq__(self, other) -> bool:
        if not isinstance(other, SortKey):
            return False
        return (self.name == other.name and
                self.direction == other.direction and
                self.nullable == other.nullable)


@dataclass(frozen=True)
class CursorSchema:
    """Defines the sort keys for a cursor-based endpoint."""
    name: str  # Human-readable name for the schema
    sort_keys: tuple[SortKey, ...]  # Frozen tuple of sort keys
    version: str = "v1"  # Schema version for compatibility

    def __hash__(self) -> int:
        return hash((self.name, tuple(self.sort_keys), self.version))


# Frozen cursor schemas for different endpoints
FROZEN_CURSOR_SCHEMAS = {
    "curves": CursorSchema(
        name="curves",
        sort_keys=(
            SortKey("curve_key", SortDirection.ASC),
            SortKey("tenor_label", SortDirection.ASC),
            SortKey("contract_month", SortDirection.ASC),
            SortKey("asof_date", SortDirection.ASC),
            SortKey("price_type", SortDirection.ASC),
        )
    ),

    "curve_diff": CursorSchema(
        name="curve_diff",
        sort_keys=(
            SortKey("curve_key", SortDirection.ASC),
            SortKey("tenor_label", SortDirection.ASC),
            SortKey("contract_month", SortDirection.ASC),
        )
    ),

    "curve_strips": CursorSchema(
        name="curve_strips",
        sort_keys=(
            SortKey("curve_key", SortDirection.ASC),
            SortKey("tenor_label", SortDirection.ASC),
            SortKey("contract_month", SortDirection.ASC),
            SortKey("asof_date", SortDirection.DESC),
        )
    ),

    "eia_series": CursorSchema(
        name="eia_series",
        sort_keys=(
            SortKey("series_id", SortDirection.ASC),
            SortKey("period_start", SortDirection.ASC),
            SortKey("period", SortDirection.ASC),
        )
    ),

    "scenarios": CursorSchema(
        name="scenarios",
        sort_keys=(
            SortKey("created_at", SortDirection.DESC),
        )
    ),

    "scenario_runs": CursorSchema(
        name="scenario_runs",
        sort_keys=(
            SortKey("created_at", SortDirection.DESC),
        )
    ),

    "scenario_outputs": CursorSchema(
        name="scenario_outputs",
        sort_keys=(
            SortKey("scenario_id", SortDirection.ASC),
            SortKey("curve_key", SortDirection.ASC),
            SortKey("tenor_label", SortDirection.ASC),
            SortKey("contract_month", SortDirection.ASC),
            SortKey("metric", SortDirection.ASC),
            SortKey("run_id", SortDirection.ASC),
        )
    ),

    "scenario_metrics": CursorSchema(
        name="scenario_metrics",
        sort_keys=(
            SortKey("metric", SortDirection.ASC),
            SortKey("tenor_label", SortDirection.ASC),
            SortKey("curve_key", SortDirection.ASC),
        )
    ),

    "ppa_contracts": CursorSchema(
        name="ppa_contracts",
        sort_keys=(
            SortKey("created_at", SortDirection.DESC),
        )
    ),

    "ppa_valuations": CursorSchema(
        name="ppa_valuations",
        sort_keys=(
            SortKey("created_at", SortDirection.DESC),
        )
    ),

    "external_providers": CursorSchema(
        name="external_providers",
        sort_keys=(
            SortKey("name", SortDirection.ASC),
        )
    ),

    "external_series": CursorSchema(
        name="external_series",
        sort_keys=(
            SortKey("provider", SortDirection.ASC),
            SortKey("series_id", SortDirection.ASC),
        )
    ),
}


@dataclass(frozen=True)
class CursorPayload:
    """Standardized cursor payload with schema validation."""
    schema_name: str
    values: Dict[str, Any]
    direction: SortDirection
    version: str = "v1"

    def __post_init__(self):
        if self.schema_name not in FROZEN_CURSOR_SCHEMAS:
            raise ValueError(f"Unknown cursor schema: {self.schema_name}")

        schema = FROZEN_CURSOR_SCHEMAS[self.schema_name]
        if self.version != schema.version:
            raise ValueError(f"Schema version mismatch: expected {schema.version}, got {self.version}")

        # Validate that we have values for required sort keys
        sort_keys = [key.name for key in schema.sort_keys]
        missing_keys = set(sort_keys) - set(self.values.keys())
        if missing_keys:
            raise ValueError(f"Missing cursor values for keys: {missing_keys}")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for encoding."""
        return {
            "schema": self.schema_name,
            "values": self.values,
            "direction": self.direction.value,
            "version": self.version
        }


def encode_cursor(payload: Dict[str, Any], schema_name: Optional[str] = None) -> str:
    """Encode a cursor payload as a base64-encoded JSON string.

    Args:
        payload: Dictionary to encode as cursor
        schema_name: Optional schema name for validation

    Returns:
        Base64-encoded cursor string
    """
    # If payload is already a CursorPayload, use its schema
    if isinstance(payload, CursorPayload):
        schema_name = payload.schema_name

    # Create standardized payload
    if schema_name and schema_name in FROZEN_CURSOR_SCHEMAS:
        schema = FROZEN_CURSOR_SCHEMAS[schema_name]
        direction = SortDirection.ASC
        if len(schema.sort_keys) > 0:
            direction = schema.sort_keys[0].direction

        standardized_payload = CursorPayload(
            schema_name=schema_name,
            values=payload,
            direction=direction
        )
        final_payload = standardized_payload.to_dict()
    else:
        # Legacy format for backward compatibility
        final_payload = payload

    raw = json.dumps(final_payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(token: str) -> CursorPayload:
    """Decode a base64-encoded cursor token.

    Args:
        token: Base64-encoded cursor string

    Returns:
        Decoded cursor payload as CursorPayload

    Raises:
        HTTPException: If cursor is invalid or malformed
    """
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        payload_dict = json.loads(raw.decode("utf-8"))

        # Check if this is a new standardized cursor
        if "schema" in payload_dict and "values" in payload_dict:
            return CursorPayload(
                schema_name=payload_dict["schema"],
                values=payload_dict["values"],
                direction=SortDirection(payload_dict.get("direction", "asc")),
                version=payload_dict.get("version", "v1")
            )
        else:
            # Legacy format - try to infer schema from payload
            # This is a fallback for backward compatibility
            return CursorPayload(
                schema_name="legacy",  # Special schema for legacy cursors
                values=payload_dict,
                direction=SortDirection.ASC
            )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc


def extract_cursor_values(cursor: CursorPayload, schema_name: str) -> Dict[str, Any]:
    """Extract cursor values for database queries.

    Args:
        cursor: CursorPayload object
        schema_name: Schema name to validate against

    Returns:
        Dictionary of cursor values for query construction

    Raises:
        HTTPException: If cursor doesn't match expected schema
    """
    if cursor.schema_name != schema_name:
        raise HTTPException(
            status_code=400,
            detail=f"Cursor schema mismatch: expected {schema_name}, got {cursor.schema_name}"
        )

    return cursor.values


def validate_cursor_schema(cursor: CursorPayload, schema_name: str) -> None:
    """Validate that cursor matches the expected schema.

    Args:
        cursor: CursorPayload to validate
        schema_name: Expected schema name

    Raises:
        HTTPException: If cursor schema is invalid
    """
    if cursor.schema_name != schema_name:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid cursor schema: expected {schema_name}, got {cursor.schema_name}"
        )

    # Validate required fields
    expected_schema = FROZEN_CURSOR_SCHEMAS.get(schema_name)
    if expected_schema:
        expected_keys = {key.name for key in expected_schema.sort_keys}
        actual_keys = set(cursor.values.keys())
        if not expected_keys.issubset(actual_keys):
            missing = expected_keys - actual_keys
            raise HTTPException(
                status_code=400,
                detail=f"Cursor missing required values: {missing}"
            )


def normalize_cursor_input(payload: Dict[str, Any]) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """Normalize cursor input to extract offset and additional metadata.

    Args:
        payload: Cursor payload dictionary

    Returns:
        Tuple of (offset, additional_metadata)

    Raises:
        HTTPException: If cursor contains invalid offset
    """
    if "offset" in payload:
        try:
            offset = int(payload.get("offset", 0))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid offset cursor") from exc

        # Preserve any supplemental payload data (filters, limits, etc.)
        extra = {key: value for key, value in payload.items() if key != "offset"}
        return offset, extra or None

    # For future cursor-based pagination extensions
    return None, payload


def extract_cursor_payload_from_row(
    row: Mapping[str, Any] | Dict[str, Any],
    fields: Iterable[str],
) -> Dict[str, Any]:
    """Serialize cursor fields from a result row into a JSON-safe payload."""

    payload: Dict[str, Any] = {}
    for field in fields:
        if hasattr(row, "get"):
            value = row.get(field)  # type: ignore[arg-type]
        else:  # pragma: no cover - defensive for non-mapping rows
            value = getattr(row, field, None)

        if isinstance(value, (datetime, date)):
            payload[field] = value.isoformat()
        else:
            payload[field] = value
    return payload


def create_pagination_metadata(
    *,
    total: Optional[int] = None,
    limit: int,
    offset: int = 0,
    next_cursor: Optional[str] = None,
    prev_cursor: Optional[str] = None,
    has_next: Optional[bool] = None,
    has_prev: Optional[bool] = None,
) -> Dict[str, Any]:
    """Create standardized pagination metadata.

    Args:
        total: Total number of items (optional)
        limit: Maximum items per page
        offset: Current offset
        next_cursor: Cursor for next page
        prev_cursor: Cursor for previous page
        has_next: Whether there are more items after current page
        has_prev: Whether there are items before current page

    Returns:
        Pagination metadata dictionary
    """
    metadata = {
        "limit": limit,
        "offset": offset,
    }

    if total is not None:
        metadata["total"] = total

    if next_cursor:
        metadata["next_cursor"] = next_cursor
    elif has_next is not None:
        metadata["has_next"] = has_next

    if prev_cursor:
        metadata["prev_cursor"] = prev_cursor
    elif has_prev is not None:
        metadata["has_prev"] = has_prev

    return metadata


def deprecation_warning_headers(deprecated_feature: str, removal_version: str) -> Dict[str, str]:
    """Generate deprecation warning headers.

    Args:
        deprecated_feature: Name of the deprecated feature
        removal_version: Version when feature will be removed

    Returns:
        Headers dictionary with deprecation warnings
    """
    return {
        "Deprecation": "true",
        "X-Deprecation-Info": f"Feature '{deprecated_feature}' is deprecated",
        "X-Sunset": f"Removed in version {removal_version}",
    }
