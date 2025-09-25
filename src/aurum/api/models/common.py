from __future__ import annotations

"""Common response envelopes and error payloads."""

from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import Field, field_validator

from aurum.api.http.pagination import MAX_CURSOR_LENGTH

from .base import AurumBaseModel


class Meta(AurumBaseModel):
    """Standard metadata envelope returned with list responses."""

    request_id: str
    query_time_ms: int = Field(ge=0)
    next_cursor: str | None = None
    prev_cursor: str | None = None
    has_more: Optional[bool] = None
    count: Optional[int] = None
    total: Optional[int] = None
    offset: Optional[int] = None
    limit: Optional[int] = None

    @field_validator("next_cursor", "prev_cursor")
    @classmethod
    def _validate_cursor_length(cls, value: str | None) -> str | None:
        if value is not None and len(value) > MAX_CURSOR_LENGTH:
            raise ValueError("Cursor too long")
        return value


class ErrorEnvelope(AurumBaseModel):
    """Consistent error response payload (RFC 7807 friendly)."""

    error: str
    message: Optional[str] = None
    code: Optional[str] = None
    field: Optional[str] = None
    value: Optional[Any] = None
    context: Optional[Dict[str, Any]] = None
    request_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("context")
    @classmethod
    def _ensure_mapping(cls, value: Dict[str, Any] | None) -> Dict[str, Any] | None:
        if value is not None and not isinstance(value, dict):
            raise ValueError("Context must be a dictionary")
        return value


class ValidationErrorDetail(AurumBaseModel):
    """Canonical shape for individual validation errors."""

    field: str
    message: str
    value: Optional[Any] = None
    code: Optional[str] = None
    constraint: Optional[str] = None

    @field_validator("constraint")
    @classmethod
    def _validate_constraint(cls, value: str | None) -> str | None:
        if value is None:
            return None
        valid = {
            "required",
            "type",
            "format",
            "min",
            "max",
            "length",
            "pattern",
            "enum",
            "unique",
            "custom",
        }
        if value not in valid:
            raise ValueError(f"Invalid constraint type: {value}")
        return value


class ValidationErrorResponse(AurumBaseModel):
    """Envelope for returning multiple validation errors."""

    error: str = "Validation Error"
    message: str
    field_errors: list[ValidationErrorDetail] = Field(default_factory=list)
    request_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)


__all__ = [
    "Meta",
    "ErrorEnvelope",
    "ValidationErrorDetail",
    "ValidationErrorResponse",
]
