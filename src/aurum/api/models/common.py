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


class TooManyRequestsError(ErrorEnvelope):
    """429 response when tenant queues exceed configured capacity."""

    error: str = "TooManyRequests"
    queue_depth: int = Field(ge=0)
    retry_after_seconds: int = Field(ge=0)


class QueueServiceUnavailableError(ErrorEnvelope):
    """503 response when queue wait exceeded configured timeout."""

    error: str = "ServiceUnavailable"
    queue_depth: int = Field(ge=0)
    retry_after_seconds: int = Field(ge=0)


class RequestTimeoutError(ErrorEnvelope):
    """504 response when request execution exceeds timeout budget."""

    error: str = "GatewayTimeout"
    timeout_seconds: int = Field(ge=0)
    retry_after_seconds: Optional[int] = Field(default=None, ge=0)


class ProblemDetail(AurumBaseModel):
    """RFC 7807 Problem Details for HTTP APIs compliant error response.
    
    This model implements the Problem Details specification to provide
    consistent, machine-readable error responses across all API endpoints.
    """
    
    type: str = Field(
        description="A URI reference that identifies the problem type",
        examples=["https://api.aurum.com/problems/validation-error", "about:blank"]
    )
    title: str = Field(
        description="A short, human-readable summary of the problem type"
    )
    status: int = Field(
        description="The HTTP status code",
        ge=100,
        le=599
    )
    detail: Optional[str] = Field(
        default=None,
        description="A human-readable explanation specific to this occurrence"
    )
    instance: Optional[str] = Field(
        default=None,
        description="A URI reference that identifies the specific occurrence"
    )
    # Additional Aurum-specific fields
    request_id: Optional[str] = Field(
        default=None,
        description="Unique identifier for the request"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z",
        description="When the error occurred (ISO 8601 format)"
    )
    errors: Optional[list[ValidationErrorDetail]] = Field(
        default=None,
        description="Detailed validation errors if applicable"
    )


__all__ = [
    "Meta",
    "ErrorEnvelope",
    "ValidationErrorDetail", 
    "ValidationErrorResponse",
    "TooManyRequestsError",
    "QueueServiceUnavailableError",
    "RequestTimeoutError",
    "ProblemDetail",
]
