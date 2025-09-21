"""Standardized exception handling for the API."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException, Request


class AurumAPIException(HTTPException):
    """Base exception for Aurum API errors."""

    def __init__(
        self,
        status_code: int,
        detail: str,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(status_code=status_code, detail=detail)
        self.request_id = request_id
        self.context = context or {}


class ValidationException(AurumAPIException):
    """Exception raised when request validation fails."""

    def __init__(
        self,
        detail: str,
        request_id: Optional[str] = None,
        field_errors: Optional[Dict[str, str]] = None,
    ):
        context = {"field_errors": field_errors} if field_errors else {}
        super().__init__(
            status_code=400,
            detail=detail,
            request_id=request_id,
            context=context,
        )


class NotFoundException(AurumAPIException):
    """Exception raised when a requested resource is not found."""

    def __init__(
        self,
        resource_type: str,
        resource_id: str,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        detail = f"{resource_type} '{resource_id}' not found"
        super().__init__(
            status_code=404,
            detail=detail,
            request_id=request_id,
            context=context or {"resource_type": resource_type, "resource_id": resource_id},
        )


class ForbiddenException(AurumAPIException):
    """Exception raised when access is forbidden."""

    def __init__(
        self,
        detail: str = "Access denied",
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            status_code=403,
            detail=detail,
            request_id=request_id,
            context=context,
        )


class ServiceUnavailableException(AurumAPIException):
    """Exception raised when a service is unavailable."""

    def __init__(
        self,
        service: str,
        detail: Optional[str] = None,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        if detail is None:
            detail = f"Service '{service}' is currently unavailable"
        super().__init__(
            status_code=503,
            detail=detail,
            request_id=request_id,
            context=context or {"service": service},
        )


class DataProcessingException(AurumAPIException):
    """Exception raised when data processing fails."""

    def __init__(
        self,
        operation: str,
        detail: Optional[str] = None,
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        if detail is None:
            detail = f"Data processing failed for operation '{operation}'"
        super().__init__(
            status_code=500,
            detail=detail,
            request_id=request_id,
            context=context or {"operation": operation},
        )


def handle_api_exception(request: Request, exc: Exception) -> HTTPException:
    """Convert exceptions to standardized HTTP responses."""
    from ..telemetry.context import get_request_id

    request_id = get_request_id()

    # Handle our custom exceptions
    if isinstance(exc, AurumAPIException):
        # Add request context to the response
        detail = exc.detail
        if exc.context:
            detail = {
                "error": detail,
                "context": exc.context,
                "request_id": request_id,
            }
        return HTTPException(status_code=exc.status_code, detail=detail)

    # Handle FastAPI's HTTPException
    if isinstance(exc, HTTPException):
        return exc

    # Handle validation errors from Pydantic
    if isinstance(exc, ValueError) and "validation" in str(exc).lower():
        return ValidationException(
            detail=str(exc),
            request_id=request_id,
        )

    # Handle all other exceptions as internal server errors
    return HTTPException(
        status_code=500,
        detail={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
            "request_id": request_id,
            "type": exc.__class__.__name__,
        },
    )


# Common exception factories
def invalid_cursor_exception(request_id: Optional[str] = None) -> ValidationException:
    """Create an exception for invalid cursor parameters."""
    return ValidationException(
        detail="Invalid cursor parameter",
        request_id=request_id,
        context={"parameter": "cursor"},
    )


def tenant_required_exception(request_id: Optional[str] = None) -> ValidationException:
    """Create an exception when tenant_id is required."""
    return ValidationException(
        detail="tenant_id is required",
        request_id=request_id,
        context={"field": "tenant_id"},
    )


def invalid_region_exception(request_id: Optional[str] = None) -> ValidationException:
    """Create an exception for invalid region parameters."""
    return ValidationException(
        detail="Invalid region parameter format",
        request_id=request_id,
        context={"parameter": "region"},
    )


def admin_required_exception(request_id: Optional[str] = None) -> ForbiddenException:
    """Create an exception when admin access is required."""
    return ForbiddenException(
        detail="Administrator access required",
        request_id=request_id,
        context={"required_role": "admin"},
    )


def metrics_unavailable_exception(request_id: Optional[str] = None) -> ServiceUnavailableException:
    """Create an exception when metrics service is unavailable."""
    return ServiceUnavailableException(
        service="metrics",
        request_id=request_id,
    )
