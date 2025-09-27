"""Standardized exception handling for the API."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.datastructures import MutableHeaders
from starlette.types import Message

from .models import ErrorEnvelope, ValidationErrorDetail, ValidationErrorResponse, ProblemDetail


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
    """Convert exceptions to standardized HTTP responses with consistent error envelopes."""
    from ..telemetry.context import get_request_id

    request_id = get_request_id()

    # Handle our custom exceptions
    if isinstance(exc, AurumAPIException):
        # Use new error envelope format
        error_envelope = ErrorEnvelope(
            error=exc.__class__.__name__,
            message=exc.detail,
            code=getattr(exc, "code", None),
            field=getattr(exc, "field", None),
            context=exc.context,
            request_id=request_id,
        )
        return HTTPException(status_code=exc.status_code, detail=error_envelope.model_dump())

    # Handle FastAPI's HTTPException
    if isinstance(exc, HTTPException):
        # Convert to our error envelope format
        error_envelope = ErrorEnvelope(
            error="HTTPException",
            message=exc.detail,
            request_id=request_id,
        )
        return HTTPException(status_code=exc.status_code, detail=error_envelope.model_dump())

    # Handle Pydantic ValidationError
    if hasattr(exc, "model") and hasattr(exc, "errors"):
        # Convert Pydantic validation errors to our format
        field_errors = []
        for error in exc.errors():
            field_errors.append(ValidationErrorDetail(
                field=".".join(str(loc) for loc in error.get("loc", [])),
                message=error.get("msg", "Validation error"),
                value=error.get("input"),
                code=error.get("type"),
            ))

        validation_response = ValidationErrorResponse(
            message="Request validation failed",
            field_errors=field_errors,
            request_id=request_id,
        )
        return HTTPException(status_code=400, detail=validation_response.model_dump())

    # Handle other ValueError exceptions
    if isinstance(exc, ValueError):
        error_envelope = ErrorEnvelope(
            error="ValueError",
            message=str(exc),
            request_id=request_id,
        )
        return HTTPException(status_code=400, detail=error_envelope.model_dump())

    # Handle all other exceptions as internal server errors
    error_envelope = ErrorEnvelope(
        error="InternalServerError",
        message="An unexpected error occurred",
        context={
            "type": exc.__class__.__name__,
            "module": exc.__class__.__module__,
        },
        request_id=request_id,
    )
    return HTTPException(status_code=500, detail=error_envelope.model_dump())


async def create_rfc7807_error_response(
    error: Exception, 
    request: Request,
    base_url: str = "https://api.aurum.com"
) -> JSONResponse:
    """Create RFC7807 compliant error response from any exception.
    
    This function is the async-compatible entry point for creating RFC7807
    Problem Detail responses. It should be used in exception handlers.
    """
    from ..telemetry.context import get_request_id
    
    request_id = get_request_id()
    instance = str(request.url)
    base_url = base_url.rstrip("/")
    
    # Handle Aurum API exceptions
    if isinstance(error, AurumAPIException):
        type_mapping = {
            "ValidationException": "validation-error",
            "NotFoundException": "not-found", 
            "ForbiddenException": "forbidden",
            "ServiceUnavailableException": "service-unavailable",
            "DataProcessingException": "data-processing-error"
        }
        
        exc_name = error.__class__.__name__
        problem_type = type_mapping.get(exc_name, "api-error")
        title = exc_name.replace("Exception", " Error").replace("_", " ")
        if title.endswith(" Error"):
            title = title[:-6] + " Error"  # Remove duplicate "Error"
        title = title.strip()
        
        problem = ProblemDetail(
            type=f"{base_url}/problems/{problem_type}",
            title=title,
            status=error.status_code,
            detail=error.detail,
            instance=instance,
            request_id=request_id or error.request_id
        )
    
    # Handle FastAPI HTTP exceptions
    elif isinstance(error, HTTPException):
        status_titles = {
            400: "Bad Request",
            401: "Unauthorized",
            403: "Forbidden", 
            404: "Not Found",
            405: "Method Not Allowed",
            422: "Unprocessable Entity",
            429: "Too Many Requests",
            500: "Internal Server Error",
            502: "Bad Gateway",
            503: "Service Unavailable",
            504: "Gateway Timeout"
        }
        
        title = status_titles.get(error.status_code, "HTTP Error")
        
        problem = ProblemDetail(
            type="about:blank",
            title=title,
            status=error.status_code,
            detail=str(error.detail) if error.detail else None,
            instance=instance,
            request_id=request_id
        )
    
    # Handle other exceptions with appropriate defaults
    else:
        problem = ProblemDetail(
            type="about:blank",
            title="Internal Server Error",
            status=500,
            detail="An unexpected error occurred",
            instance=instance,
            request_id=request_id
        )
    
    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(exclude_none=True),
        headers={"Content-Type": "application/problem+json"}
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
