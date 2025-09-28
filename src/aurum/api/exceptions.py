"""Standardized exception handling for the API."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
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


class BadRequestException(AurumAPIException):
    """Generic 400 Bad Request exception for client input errors."""

    def __init__(
        self,
        detail: str = "Bad request",
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            status_code=400,
            detail=detail,
            request_id=request_id,
            context=context,
        )


class QueryParameterValidationException(BadRequestException):
    """Exception raised for invalid query parameter values."""

    def __init__(
        self,
        parameter: str,
        message: str,
        request_id: Optional[str] = None,
        value: Any | None = None,
    ):
        context: Dict[str, Any] = {"parameter": parameter}
        if value is not None:
            context["value"] = value
        super().__init__(
            detail=message,
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


class NotImplementedException(AurumAPIException):
    """Exception raised when an endpoint is not implemented (501)."""

    def __init__(
        self,
        detail: str = "Not implemented",
        request_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            status_code=501,
            detail=detail,
            request_id=request_id,
            context=context,
        )


class CurveNotFoundException(NotFoundException):
    """Exception raised when a curve cannot be found."""

    def __init__(
        self,
        curve_key: str,
        request_id: Optional[str] = None,
    ):
        super().__init__(
            resource_type="Curve",
            resource_id=curve_key,
            request_id=request_id,
            context={"curve_key": curve_key},
        )


class ScenarioNotFoundException(NotFoundException):
    """Exception raised when a scenario cannot be found."""

    def __init__(
        self,
        scenario_id: str,
        request_id: Optional[str] = None,
    ):
        super().__init__(
            resource_type="Scenario",
            resource_id=scenario_id,
            request_id=request_id,
            context={"scenario_id": scenario_id},
        )


class InsufficientDataException(AurumAPIException):
    """Exception raised when insufficient data is available for the requested operation."""

    def __init__(
        self,
        detail: str,
        request_id: Optional[str] = None,
        required_data: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            status_code=422,
            detail=detail,
            request_id=request_id,
            context=context or {"required_data": required_data},
        )


class RateLimitExceededException(AurumAPIException):
    """Exception raised when rate limits are exceeded."""

    def __init__(
        self,
        detail: str,
        request_id: Optional[str] = None,
        retry_after: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            status_code=429,
            detail=detail,
            request_id=request_id,
            context=context or {"retry_after": retry_after},
        )


class CacheUnavailableException(ServiceUnavailableException):
    """Exception raised when cache is unavailable."""

    def __init__(
        self,
        service: str = "cache",
        request_id: Optional[str] = None,
    ):
        super().__init__(
            detail=f"Cache service '{service}' is unavailable",
            request_id=request_id,
            service=service,
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
    from ..telemetry.context import get_request_id, get_correlation_id
    
    request_id = get_request_id()
    correlation_id = get_correlation_id()
    instance = str(request.url)
    base_url = base_url.rstrip("/")
    
    # Handle Aurum API exceptions
    if isinstance(error, AurumAPIException):
        type_mapping = {
            "ValidationException": "validation-error",
            "BadRequestException": "bad-request",
            "QueryParameterValidationException": "invalid-parameter",
            "NotFoundException": "not-found", 
            "ForbiddenException": "forbidden",
            "ServiceUnavailableException": "service-unavailable",
            "RateLimitExceededException": "too-many-requests",
            "DataProcessingException": "data-processing-error",
            "NotImplementedException": "not-implemented"
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
    
    # Handle FastAPI Request validation errors
    elif isinstance(error, RequestValidationError):
        errors = []
        for err in error.errors():
            field = ".".join(str(loc) for loc in err.get("loc", []))
            errors.append(ValidationErrorDetail(
                field=field,
                message=err.get("msg", "Validation error"),
                value=err.get("input"),
                code=err.get("type"),
            ))

        problem = ProblemDetail(
            type=f"{base_url}/problems/validation-error",
            title="Validation Error",
            status=422,
            detail="Request validation failed",
            instance=instance,
            request_id=request_id,
            errors=errors,
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
    
    headers: Dict[str, str] = {"Content-Type": "application/problem+json"}
    if request_id:
        headers["X-Request-Id"] = request_id
    if correlation_id:
        headers["X-Correlation-Id"] = correlation_id

    # Propagate Retry-After header when present in context of known exceptions
    retry_after: Optional[int] = None
    if isinstance(error, AurumAPIException):
        # Our convention: retry_after may be in context
        try:
            retry_after = int(error.context.get("retry_after")) if error.context else None
        except Exception:
            retry_after = None
    if problem.status in (429, 503, 504) and retry_after is not None:
        headers["Retry-After"] = str(retry_after)

    return JSONResponse(
        status_code=problem.status,
        content=problem.model_dump(exclude_none=True),
        headers=headers,
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
