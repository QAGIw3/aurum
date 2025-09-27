"""RFC7807 exception handling middleware."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional, Union

from fastapi import Request, HTTPException
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from pydantic import ValidationError

from ..models import ProblemDetail, ValidationErrorDetail
from ..exceptions import AurumAPIException
from ...telemetry.context import get_request_id

logger = logging.getLogger(__name__)


class RFC7807ExceptionMiddleware(BaseHTTPMiddleware):
    """Middleware to handle exceptions and return RFC7807 compliant responses.
    
    This middleware catches exceptions in async handlers and converts them to
    standardized Problem Details format according to RFC 7807.
    """

    def __init__(self, app, base_url: str = "https://api.aurum.com"):
        super().__init__(app)
        self.base_url = base_url.rstrip("/")

    async def dispatch(self, request: Request, call_next) -> Response:
        """Process request and handle any exceptions that occur."""
        try:
            response = await call_next(request)
            return response
        except Exception as exc:
            return await self._handle_exception(request, exc)

    async def _handle_exception(self, request: Request, exc: Exception) -> JSONResponse:
        """Convert exceptions to RFC7807 compliant responses."""
        request_id = get_request_id()
        instance = str(request.url)

        try:
            problem_detail = self._create_problem_detail(exc, request_id, instance)
        except Exception as conversion_error:
            # Fallback if problem detail creation fails
            logger.error(
                "Failed to create problem detail",
                exc_info=conversion_error,
                extra={"original_exception": str(exc), "request_id": request_id}
            )
            problem_detail = ProblemDetail(
                type="about:blank",
                title="Internal Server Error", 
                status=500,
                detail="An unexpected error occurred",
                instance=instance,
                request_id=request_id
            )

        # Log the exception for observability
        if problem_detail.status >= 500:
            logger.error(
                "Server error occurred",
                exc_info=exc,
                extra={
                    "request_id": request_id,
                    "problem_type": problem_detail.type,
                    "status_code": problem_detail.status
                }
            )
        elif problem_detail.status >= 400:
            logger.warning(
                "Client error occurred",
                extra={
                    "request_id": request_id,
                    "problem_type": problem_detail.type,
                    "status_code": problem_detail.status,
                    "detail": problem_detail.detail
                }
            )

        return JSONResponse(
            status_code=problem_detail.status,
            content=problem_detail.model_dump(exclude_none=True),
            headers={"Content-Type": "application/problem+json"}
        )

    def _create_problem_detail(
        self, 
        exc: Exception, 
        request_id: Optional[str], 
        instance: str
    ) -> ProblemDetail:
        """Create a ProblemDetail from an exception."""
        
        # Handle Aurum API exceptions
        if isinstance(exc, AurumAPIException):
            return self._handle_aurum_exception(exc, request_id, instance)
        
        # Handle FastAPI HTTP exceptions
        if isinstance(exc, HTTPException):
            return self._handle_http_exception(exc, request_id, instance)
        
        # Handle Pydantic validation errors
        if isinstance(exc, ValidationError):
            return self._handle_validation_error(exc, request_id, instance)
        
        # Handle async-related exceptions
        if isinstance(exc, asyncio.CancelledError):
            return ProblemDetail(
                type=f"{self.base_url}/problems/request-cancelled",
                title="Request Cancelled",
                status=499,
                detail="The request was cancelled before completion",
                instance=instance,
                request_id=request_id
            )
        
        if isinstance(exc, asyncio.TimeoutError):
            return ProblemDetail(
                type=f"{self.base_url}/problems/request-timeout",
                title="Request Timeout",
                status=504,
                detail="The request timed out",
                instance=instance,
                request_id=request_id
            )
        
        # Handle other common exceptions
        if isinstance(exc, ValueError):
            return ProblemDetail(
                type=f"{self.base_url}/problems/invalid-value",
                title="Invalid Value",
                status=400,
                detail=str(exc),
                instance=instance,
                request_id=request_id
            )
        
        if isinstance(exc, KeyError):
            return ProblemDetail(
                type=f"{self.base_url}/problems/missing-field",
                title="Missing Required Field",
                status=400,
                detail=f"Required field missing: {str(exc)}",
                instance=instance,
                request_id=request_id
            )
        
        # Default fallback for unknown exceptions
        return ProblemDetail(
            type="about:blank",
            title="Internal Server Error",
            status=500,
            detail="An unexpected error occurred",
            instance=instance,
            request_id=request_id
        )

    def _handle_aurum_exception(
        self, 
        exc: AurumAPIException, 
        request_id: Optional[str], 
        instance: str
    ) -> ProblemDetail:
        """Handle Aurum-specific exceptions."""
        
        # Map exception class to problem type
        type_mapping = {
            "ValidationException": "validation-error",
            "NotFoundException": "not-found",
            "ForbiddenException": "forbidden",
            "ServiceUnavailableException": "service-unavailable",
            "DataProcessingException": "data-processing-error"
        }
        
        exc_name = exc.__class__.__name__
        problem_type = type_mapping.get(exc_name, "api-error")
        
        return ProblemDetail(
            type=f"{self.base_url}/problems/{problem_type}",
            title=exc_name.replace("Exception", " Error").replace("", " ").title(),
            status=exc.status_code,
            detail=exc.detail,
            instance=instance,
            request_id=request_id or exc.request_id
        )

    def _handle_http_exception(
        self, 
        exc: HTTPException, 
        request_id: Optional[str], 
        instance: str
    ) -> ProblemDetail:
        """Handle FastAPI HTTP exceptions."""
        
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
        
        title = status_titles.get(exc.status_code, "HTTP Error")
        
        return ProblemDetail(
            type="about:blank",
            title=title,
            status=exc.status_code,
            detail=str(exc.detail) if exc.detail else None,
            instance=instance,
            request_id=request_id
        )

    def _handle_validation_error(
        self, 
        exc: ValidationError, 
        request_id: Optional[str], 
        instance: str
    ) -> ProblemDetail:
        """Handle Pydantic validation errors."""
        
        errors = []
        for error in exc.errors():
            field_path = ".".join(str(loc) for loc in error.get("loc", []))
            errors.append(ValidationErrorDetail(
                field=field_path,
                message=error.get("msg", "Validation error"),
                value=error.get("input"),
                code=error.get("type")
            ))
        
        return ProblemDetail(
            type=f"{self.base_url}/problems/validation-error",
            title="Validation Error",
            status=422,
            detail="Request validation failed",
            instance=instance,
            request_id=request_id,
            errors=errors
        )


__all__ = ["RFC7807ExceptionMiddleware"]