"""Security middleware for comprehensive request protection and audit logging."""

from __future__ import annotations

import time
from typing import Any, List, Optional

from fastapi import HTTPException, Request, Response
from fastapi.security import HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from ..telemetry.context import get_request_id, log_structured, request_id_context
from .audit import log_security_event
from .auth import AuthorizationManager, get_auth_manager
from .validation import SecurityHeaders, validate_and_sanitize_query_params


class SecurityMiddleware(BaseHTTPMiddleware):
    """Comprehensive security middleware with authentication, authorization, and audit logging."""

    def __init__(
        self,
        app: ASGIApp,
        auth_manager: Optional[AuthorizationManager] = None,
        exclude_paths: Optional[List[str]] = None,
        security_headers: bool = True,
        audit_suspicious_activity: bool = True,
    ):
        super().__init__(app)
        self.auth_manager = auth_manager or get_auth_manager()
        self.exclude_paths = exclude_paths or ["/health", "/ready", "/metrics", "/docs", "/openapi.json"]
        self.security_headers_enabled = security_headers
        self.audit_suspicious_enabled = audit_suspicious_activity

    async def dispatch(self, request: Request, call_next):
        """Process request with security checks."""
        start_time = time.perf_counter()
        request_id = get_request_id() or "unknown"

        # Extract client information
        client_ip = self._get_client_ip(request)
        user_agent = request.headers.get("user-agent")
        method = request.method
        path = request.url.path

        # Check if path should be excluded from security checks
        if path in self.exclude_paths:
            return await call_next(request)

        with request_id_context(request_id):
            try:
                # Security headers validation
                if self.security_headers_enabled:
                    await self._validate_security_headers(request)

                # Query parameter sanitization
                await self._sanitize_query_params(request)

                # Authentication
                user_context = await self._authenticate_request(request)

                # Authorization
                await self._authorize_request(request, user_context)

                # Set security context for downstream processing
                if user_context:
                    # This would set the user context in the request state
                    request.state.user_context = user_context

                # Process request
                response = await call_next(request)

                # Audit successful request
                await self._audit_request(
                    request=request,
                    user_context=user_context,
                    response=response,
                    duration=time.perf_counter() - start_time,
                    status_code=response.status_code,
                    success=True
                )

                # Add security headers to response
                if self.security_headers_enabled:
                    self._add_security_headers(response)

                return response

            except HTTPException:
                # Re-raise HTTP exceptions (they're handled by FastAPI)
                raise
            except Exception as e:
                # Audit failed request
                await self._audit_request(
                    request=request,
                    user_context=None,
                    response=None,
                    duration=time.perf_counter() - start_time,
                    status_code=500,
                    success=False,
                    error=str(e)
                )
                # Re-raise the exception
                raise

    async def _authenticate_request(self, request: Request) -> Optional[Any]:
        """Authenticate request using the authorization manager."""
        # Extract authorization header
        auth_header = request.headers.get("authorization")
        credentials = None

        if auth_header and auth_header.startswith("Bearer "):
            credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=auth_header[7:])

        # Use auth manager to authenticate
        return await self.auth_manager.authenticate_request(request, credentials)

    async def _authorize_request(self, request: Request, user_context: Optional[Any]) -> None:
        """Authorize request using the authorization manager."""
        path = request.url.path
        method = request.method

        # Use auth manager to authorize
        self.auth_manager.authorize_request(user_context, path, method)

    async def _validate_security_headers(self, request: Request) -> None:
        """Validate security-related headers."""
        headers = SecurityHeaders(
            user_agent=request.headers.get("user-agent"),
            referer=request.headers.get("referer"),
            origin=request.headers.get("origin"),
            x_forwarded_for=request.headers.get("x-forwarded-for"),
            x_real_ip=request.headers.get("x-real-ip")
        )

        # Validate headers - this will raise HTTPException if invalid
        try:
            # The SecurityHeaders model validation will handle this
            headers.dict()
        except Exception as e:
            log_security_event(
                event_type="suspicious_headers",
                ip_address=self._get_client_ip(request),
                user_agent=request.headers.get("user-agent"),
                details={"error": str(e), "headers": dict(request.headers)},
                severity="warning"
            )
            # For now, we don't block on header validation, just audit
            pass

    async def _sanitize_query_params(self, request: Request) -> None:
        """Sanitize query parameters to prevent injection attacks."""
        try:
            # Get query parameters
            query_params = dict(request.query_params)

            # Validate and sanitize
            sanitized_params = validate_and_sanitize_query_params(query_params)

            # Update request query params if they were modified
            if sanitized_params != query_params:
                # This would require modifying the request object
                # For now, we just log the issue
                log_structured(
                    "warning",
                    "sanitized_query_params",
                    original_params=query_params,
                    sanitized_params=sanitized_params,
                    path=request.url.path
                )

        except Exception as e:
            # If sanitization fails, log and potentially block
            log_security_event(
                event_type="query_param_validation_failed",
                ip_address=self._get_client_ip(request),
                user_agent=request.headers.get("user-agent"),
                details={"error": str(e), "params": dict(request.query_params)},
                severity="error"
            )
            # For now, we don't block, just audit
            pass

    async def _audit_request(
        self,
        request: Request,
        user_context: Optional[Any],
        response: Optional[Response],
        duration: float,
        status_code: int,
        success: bool,
        error: Optional[str] = None
    ) -> None:
        """Audit request for security monitoring."""
        # Only audit suspicious or important requests
        if not self.audit_suspicious_enabled and success:
            return

        # Check if request should be audited
        if not self._should_audit_request(request.url.path, status_code, success):
            return

        event_type = "request_success" if success else "request_error"

        details = {
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "duration_ms": round(duration * 1000, 2),
            "status_code": status_code,
            "user_agent": request.headers.get("user-agent"),
        }

        if error:
            details["error"] = error

        if user_context:
            details["user_id"] = user_context.user_id
            details["tenant_id"] = user_context.tenant_id

        log_security_event(
            event_type=event_type,
            user_id=user_context.user_id if user_context else None,
            tenant_id=user_context.tenant_id if user_context else None,
            resource=request.url.path,
            action=request.method,
            ip_address=self._get_client_ip(request),
            user_agent=request.headers.get("user-agent"),
            details=details,
            severity="info" if success else "error"
        )

    def _should_audit_request(self, path: str, status_code: int, success: bool) -> bool:
        """Determine if a request should be audited."""
        # Always audit errors
        if not success:
            return True

        # Audit specific status codes
        if status_code in [401, 403, 404, 429]:
            return True

        # Audit sensitive paths
        sensitive_paths = ["/admin", "/api/v2/admin", "/users", "/config"]
        if any(path.startswith(sensitive) for sensitive in sensitive_paths):
            return True

        # Audit requests with unusual patterns
        # This could be extended with more sophisticated detection
        return False

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP address from request."""
        # Check X-Forwarded-For header first (for proxies)
        forwarded_for = request.headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("x-real-ip")
        if real_ip:
            return real_ip

        # Fall back to client host
        return request.client.host if request.client else "unknown"

    def _add_security_headers(self, response: Response) -> None:
        """Add security headers to response."""
        # Security headers
        security_headers = {
            "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY",
            "X-XSS-Protection": "1; mode=block",
            "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
            "Referrer-Policy": "strict-origin-when-cross-origin",
            "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
        }

        for header, value in security_headers.items():
            if header not in response.headers:
                response.headers[header] = value


class SecurityContextMiddleware(BaseHTTPMiddleware):
    """Middleware to establish security context for requests."""

    def __init__(self, app: ASGIApp):
        super().__init__(app)

    async def dispatch(self, request: Request, call_next):
        """Establish security context for the request."""
        # Extract request ID from headers or generate one
        request_id = request.headers.get("x-request-id") or "unknown"

        # Extract tenant ID from headers
        tenant_id = request.headers.get("x-aurum-tenant")

        # Extract correlation ID
        correlation_id = request.headers.get("x-correlation-id")

        # Set up context for the request
        context_data = {}
        if request_id:
            context_data["request_id"] = request_id
        if tenant_id:
            context_data["tenant_id"] = tenant_id
        if correlation_id:
            context_data["correlation_id"] = correlation_id

        # Log request start with context
        log_structured(
            "info",
            "request_started",
            method=request.method,
            path=request.url.path,
            client_ip=request.client.host if request.client else "unknown",
            user_agent=request.headers.get("user-agent"),
            **context_data
        )

        # Process request
        response = await call_next(request)

        # Log request completion
        log_structured(
            "info",
            "request_completed",
            method=request.method,
            path=request.url.path,
            status_code=response.status_code,
            **context_data
        )

        return response
