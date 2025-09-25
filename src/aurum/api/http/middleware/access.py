"""Structured access logging middleware."""

from __future__ import annotations

import logging
import os
import time
import uuid
from typing import Any, Awaitable, Callable

try:  # pragma: no cover - optional dependency
    from opentelemetry import trace
except ImportError:  # pragma: no cover - optional dependency
    trace = None  # type: ignore[assignment]

from starlette.requests import Request
from starlette.responses import Response

from aurum.telemetry.context import (
    correlation_context,
    get_request_id,
    get_trace_span_ids,
    log_structured,
)

ACCESS_LOGGER = logging.getLogger("aurum.api.access")


async def access_log_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    """Emit structured access logs and propagate tracing headers."""

    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    correlation_id = request.headers.get("x-correlation-id") or request_id
    tenant_id = request.headers.get("x-aurum-tenant")
    user_id = request.headers.get("x-user-id")

    with correlation_context(
        correlation_id=correlation_id,
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=request_id,
    ) as context:
        request.state.request_id = context["request_id"]
        request.state.correlation_id = context["correlation_id"]
        request.state.tenant_id = context["tenant_id"]
        request.state.user_id = context["user_id"]
        request.state.session_id = context["session_id"]

        query_params: dict[str, Any] | None = (
            dict(request.query_params.multi_items()) if request.query_params else None
        )

        span = trace.get_current_span() if trace else None
        if span is not None and getattr(span, "is_recording", lambda: False)():
            span.set_attribute("aurum.request_id", context["request_id"])
            span.set_attribute("aurum.correlation_id", context["correlation_id"])
            span.set_attribute("aurum.tenant_id", context["tenant_id"])
            span.set_attribute("aurum.user_id", context["user_id"])
            span.set_attribute("http.request_id", context["request_id"])
            span.set_attribute("http.correlation_id", context["correlation_id"])

        start = time.perf_counter()

        try:
            response = await call_next(request)
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000.0
            principal = getattr(request.state, "principal", {}) or {}
            trace_id, span_id = get_trace_span_ids()
            if trace_id:
                request.state.trace_id = trace_id
            if span_id:
                request.state.span_id = span_id

            log_structured(
                "error",
                "http_request_failed",
                method=request.method,
                path=request.url.path,
                status=500,
                duration_ms=round(duration_ms, 2),
                client_ip=request.client.host if request.client else None,
                tenant=principal.get("tenant"),
                subject=principal.get("sub"),
                error_type=exc.__class__.__name__,
                error_message=str(exc),
                service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-api"),
                query_params=query_params,
                trace_id=trace_id,
                span_id=span_id,
            )
            raise

        duration_ms = (time.perf_counter() - start) * 1000.0
        principal = getattr(request.state, "principal", {}) or {}
        trace_id, span_id = get_trace_span_ids()
        if trace_id:
            request.state.trace_id = trace_id
        if span_id:
            request.state.span_id = span_id

        try:
            response.headers["X-Request-Id"] = context["request_id"]
            response.headers["X-Correlation-Id"] = context["correlation_id"]
            if context["tenant_id"]:
                response.headers["X-Aurum-Tenant"] = context["tenant_id"]
            if trace_id:
                response.headers["X-Trace-Id"] = trace_id
            if span_id:
                response.headers["X-Span-Id"] = span_id
        except Exception:  # pragma: no cover - defensive
            pass

        log_structured(
            "info",
            "http_request_completed",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration_ms, 2),
            client_ip=request.client.host if request.client else None,
            tenant=principal.get("tenant"),
            subject=principal.get("sub"),
            response_size=len(response.body) if hasattr(response, "body") else 0,
            user_agent=request.headers.get("user-agent", "unknown"),
            service_name=os.getenv("AURUM_OTEL_SERVICE_NAME", "aurum-api"),
            query_params=query_params,
            trace_id=trace_id,
            span_id=span_id,
        )

    return response
