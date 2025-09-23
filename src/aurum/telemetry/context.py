"""Shared context helpers for request-scoped identifiers and structured logging."""
from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import datetime
from typing import Iterator, Optional, Dict, Any, Mapping, Iterable

try:  # pragma: no cover - optional dependency
    from opentelemetry import trace as _otel_trace  # type: ignore
except ImportError:  # pragma: no cover - tracing optional
    _otel_trace = None

_SENSITIVE_KEYS = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "x-api-key",
    "x-aurum-api-key",
    "access_token",
    "refresh_token",
    "id_token",
    "token",
    "password",
    "secret",
    "client_secret",
    "session",
    "session_id",
}
_REDACTED = "[REDACTED]"

_REQUEST_ID: ContextVar[Optional[str]] = ContextVar("aurum_request_id", default=None)
_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar("aurum_correlation_id", default=None)
_TENANT_ID: ContextVar[Optional[str]] = ContextVar("aurum_tenant_id", default=None)
_USER_ID: ContextVar[Optional[str]] = ContextVar("aurum_user_id", default=None)
_SESSION_ID: ContextVar[Optional[str]] = ContextVar("aurum_session_id", default=None)

# Logger for structured logging
STRUCTURED_LOGGER = logging.getLogger("aurum.structured")


def set_request_id(request_id: str) -> Token:
    """Bind a request identifier to the current context and return the token."""
    return _REQUEST_ID.set(request_id)


def get_request_id(default: Optional[str] = None) -> Optional[str]:
    """Return the current request identifier if one has been set."""
    return _REQUEST_ID.get(default)


def reset_request_id(token: Token) -> None:
    """Restore the request identifier context to a previous state."""
    _REQUEST_ID.reset(token)


def set_correlation_id(correlation_id: str) -> Token:
    """Bind a correlation identifier to the current context and return the token."""
    return _CORRELATION_ID.set(correlation_id)


def get_correlation_id(default: Optional[str] = None) -> Optional[str]:
    """Return the current correlation identifier if one has been set."""
    return _CORRELATION_ID.get(default)


def reset_correlation_id(token: Token) -> None:
    """Restore the correlation identifier context to a previous state."""
    _CORRELATION_ID.reset(token)


def set_tenant_id(tenant_id: str) -> Token:
    """Bind a tenant identifier to the current context and return the token."""
    return _TENANT_ID.set(tenant_id)


def get_tenant_id(default: Optional[str] = None) -> Optional[str]:
    """Return the current tenant identifier if one has been set."""
    return _TENANT_ID.get(default)


def reset_tenant_id(token: Token) -> None:
    """Restore the tenant identifier context to a previous state."""
    _TENANT_ID.reset(token)


def set_user_id(user_id: str) -> Token:
    """Bind a user identifier to the current context and return the token."""
    return _USER_ID.set(user_id)


def get_user_id(default: Optional[str] = None) -> Optional[str]:
    """Return the current user identifier if one has been set."""
    return _USER_ID.get(default)


def reset_user_id(token: Token) -> None:
    """Restore the user identifier context to a previous state."""
    _USER_ID.reset(token)


def set_session_id(session_id: str) -> Token:
    """Bind a session identifier to the current context and return the token."""
    return _SESSION_ID.set(session_id)


def get_session_id(default: Optional[str] = None) -> Optional[str]:
    """Return the current session identifier if one has been set."""
    return _SESSION_ID.get(default)


def reset_session_id(token: Token) -> None:
    """Restore the session identifier context to a previous state."""
    _SESSION_ID.reset(token)


def get_context() -> Dict[str, Optional[str]]:
    """Get all context variables as a dictionary."""
    return {
        "request_id": get_request_id(),
        "correlation_id": get_correlation_id(),
        "tenant_id": get_tenant_id(),
        "user_id": get_user_id(),
        "session_id": get_session_id(),
    }


@contextmanager
def request_id_context(request_id: str) -> Iterator[None]:
    """Context manager that temporarily sets the request identifier."""
    token = set_request_id(request_id)
    try:
        yield
    finally:
        reset_request_id(token)


@contextmanager
def correlation_context(
    correlation_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Iterator[Dict[str, Optional[str]]]:
    """Temporarily bind correlation, tenant, user, and session identifiers.

    Ensures that each context variable is reset to its previous value on exit,
    even if an exception occurs inside the context manager.
    """
    import uuid

    correlation_id = correlation_id or str(uuid.uuid4())

    # Capture tokens along with their corresponding reset functions
    tokens: list[tuple[callable, Token]] = []
    tokens.append((reset_correlation_id, set_correlation_id(correlation_id)))
    tokens.append((reset_tenant_id, set_tenant_id(tenant_id or "unknown")))
    tokens.append((reset_user_id, set_user_id(user_id or "unknown")))
    tokens.append((reset_session_id, set_session_id(session_id or correlation_id)))

    try:
        yield get_context()
    finally:
        # Reset in reverse order of setting
        for reset_fn, token in reversed(tokens):
            try:
                reset_fn(token)
            except Exception:
                # Never raise from cleanup
                pass


def redact_sensitive_data(value: Any) -> Any:
    """Recursively redact sensitive keys inside mappings and sequences."""

    if isinstance(value, Mapping):
        redacted: Dict[Any, Any] = {}
        for key, item in value.items():
            if isinstance(key, str) and key.lower() in _SENSITIVE_KEYS:
                redacted[key] = _REDACTED
            else:
                redacted[key] = redact_sensitive_data(item)
        return redacted
    if isinstance(value, list):
        return [redact_sensitive_data(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact_sensitive_data(item) for item in value)
    if isinstance(value, set):
        return {redact_sensitive_data(item) for item in value}
    return value


def get_trace_span_ids() -> tuple[Optional[str], Optional[str]]:
    """Return the current trace and span identifiers if OpenTelemetry is active."""

    if _otel_trace is None:  # pragma: no cover - tracing optional
        return None, None
    try:
        span = _otel_trace.get_current_span()
    except Exception:  # pragma: no cover - defensive for instrumentation edge cases
        return None, None
    if span is None:
        return None, None

    try:
        context = span.get_span_context()
    except AttributeError:  # pragma: no cover - missing API support
        return None, None
    if context is None:
        return None, None

    try:
        if not context.is_valid:
            return None, None
        trace_id = f"{context.trace_id:032x}"
        span_id = f"{context.span_id:016x}"
        return trace_id, span_id
    except Exception:  # pragma: no cover - guard against unexpected attr access
        return None, None


def log_structured(
    level: str,
    event: str,
    **kwargs: Any
) -> None:
    """Log a structured message with context information."""
    context = get_context()
    sanitized_kwargs = redact_sensitive_data(dict(kwargs)) if kwargs else {}
    trace_id, span_id = get_trace_span_ids()

    log_data = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "event": event,
        "request_id": context["request_id"],
        "correlation_id": context["correlation_id"],
        "tenant_id": context["tenant_id"],
        "user_id": context["user_id"],
        "session_id": context["session_id"],
        **sanitized_kwargs,
    }

    if trace_id:
        log_data["trace_id"] = trace_id
    if span_id:
        log_data["span_id"] = span_id

    # Filter out None values
    log_data = {k: v for k, v in log_data.items() if v is not None}

    message = json.dumps(log_data, default=str, separators=(',', ':'))

    if level == "debug":
        STRUCTURED_LOGGER.debug(message)
    elif level == "info":
        STRUCTURED_LOGGER.info(message)
    elif level == "warning":
        STRUCTURED_LOGGER.warning(message)
    elif level == "error":
        STRUCTURED_LOGGER.error(message)
    elif level == "critical":
        STRUCTURED_LOGGER.critical(message)
    else:
        STRUCTURED_LOGGER.info(message)
