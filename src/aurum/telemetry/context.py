"""Shared context helpers for request-scoped identifiers and structured logging."""
from __future__ import annotations

import json
import logging
import sys
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import datetime
from typing import Iterator, Optional, Dict, Any

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


@contextmanager
def request_id_context(request_id: str) -> Iterator[None]:
    """Context manager that temporarily sets the request identifier."""
    token = set_request_id(request_id)
    try:
        yield
    finally:
        reset_request_id(token)
