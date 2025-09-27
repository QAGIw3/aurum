from __future__ import annotations

"""Config-driven middleware registry for the Aurum API.

This module centralizes assembly of the middleware stack so ordering and
enablement can be controlled via settings. It mirrors the router registry
pattern and wraps the FastAPI/Starlette app in the selected middlewares.
"""

from typing import Any, Callable, Optional

from fastapi import FastAPI
from starlette.types import ASGIApp

from aurum.core import AurumSettings

from .rfc7807 import RFC7807ExceptionMiddleware


MiddlewareFactory = Callable[[ASGIApp, AurumSettings], ASGIApp]


def _wrap_gzip(app: FastAPI, settings: AurumSettings) -> None:
    from fastapi.middleware.gzip import GZipMiddleware

    gzip_min = int(getattr(settings.api, "gzip_min_bytes", 0) or 0)
    if gzip_min > 0:
        app.add_middleware(GZipMiddleware, minimum_size=gzip_min)


def _wrap_cors(app: FastAPI, settings: AurumSettings) -> None:
    from fastapi.middleware.cors import CORSMiddleware

    cors_origins = getattr(settings.api, "cors_origins", []) or []
    allow_origins = cors_origins if cors_origins else ["*"]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


def _wrap_rfc7807_exceptions(app: FastAPI, settings: AurumSettings) -> None:
    """Add RFC7807 compliant exception handling middleware."""
    base_url = getattr(settings.api, "base_url", "https://api.aurum.com")
    app.add_middleware(RFC7807ExceptionMiddleware, base_url=base_url)


def apply_middleware_stack(app: FastAPI, settings: AurumSettings) -> ASGIApp:
    """Apply middleware in a well-defined order based on settings.

    Order:
    - CORS (outermost)
    - GZip
    - RFC7807 Exception handling (critical for consistent error responses)
    - Concurrency/queueing (handled separately in app.py to avoid circular imports)
    - Sliding-window rate limiting (handled separately in app.py to avoid circular imports)
    """

    # Outer wrappers change request/response headers early
    _wrap_cors(app, settings)
    _wrap_gzip(app, settings)
    
    # RFC7807 exception handling should be early in the stack
    # to catch exceptions from all other middleware
    _wrap_rfc7807_exceptions(app, settings)

    # Note: Concurrency and rate limiting are handled separately in app.py
    # to avoid circular import issues with the middleware registry
    return app


__all__ = ["apply_middleware_stack"]

