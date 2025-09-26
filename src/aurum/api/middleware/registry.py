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

# Reuse existing installers to preserve behavior
from aurum.api.app import (  # type: ignore
    _install_concurrency_middleware as _install_concurrency,
)
from aurum.api.app import (  # type: ignore
    _install_rate_limit_middleware as _install_rate_limit,
)


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


def apply_middleware_stack(app: FastAPI, settings: AurumSettings) -> ASGIApp:
    """Apply middleware in a well-defined order based on settings.

    Order:
    - CORS (outermost)
    - GZip
    - Concurrency/queueing
    - Sliding-window rate limiting (innermost of our custom wrappers)
    """

    # Outer wrappers change request/response headers early
    _wrap_cors(app, settings)
    _wrap_gzip(app, settings)

    # Concurrency and rate limiting wrap the app object itself
    wrapped: ASGIApp = app
    wrapped = _install_concurrency(wrapped, settings)
    wrapped = _install_rate_limit(wrapped, settings)
    return wrapped


__all__ = ["apply_middleware_stack"]

