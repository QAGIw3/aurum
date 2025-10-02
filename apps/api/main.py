"""Deprecated entrypoint.

Use `aurum.api.app:create_app` instead. This module delegates to the unified
factory to preserve backwards compatibility.
"""
from __future__ import annotations

import logging
from fastapi import FastAPI

logger = logging.getLogger(__name__)


def create_app(*args, **kwargs) -> FastAPI:  # type: ignore[override]
    """Deprecated: delegate to `aurum.api.app:create_app`.

    Kept for backward compatibility with older launchers.
    """
    logger.warning("apps.api.main.create_app is deprecated; use aurum.api.app:create_app")
    from aurum.api.app import create_app as unified_create_app

    return unified_create_app(*args, **kwargs)