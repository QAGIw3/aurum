"""Backwards-compatibility shim for async execution helpers.

The authoritative implementation now lives in `libs.services.async_exec`.
This module re-exports the same public API to avoid breaking imports.
"""
from __future__ import annotations

import os
from aurum.libs.services.async_exec import (  # type: ignore F401
    JOB_REGISTRY,
    register_job,
    run_job_async,
    fetch_job_result,
)

try:  # Provide get_celery_app for code paths that need it
    from apps.workers.main import get_celery_app  # type: ignore F401
except Exception:  # pragma: no cover
    from .celery_app import get_celery_app  # type: ignore F401

__all__ = [
    "JOB_REGISTRY",
    "register_job",
    "run_job_async",
    "fetch_job_result",
    "get_celery_app",
]
