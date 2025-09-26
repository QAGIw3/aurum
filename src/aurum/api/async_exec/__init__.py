"""Celery-based async execution helpers for API offloading.

These helpers let API routes enqueue long-running work onto Celery workers
and immediately return a 202 Accepted + task id. Use this to keep API
latency low while handling heavy computations out-of-band.
"""
from __future__ import annotations

try:  # pragma: no cover - exercised implicitly via import
    from .celery_app import get_celery_app  # type: ignore
    from .tasks import (  # type: ignore
        JOB_REGISTRY,
        register_job,
        run_job_async,
        fetch_job_result,
    )
except Exception:  # pragma: no cover - when Celery is unavailable
    from .stub import (  # type: ignore
        JOB_REGISTRY,
        get_celery_app,
        register_job,
        run_job_async,
        fetch_job_result,
    )

__all__ = [
    "get_celery_app",
    "JOB_REGISTRY",
    "register_job",
    "run_job_async",
    "fetch_job_result",
]
