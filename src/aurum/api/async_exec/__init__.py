"""Celery-based async execution helpers for API offloading.

These helpers let API routes enqueue long-running work onto Celery workers
and immediately return a 202 Accepted + task id. Use this to keep API
latency low while handling heavy computations out-of-band.

When Celery is unavailable (or explicitly disabled via environment flags) the
module exposes an in-memory stub that mimics the public API so local
development remains productive without external infrastructure.
"""
from __future__ import annotations

import os
from typing import Final


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


_force_stub = _env_flag("AURUM_API_OFFLOAD_USE_STUB", default=False)
_offload_enabled = _env_flag("AURUM_API_OFFLOAD_ENABLED", default=True)
_use_stub = _force_stub or not _offload_enabled

if not _use_stub:
    try:  # pragma: no cover - exercised implicitly via import
        from .celery_app import get_celery_app  # type: ignore
        from .tasks import (  # type: ignore
            JOB_REGISTRY,
            register_job,
            run_job_async,
            fetch_job_result,
        )
    except Exception:  # pragma: no cover - fall back when Celery unavailable
        _use_stub = True

if _use_stub:
    from .stub import (  # type: ignore
        JOB_REGISTRY,
        get_celery_app,
        register_job,
        run_job_async,
        fetch_job_result,
    )


OFFLOAD_ENABLED: Final[bool] = not _use_stub


__all__ = [
    "get_celery_app",
    "JOB_REGISTRY",
    "register_job",
    "run_job_async",
    "fetch_job_result",
    "OFFLOAD_ENABLED",
]
