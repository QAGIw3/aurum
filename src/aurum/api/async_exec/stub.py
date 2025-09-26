"""Development stub for async execution when Celery is unavailable.

The real async execution layer depends on Celery. During local development or
unit testing we do not want to require a broker, so this stub mimics the public
interface with in-memory bookkeeping and generates synthetic task identifiers.
"""
from __future__ import annotations

import itertools
import uuid
from typing import Any, Callable, Dict, Optional

JOB_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Any]] = {}
_TASK_RESULTS: Dict[str, Dict[str, Any]] = {}
_counter = itertools.count(1)


def get_celery_app() -> None:  # pragma: no cover - only used when Celery missing
    """Stub hook to satisfy callers that expect `get_celery_app`.

    The real Celery app is unavailable; callers should treat this as a no-op.
    """
    return None


def register_job(name: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
    JOB_REGISTRY[name] = fn


def run_job_async(name: str, payload: Dict[str, Any], *, queue: Optional[str] = None) -> str:
    """Return a synthetic task id without performing any remote work."""
    task_id = f"dev-{next(_counter)}-{uuid.uuid4().hex}"
    _TASK_RESULTS[task_id] = {
        "task_id": task_id,
        "state": "PENDING",
        "queue": queue or "default",
        "job": name,
        "payload": payload,
    }
    return task_id


def fetch_job_result(task_id: str) -> Dict[str, Any]:
    """Return a fake result payload for stubbed jobs."""
    return _TASK_RESULTS.get(task_id, {"task_id": task_id, "state": "PENDING"})


__all__ = [
    "JOB_REGISTRY",
    "get_celery_app",
    "register_job",
    "run_job_async",
    "fetch_job_result",
]
