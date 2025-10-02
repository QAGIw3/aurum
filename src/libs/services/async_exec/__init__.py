"""Shared async execution helpers (Celery + stub) for API and workers.

Public API:
- register_job(name, fn)
- run_job_async(name, payload, queue=None) -> task_id
- fetch_job_result(task_id) -> status dict
"""

from __future__ import annotations

import os
from typing import Any, Callable, Dict, Optional

from celery.result import AsyncResult
import importlib
from types import ModuleType

_get_central_app = None  # set lazily via importlib to avoid static import across boundaries


def _env_flag(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


# Normalized envs (preferred)
_force_stub = _env_flag("AURUM_CELERY_USE_STUB", default=False)
_offload_enabled = _env_flag("AURUM_CELERY_ENABLED", default=True)

# Backwards-compatibility with deprecated names
if not _force_stub:
    _force_stub = _env_flag("AURUM_API_OFFLOAD_USE_STUB", default=False)
if _offload_enabled:
    # If explicitly disabled via old flag, honor it
    if not _env_flag("AURUM_API_OFFLOAD_ENABLED", default=True):
        _offload_enabled = False
_use_stub = _force_stub or not _offload_enabled


JOB_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_job(name: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
    JOB_REGISTRY[name] = fn


def _get_celery_app():
    global _get_central_app
    if _get_central_app is None:
        # Lazy, runtime-only import to avoid static boundary violation
        try:
            module: ModuleType = importlib.import_module("apps.workers.main")
            _get_central_app = getattr(module, "get_celery_app", None)
        except Exception:
            _get_central_app = None
    if _get_central_app is not None:
        try:
            return _get_central_app()
        except Exception:
            pass
    # Fallback to old shim for compatibility
    from aurum.api.async_exec.celery_app import get_celery_app as _shim

    return _shim()


def run_job_async(name: str, payload: Dict[str, Any], *, queue: Optional[str] = None) -> str:
    if _use_stub:
        # Deterministic stub id
        return f"stub-{name}-0000"
    app = _get_celery_app()
    task_sig = run_registered_job.si(name=name, payload=payload)
    if queue:
        task_sig = task_sig.set(queue=queue)
    result = task_sig.apply_async(app=app)
    return result.id


def fetch_job_result(task_id: str) -> Dict[str, Any]:
    if _use_stub:
        return {"task_id": task_id, "state": "PENDING", "detail": None}
    app = _get_celery_app()
    res = AsyncResult(task_id, app=app)
    state = res.state
    try:
        info = res.result
    except Exception as exc:  # pragma: no cover
        info = {"error": str(exc)}
    response: Dict[str, Any] = {"task_id": task_id, "state": state}
    if state == "SUCCESS":
        response["result"] = info
    elif state in {"FAILURE", "REVOKED"}:
        response["error"] = str(info)
    else:
        response["detail"] = str(info) if info else None
    return response


# Celery task wrapper; defined late to avoid import cycles
def _bind_task():  # pragma: no cover - bound at import time
    try:
        app = _get_celery_app()

        @app.task(name="aurum.run_registered_job")
        def run_registered_job(*, name: str, payload: Dict[str, Any]) -> Any:  # type: ignore[no-redef]
            fn = JOB_REGISTRY.get(name)
            if fn is None:
                raise RuntimeError(f"No job registered with name '{name}'")
            return fn(payload)

        return run_registered_job
    except Exception:
        # In stub mode there is no Celery app to bind
        def run_registered_job(*, name: str, payload: Dict[str, Any]) -> Any:  # type: ignore[no-redef]
            fn = JOB_REGISTRY.get(name)
            if fn is None:
                raise RuntimeError(f"No job registered with name '{name}'")
            return fn(payload)

        return run_registered_job


run_registered_job = _bind_task()


__all__ = [
    "JOB_REGISTRY",
    "register_job",
    "run_job_async",
    "fetch_job_result",
]


