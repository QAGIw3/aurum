"""Generic Celery tasks and registry for API offloading.

Pattern:

1) Register a callable job (name -> function) at API startup:

   from aurum.api.async_exec import register_job

   def heavy_compute(payload: dict) -> dict:
       # ... do work ...
       return {"ok": True, "input": payload}

   register_job("heavy_compute", heavy_compute)

2) In your route handler, enqueue and return 202:

   from aurum.api.async_exec import run_job_async
   task_id = run_job_async("heavy_compute", payload, queue="cpu_bound")
   return JSONResponse({"task_id": task_id}, status_code=202)

3) Expose a status endpoint that queries results:

   from aurum.api.async_exec import fetch_job_result
   status = fetch_job_result(task_id)
   return JSONResponse(status)
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional

from celery.result import AsyncResult

from .celery_app import get_celery_app


JOB_REGISTRY: Dict[str, Callable[[Dict[str, Any]], Any]] = {}


def register_job(name: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
    """Register a job callable by name.

    The callable must accept a single payload dict and return JSON-serializable output.
    """
    JOB_REGISTRY[name] = fn


def run_job_async(name: str, payload: Dict[str, Any], *, queue: Optional[str] = None) -> str:
    """Enqueue a registered job into Celery and return its task id."""
    app = get_celery_app()
    task_sig = run_registered_job.si(name=name, payload=payload)
    if queue:
        task_sig = task_sig.set(queue=queue)
    result = task_sig.apply_async()
    return result.id


def fetch_job_result(task_id: str) -> Dict[str, Any]:
    """Fetch job status/result for a given task id."""
    app = get_celery_app()
    res = AsyncResult(task_id, app=app)
    state = res.state
    info: Any
    try:
        info = res.result  # may be exception or return value
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


@get_celery_app().task(name="aurum.api.run_registered_job")
def run_registered_job(*, name: str, payload: Dict[str, Any]) -> Any:  # type: ignore[no-redef]
    """Celery task that dispatches to a registered job by name."""
    fn = JOB_REGISTRY.get(name)
    if fn is None:
        raise RuntimeError(f"No job registered with name '{name}'")
    return fn(payload)

