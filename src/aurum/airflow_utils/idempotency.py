"""Helpers for building idempotent Airflow tasks.

The helpers in this module intentionally avoid heavy Airflow imports so they
can be exercised in unit tests without an Airflow installation. When running
inside Airflow they fall back to XCom for lightweight state management.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

try:  # pragma: no cover - Airflow may not be installed in unit test envs
    from airflow.models.taskinstance import TaskInstance  # type: ignore
except Exception:  # pragma: no cover - keep type hints friendly
    TaskInstance = Any  # type: ignore


__all__ = [
    "IdempotentExecution",
    "idempotent_run",
    "get_idempotent_marker",
]


_MARKER_PREFIX = "idempotent::"


@dataclass(frozen=True)
class IdempotentExecution:
    """Result metadata describing whether a callable executed or was skipped."""

    executed: bool
    result: Any = None
    marker: Optional[Dict[str, Any]] = None


def _resolve_task_instance(context: Optional[Dict[str, Any]]) -> Optional[TaskInstance]:
    """Best-effort extraction of the TaskInstance from an Airflow context."""

    if not context:
        return None
    ti = context.get("ti") or context.get("task_instance")
    if ti is None:
        return None
    # Defensive import to avoid hard Airflow dependency when not available
    try:
        from airflow.models.taskinstance import TaskInstance as _TI  # type: ignore

        if isinstance(ti, _TI):
            return ti
    except Exception:  # pragma: no cover - tolerant when Airflow missing
        return None
    return None


def get_idempotent_marker(
    *,
    context: Optional[Dict[str, Any]],
    operation_key: str,
    task_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return the marker dict pushed by ``idempotent_run`` if it exists."""

    ti = _resolve_task_instance(context)
    if not ti:
        return None

    marker_key = _MARKER_PREFIX + operation_key
    lookup_task_id = task_id or ti.task_id
    try:
        marker = ti.xcom_pull(task_ids=lookup_task_id, key=marker_key)
    except Exception:
        marker = None
    if isinstance(marker, dict) and marker.get("status") == "completed":
        return marker
    return None


def idempotent_run(
    *,
    context: Optional[Dict[str, Any]],
    operation_key: str,
    callable_: Callable[[], Any],
    cache_result: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
) -> IdempotentExecution:
    """Execute ``callable_`` only once per DAG run + ``operation_key``.

    When an Airflow TaskInstance is available, the helper stores an XCom marker
    using the composite key ``idempotent::<operation_key>``. Subsequent calls in
    the same DAG run return the original result (when ``cache_result`` is True)
    without re-executing the callable.

    Outside Airflow (e.g. unit tests) the callable always runs.
    """

    marker = get_idempotent_marker(context=context, operation_key=operation_key)
    if marker is not None:
        cached_result = marker.get("result") if cache_result else marker.get("result")
        return IdempotentExecution(executed=False, result=cached_result, marker=marker)

    result = callable_()

    ti = _resolve_task_instance(context)
    if ti:
        marker_payload: Dict[str, Any] = {
            "status": "completed",
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }
        if metadata:
            marker_payload["metadata"] = dict(metadata)
        if cache_result:
            marker_payload["result"] = result

        marker_key = _MARKER_PREFIX + operation_key
        try:  # pragma: no cover - Airflow push may not be exercised in tests
            ti.xcom_push(key=marker_key, value=marker_payload)
        except Exception:
            pass

    return IdempotentExecution(executed=True, result=result, marker=None)

