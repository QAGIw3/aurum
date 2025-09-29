"""Observability helpers for dynamically generated workflows."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, Optional

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG

logger = logging.getLogger("aurum.workflow.observability")


def _serialize_dt(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


def _safe_json(payload: Dict[str, Any]) -> str:
    try:
        return json.dumps(payload, default=str)
    except Exception:  # pragma: no cover - defensive logging
        return str(payload)


def _make_task_callback(event: str, metadata: Dict[str, Any]):
    def _callback(context: Dict[str, Any]) -> None:
        ti = context.get("ti")
        payload = {
            "event": event,
            "dag_id": metadata.get("dag_id"),
            "task_id": metadata.get("task_id"),
            "task_type": metadata.get("task_type"),
            "version": metadata.get("version"),
            "run_id": getattr(ti, "run_id", None),
            "try_number": getattr(ti, "try_number", None),
            "state": getattr(ti, "state", None),
            "map_index": getattr(ti, "map_index", None),
            "execution_date": _serialize_dt(context.get("execution_date")),
        }
        logger.info(_safe_json(payload))

    return _callback


def _make_dag_callback(event: str, dag_id: str, version: Optional[str]):
    def _callback(context: Dict[str, Any]) -> None:
        payload = {
            "event": event,
            "dag_id": dag_id,
            "version": version,
            "run_id": context.get("run_id"),
            "execution_date": _serialize_dt(context.get("execution_date")),
        }
        logger.info(_safe_json(payload))

    return _callback


def instrument_task(
    task: BaseOperator,
    *,
    dag_id: str,
    task_type: str,
    version: Optional[str],
) -> None:
    metadata = {
        "dag_id": dag_id,
        "task_id": task.task_id,
        "task_type": task_type,
        "version": version,
    }
    if getattr(task, "on_success_callback", None) is None:
        task.on_success_callback = _make_task_callback("task_success", metadata)
    if getattr(task, "on_retry_callback", None) is None:
        task.on_retry_callback = _make_task_callback("task_retry", metadata)
    # Respect existing failure callback set via default_args
    if getattr(task, "on_failure_callback", None) is None:
        task.on_failure_callback = _make_task_callback("task_failure", metadata)


def instrument_dag(dag: DAG, *, version: Optional[str]) -> None:
    if dag.on_failure_callback is None:
        dag.on_failure_callback = _make_dag_callback("dag_failure", dag.dag_id, version)
    if dag.on_success_callback is None:
        dag.on_success_callback = _make_dag_callback("dag_success", dag.dag_id, version)
