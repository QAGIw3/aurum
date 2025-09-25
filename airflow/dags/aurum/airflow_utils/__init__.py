"""Deprecated DAG-local Airflow utilities (shim).

This package is deprecated. Prefer importing from src/aurum/airflow_utils.
We keep this shim to maintain backward compatibility for existing DAGs and
to ensure submodules like ``aurum.airflow_utils.iso`` resolve from ``src``.
"""

from __future__ import annotations

from typing import Iterable, Sequence
import os
import sys
from pathlib import Path

# Extend package search path so submodules resolve from src/ when not present locally
try:  # pragma: no cover - best effort
    repo_root = Path(__file__).resolve().parents[4]
    src_path = str(repo_root / "src" / "aurum" / "airflow_utils")
    if src_path not in __path__:  # type: ignore[name-defined]
        __path__.append(src_path)  # type: ignore[name-defined]
except Exception:
    pass

try:
    if os.getenv("AURUM_DEBUG_SHIM", "0") not in {"", "0", "false", "False"}:
        print("[aurum.airflow_utils] Using deprecated DAG-local shim; prefer src/aurum/airflow_utils")
except Exception:
    pass

from .alerting import build_failure_callback, emit_alert
from .dag_factory import (
    DAGFactory,
    create_incremental_dag,
    create_seatunnel_dag,
    create_backfill_dag,
    POOL_CONFIGS,
    SLA_CONFIGS,
)
from .deferrable_operators import (
    SeaTunnelJobOperator,
    APICallOperator,
    KafkaMessageSensor,
    DatabaseOperationOperator,
    create_seatunnel_job_task,
    create_api_call_task,
    create_kafka_sensor_task,
)
from . import metrics


def build_preflight_callable(
    *,
    required_variables: Sequence[str] | None = None,
    optional_variables: Sequence[str] | None = None,
    required_connections: Sequence[str] | None = None,
    optional_connections: Sequence[str] | None = None,
    warn_only_variables: Sequence[str] | None = None,
    warn_only_connections: Sequence[str] | None = None,
):  # pragma: no cover - simple stub
    required_variables = list(dict.fromkeys(required_variables or ()))
    optional_variables = list(dict.fromkeys(optional_variables or ()))
    required_connections = list(dict.fromkeys(required_connections or ()))
    optional_connections = list(dict.fromkeys(optional_connections or ()))
    warn_only_variables = list(dict.fromkeys(warn_only_variables or ()))
    warn_only_connections = list(dict.fromkeys(warn_only_connections or ()))

    def _preflight_stub(**_: object) -> None:
        print(
            "[stub] preflight check"
            f" required_variables={required_variables}"
            f" optional_variables={optional_variables}"
            f" required_connections={required_connections}"
            f" optional_connections={optional_connections}"
            f" warn_only_variables={warn_only_variables}"
            f" warn_only_connections={warn_only_connections}"
        )

    return _preflight_stub


__all__ = [
    "build_preflight_callable",
    "emit_alert",
    "build_failure_callback",
    "metrics",
    "DAGFactory",
    "create_incremental_dag",
    "create_seatunnel_dag",
    "create_backfill_dag",
    "POOL_CONFIGS",
    "SLA_CONFIGS",
    "SeaTunnelJobOperator",
    "APICallOperator",
    "KafkaMessageSensor",
    "DatabaseOperationOperator",
    "create_seatunnel_job_task",
    "create_api_call_task",
    "create_kafka_sensor_task",
]
