"""Utilities for Airflow DAGs shared across the ingestion platform."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Iterable, Sequence

from .alerting import build_failure_callback, emit_alert
from . import metrics


@dataclass(frozen=True)
class PreflightConfig:
    """Configuration describing what the preflight check should validate."""

    required_variables: tuple[str, ...] = ()
    optional_variables: tuple[str, ...] = ()
    required_connections: tuple[str, ...] = ()
    optional_connections: tuple[str, ...] = ()
    warn_only_variables: tuple[str, ...] = ()
    warn_only_connections: tuple[str, ...] = ()


def _normalise(values: Iterable[str] | None) -> tuple[str, ...]:
    if not values:
        return ()
    # Preserve order but drop duplicates
    seen: set[str] = set()
    normalised: list[str] = []
    for item in values:
        if item in seen:
            continue
        seen.add(item)
        normalised.append(item)
    return tuple(normalised)


def build_preflight_callable(
    *,
    required_variables: Sequence[str] | None = None,
    optional_variables: Sequence[str] | None = None,
    required_connections: Sequence[str] | None = None,
    optional_connections: Sequence[str] | None = None,
    warn_only_variables: Sequence[str] | None = None,
    warn_only_connections: Sequence[str] | None = None,
):
    """Return a callable suitable for an Airflow PythonOperator preflight task.

    The callable raises ``RuntimeError`` when required variables or connections
    are missing. Optional entries are logged as warnings so operators can fix
    configuration before runs start. ``warn_only_variables`` can be used to
    upgrade optional variables to warnings even if present in
    ``optional_variables``.
    """

    cfg = PreflightConfig(
        required_variables=_normalise(required_variables),
        optional_variables=_normalise(optional_variables),
        required_connections=_normalise(required_connections),
        optional_connections=_normalise(optional_connections),
        warn_only_variables=_normalise(warn_only_variables),
        warn_only_connections=_normalise(warn_only_connections),
    )

    def _preflight(**_: object) -> None:
        debug_enabled = os.getenv("AURUM_DEBUG", "0") not in {"", "0", "false", "False"}
        if debug_enabled:
            print(
                "[preflight] required variables:",
                ", ".join(cfg.required_variables) or "<none>",
            )
            if cfg.optional_variables:
                print("[preflight] optional variables:", ", ".join(cfg.optional_variables))
            if cfg.required_connections:
                print("[preflight] required connections:", ", ".join(cfg.required_connections))
            if cfg.optional_connections:
                print(
                    "[preflight] optional connections:", ", ".join(cfg.optional_connections)
                )

        try:
            from airflow.models import Variable  # type: ignore
        except Exception as exc:  # pragma: no cover - defensive in DAG parse envs
            print(f"Airflow Variable API unavailable during preflight: {exc}")
            return

        missing_required: list[str] = []
        missing_optional: list[str] = []
        warn_only: list[str] = []

        for key in cfg.required_variables:
            try:
                Variable.get(key)
            except Exception:
                missing_required.append(key)

        optional_pool = set(cfg.optional_variables)
        optional_pool.update(cfg.warn_only_variables)
        for key in optional_pool:
            try:
                Variable.get(key)
            except Exception:
                if key in cfg.warn_only_variables:
                    warn_only.append(key)
                else:
                    missing_optional.append(key)

        base_hook = None
        try:
            from airflow.hooks.base import BaseHook  # type: ignore

            base_hook = BaseHook
        except Exception:
            # Connections cannot be checked when BaseHook is unavailable.
            pass

        missing_connections: list[str] = []
        missing_optional_connections: list[str] = []
        warn_only_connections: list[str] = []
        if base_hook:
            for conn_id in cfg.required_connections:
                try:
                    base_hook.get_connection(conn_id)
                except Exception:
                    missing_connections.append(conn_id)
            optional_conn_pool = set(cfg.optional_connections)
            optional_conn_pool.update(cfg.warn_only_connections)
            for conn_id in optional_conn_pool:
                try:
                    base_hook.get_connection(conn_id)
                except Exception:
                    if conn_id in cfg.warn_only_connections:
                        warn_only_connections.append(conn_id)
                    else:
                        missing_optional_connections.append(conn_id)

        messages: list[str] = []
        if missing_required:
            messages.append(
                "Missing required Airflow Variables: " + ", ".join(sorted(missing_required))
            )
        if missing_connections:
            messages.append(
                "Missing required Airflow Connections: " + ", ".join(sorted(missing_connections))
            )

        if messages:
            raise RuntimeError("; ".join(messages))

        if missing_optional:
            print(
                "Warning: missing optional Airflow Variables: "
                + ", ".join(sorted(missing_optional))
            )
        if missing_optional_connections:
            print(
                "Warning: missing optional Airflow Connections: "
                + ", ".join(sorted(missing_optional_connections))
            )
        if warn_only:
            print(
                "Warning: recommended Airflow Variables not configured: "
                + ", ".join(sorted(warn_only))
            )
        if warn_only_connections:
            print(
                "Warning: recommended Airflow Connections not configured: "
                + ", ".join(sorted(warn_only_connections))
            )

    return _preflight


__all__ = [
    "build_preflight_callable",
    "PreflightConfig",
    "emit_alert",
    "build_failure_callback",
    "metrics",
]
