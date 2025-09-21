"""Runtime stubs for Airflow preflight utilities."""
from __future__ import annotations

from typing import Iterable, Sequence

from .alerting import build_failure_callback, emit_alert
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


__all__ = ["build_preflight_callable", "emit_alert", "build_failure_callback", "metrics"]
