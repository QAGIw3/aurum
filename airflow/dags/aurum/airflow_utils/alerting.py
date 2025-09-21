"""Stub alerting helpers for DAG parsing environments."""
from __future__ import annotations

from typing import Any, Callable, Dict, Optional


def emit_alert(
    message: str,
    *,
    severity: str = "ERROR",
    source: str,
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:  # pragma: no cover - stub
    print(
        "[stub alert]", message,
        severity,
        source,
        topic,
        bootstrap_servers,
        schema_registry_url,
        context,
    )


def build_failure_callback(
    *,
    source: str,
    severity: str = "CRITICAL",
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
) -> Callable[[Dict[str, Any]], None]:  # pragma: no cover - stub
    def _callback(context: Dict[str, Any]) -> None:
        print(
            "[stub failure callback]",
            source,
            severity,
            topic,
            bootstrap_servers,
            schema_registry_url,
            context,
        )

    return _callback


__all__ = ["emit_alert", "build_failure_callback"]
