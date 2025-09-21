"""Healthcheck helpers for container probes."""

from __future__ import annotations

__all__ = ["api_healthcheck", "worker_healthcheck"]


def api_healthcheck() -> int:
    from .api import main
    return main()


def worker_healthcheck() -> int:
    from .worker import main
    return main()
