"""Compatibility shim exposing the canonical SeaTunnel assertions package."""

from aurum.seatunnel.assertions import *  # noqa: F401,F403


try:
    from aurum.seatunnel.assertions import __all__ as _assertions_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_assertions_all)
