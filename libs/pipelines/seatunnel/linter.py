"""Compatibility shim exposing the canonical SeaTunnel linter."""

from aurum.seatunnel.linter import *  # noqa: F401,F403


try:
    from aurum.seatunnel.linter import __all__ as _linter_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_linter_all)
