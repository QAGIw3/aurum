"""Compatibility shim exposing canonical SeaTunnel subject naming helpers."""

from aurum.seatunnel.subject_naming import *  # noqa: F401,F403


try:
    from aurum.seatunnel.subject_naming import __all__ as _subject_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_subject_all)
