"""Compatibility shim for legacy `libs.pipelines.seatunnel` imports."""

from aurum.seatunnel import *  # noqa: F401,F403


try:
    from aurum.seatunnel import __all__ as _seatunnel_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_seatunnel_all)
