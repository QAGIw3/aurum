"""Compatibility shim exposing the canonical SeaTunnel renderer implementation."""

from aurum.seatunnel.renderer import *  # noqa: F401,F403


try:
    from aurum.seatunnel.renderer import __all__ as _renderer_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_renderer_all)
