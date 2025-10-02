"""Compatibility shim exposing the canonical SeaTunnel dry-run renderer."""

from aurum.seatunnel.dry_run_renderer import *  # noqa: F401,F403


try:
    from aurum.seatunnel.dry_run_renderer import __all__ as _dry_run_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_dry_run_all)
