"""Compatibility shim exposing `aurum.core.pagination`."""

from aurum.core.pagination import *  # noqa: F401,F403


try:
    from aurum.core.pagination import __all__ as _pagination_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_pagination_all)
