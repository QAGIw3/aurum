"""Compatibility shim exposing `aurum.core.models`."""

from aurum.core.models import *  # noqa: F401,F403


try:
    from aurum.core.models import __all__ as _models_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_models_all)
