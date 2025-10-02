"""Compatibility shim for legacy `libs.core` imports."""

from aurum.core import *  # noqa: F401,F403
from aurum.core.models import Watermark


try:
    from aurum.core import __all__ as _core_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_core_all)
    if "Watermark" not in __all__:
        __all__.append("Watermark")
