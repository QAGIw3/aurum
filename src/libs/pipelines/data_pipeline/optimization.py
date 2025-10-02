"""Compatibility shim exposing `aurum.data_pipeline.optimization`."""

from aurum.data_pipeline.optimization import *  # noqa: F401,F403


try:
    from aurum.data_pipeline.optimization import __all__ as _optimization_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_optimization_all)
