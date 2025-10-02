"""Compatibility shim exposing the canonical assertion config generator."""

from aurum.seatunnel.generate_assertion_config import *  # noqa: F401,F403


try:
    from aurum.seatunnel.generate_assertion_config import __all__ as _generator_all
except ImportError:  # pragma: no cover - defensive
    __all__ = [name for name in globals() if not name.startswith("_")]
else:
    __all__ = list(_generator_all)
