"""Compatibility shim exposing telemetry helpers under ``aurum.api`` namespace."""

from __future__ import annotations

from aurum.telemetry import *  # noqa: F401,F403

__all__ = [name for name in globals().keys() if not name.startswith("_")]
