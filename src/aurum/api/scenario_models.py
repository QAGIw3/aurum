"""Compatibility layer for legacy imports of scenario models."""

from __future__ import annotations

from .scenarios.scenario_models import *  # noqa: F401,F403

__all__ = [name for name in globals().keys() if not name.startswith("_")]
