"""Shared base classes for service modules."""

from __future__ import annotations

from typing import Optional

from aurum.core import get_settings
from .storage.trino import TrinoAnalyticRepo


class BaseTrinoService:
    """Provide shared settings + Trino repository wiring for service classes."""

    def __init__(self, *, trino: Optional[TrinoAnalyticRepo] = None) -> None:
        self._settings = get_settings()
        self._trino = trino or TrinoAnalyticRepo(self._settings.database)


__all__ = ["BaseTrinoService"]
