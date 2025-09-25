from __future__ import annotations

"""Compatibility shim for database configuration imports.

Re-exports TrinoConfig from consolidated `aurum.api.config` so that
legacy imports `aurum.api.database.config` continue to work.
"""

from ..config import TrinoConfig, TrinoCatalogType, TrinoAccessLevel, TrinoCatalogConfig  # re-export

__all__ = ["TrinoConfig", "TrinoCatalogType", "TrinoAccessLevel", "TrinoCatalogConfig"]

