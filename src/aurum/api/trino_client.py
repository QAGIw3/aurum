"""Compatibility shim for Trino client imports.

This module forwards the public API to the canonical implementation under
`aurum.api.database.trino_client`. Prefer importing from the database package
directly in new code.
"""

from __future__ import annotations

from .database.trino_client import (
    get_trino_client,
    get_default_trino_client,
    get_trino_client_by_catalog,
    create_trino_client,
    HybridTrinoClientManager,
    TrinoClient,
)

__all__ = [
    "get_trino_client",
    "get_default_trino_client",
    "get_trino_client_by_catalog",
    "create_trino_client",
    "HybridTrinoClientManager",
    "TrinoClient",
]

