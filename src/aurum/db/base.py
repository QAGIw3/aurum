"""Minimal database client abstractions used in the slice ledger tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class DatabaseError(RuntimeError):
    """Generic database interaction error."""


class SupportsExecute(Protocol):  # pragma: no cover - structural typing helper
    def execute(self, query: str, parameters: tuple[Any, ...] | None = None) -> Any:
        ...


@dataclass
class DatabaseClient:
    """Lightweight stub for a DB client used in tests.

    The production implementation exposes a richer interface.  For tests we only
    need ``execute`` and ``fetchall`` style behaviour, which is normally provided
    by mocks.
    """

    dsn: str

    def connect(self) -> SupportsExecute:
        raise DatabaseError("DatabaseClient.connect must be mocked in tests")

    def close(self) -> None:  # pragma: no cover - no-op placeholder
        return None


__all__ = ["DatabaseClient", "DatabaseError"]
