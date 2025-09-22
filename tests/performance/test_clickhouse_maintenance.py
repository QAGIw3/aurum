"""Tests for ClickHouse maintenance job."""

from __future__ import annotations

import pytest

from aurum.core import AurumSettings
from aurum.performance.warehouse import (
    ClickHouseMaintenanceJob,
    ClickHouseTableConfig,
    WarehouseMaintenanceMetrics,
)


class FakeClickHouseClient:
    """Simple ClickHouse client stub capturing executed statements."""

    def __init__(self) -> None:
        self.commands: list[tuple[str, dict | None]] = []
        self.ttl_expression: str | None = None

    def execute(self, query: str, params: dict | None = None):
        self.commands.append((query, params))
        normalized = " ".join(query.strip().split())
