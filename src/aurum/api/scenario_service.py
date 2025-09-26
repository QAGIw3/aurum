"""Compatibility layer exposing scenario store implementations.

Historically the scenario store lived at ``aurum.api.scenario_service``.  The
recent refactor moved the implementation under ``aurum.api.scenarios`` but a
large portion of the codebase – including the test-suite – still imports from
this legacy location.  Re-export the public classes so those imports continue to
work without dragging in heavy dependencies eagerly.
"""

from __future__ import annotations

from aurum.api.scenarios.scenario_service import (
    BaseScenarioStore,
    InMemoryScenarioStore,
    PostgresScenarioStore,
    ScenarioRecord,
    ScenarioRunRecord,
    STORE,
)

__all__ = [
    "BaseScenarioStore",
    "InMemoryScenarioStore",
    "PostgresScenarioStore",
    "ScenarioRecord",
    "ScenarioRunRecord",
    "STORE",
]


def get_store() -> BaseScenarioStore:
    """Return the configured scenario store instance."""

    return STORE
