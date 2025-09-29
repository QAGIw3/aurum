"""Minimal in-memory scenario store shim for tests and legacy imports."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class _Run:
    id: str
    scenario_id: str
    status: str = "QUEUED"
    created_at: datetime = field(default_factory=datetime.utcnow)


class _InMemoryScenarioStore:
    def __init__(self) -> None:
        self._scenarios: Dict[str, Dict[str, Any]] = {}
        self._runs: Dict[str, _Run] = {}
        self._outputs: List[Dict[str, Any]] = []

    async def list_scenarios(self, **kwargs: Any) -> Tuple[List[Dict[str, Any]], int]:
        items = list(self._scenarios.values())
        return items, len(items)

    async def create_scenario(self, scenario_data: Dict[str, Any]) -> Dict[str, Any]:
        self._scenarios[scenario_data["id"]] = scenario_data
        return scenario_data

    async def get_scenario(self, scenario_id: str) -> Optional[Dict[str, Any]]:
        return self._scenarios.get(scenario_id)

    async def delete_scenario(self, scenario_id: str) -> bool:
        return self._scenarios.pop(scenario_id, None) is not None

    async def list_runs(self, **kwargs: Any) -> Tuple[List[Dict[str, Any]], int]:
        runs = [r.__dict__ for r in self._runs.values() if r.scenario_id == kwargs.get("scenario_id")]
        return runs, len(runs)

    async def create_run_from_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        run = _Run(id=data["id"], scenario_id=data["scenario_id"], status=data.get("status", "QUEUED"))
        self._runs[run.id] = run
        return run.__dict__

    async def get_run(self, scenario_id: str, run_id: str) -> Optional[Dict[str, Any]]:
        run = self._runs.get(run_id)
        return run.__dict__ if run else None

    async def update_run_state(self, run_id: str, update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        run = self._runs.get(run_id)
        if not run:
            return None
        for k, v in update.items():
            setattr(run, k, v)
        return run.__dict__

    async def create_run_output(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self._outputs.append(data)
        return data

    async def get_outputs(self, **kwargs: Any) -> Tuple[List[Dict[str, Any]], int]:
        items = [o for o in self._outputs if o.get("scenario_run_id") == kwargs.get("scenario_run_id")]
        return items, len(items)


_STORE: Optional[_InMemoryScenarioStore] = None


def get_scenario_store() -> _InMemoryScenarioStore:
    global _STORE
    if _STORE is None:
        _STORE = _InMemoryScenarioStore()
    return _STORE


