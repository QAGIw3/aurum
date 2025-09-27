"""Fake ScenariosDAO for testing."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from ..scenarios_dao import ScenariosDAO, ScenarioFilter, ScenarioRunFilter
from ...scenarios.scenario_models import ScenarioResponse, ScenarioRunResponse


class FakeScenariosDAO(ScenariosDAO):
    """Fake implementation of ScenariosDAO for testing."""

    def __init__(self, trino_config: Optional[Dict[str, Any]] = None):
        """Initialize fake Scenarios DAO."""
        super().__init__(trino_config)
        self._scenarios: Dict[str, ScenarioResponse] = {}
        self._runs: Dict[str, ScenarioRunResponse] = {}
        self._query_delay = 0.01
        self._fail_next_operations = False
        self._operation_count = 0

    async def _connect(self) -> None:
        """Fake connection."""
        await asyncio.sleep(0.001)

    async def _disconnect(self) -> None:
        """Fake disconnection."""
        self._scenarios.clear()
        self._runs.clear()

    async def _execute_trino_query(
        self,
        query: str,
        parameters: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Fake query execution."""
        await asyncio.sleep(self._query_delay)

        if self._fail_next_operations:
            self._fail_next_operations = False
            raise Exception("Simulated query failure")

        self._operation_count += 1
        return []

    def inject_scenario(self, scenario: ScenarioResponse) -> None:
        """Inject scenario for testing."""
        self._scenarios[scenario.id] = scenario

    def inject_run(self, run: ScenarioRunResponse) -> None:
        """Inject scenario run for testing."""
        self._runs[run.id] = run

    async def create(self, scenario: ScenarioResponse) -> ScenarioResponse:
        """Create scenario."""
        await asyncio.sleep(self._query_delay)
        if not scenario.id:
            scenario.id = str(uuid4())
        self._scenarios[scenario.id] = scenario
        self._operation_count += 1
        return scenario

    async def get_by_id(self, scenario_id: str) -> Optional[ScenarioResponse]:
        """Get scenario by ID."""
        await asyncio.sleep(self._query_delay)
        self._operation_count += 1
        return self._scenarios.get(scenario_id)

    async def update(self, scenario_id: str, scenario: ScenarioResponse) -> Optional[ScenarioResponse]:
        """Update scenario."""
        await asyncio.sleep(self._query_delay)
        if scenario_id in self._scenarios:
            self._scenarios[scenario_id] = scenario
            self._operation_count += 1
            return scenario
        return None

    async def delete(self, scenario_id: str) -> bool:
        """Delete scenario."""
        await asyncio.sleep(self._query_delay)
        if scenario_id in self._scenarios:
            del self._scenarios[scenario_id]
            # Also delete related runs
            runs_to_delete = [rid for rid, run in self._runs.items() if run.scenario_id == scenario_id]
            for rid in runs_to_delete:
                del self._runs[rid]
            self._operation_count += 1
            return True
        return False

    async def list(
        self,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        order_desc: bool = False
    ) -> List[ScenarioResponse]:
        """List scenarios."""
        await asyncio.sleep(self._query_delay)
        self._operation_count += 1

        scenarios = list(self._scenarios.values())

        if filters:
            scenario_filter = ScenarioFilter(**filters)
            if scenario_filter.scenario_ids:
                scenarios = [s for s in scenarios if s.id in scenario_filter.scenario_ids]
            if scenario_filter.tenant_ids:
                scenarios = [s for s in scenarios if s.tenant_id in scenario_filter.tenant_ids]
            if scenario_filter.statuses:
                scenarios = [s for s in scenarios if s.status in scenario_filter.statuses]

        if order_by:
            reverse = order_desc
            if order_by == "name":
                scenarios.sort(key=lambda s: s.name, reverse=reverse)
            elif order_by == "created_at":
                scenarios.sort(key=lambda s: s.created_at, reverse=reverse)

        start = offset
        end = offset + limit
        return scenarios[start:end]

    async def create_scenario_run(self, run: ScenarioRunResponse) -> ScenarioRunResponse:
        """Create scenario run."""
        await asyncio.sleep(self._query_delay)
        if not run.id:
            run.id = str(uuid4())
        if not run.started_at:
            run.started_at = datetime.utcnow()
        self._runs[run.id] = run
        self._operation_count += 1
        return run

    async def get_scenario_run_by_id(self, run_id: str) -> Optional[ScenarioRunResponse]:
        """Get scenario run by ID."""
        await asyncio.sleep(self._query_delay)
        self._operation_count += 1
        return self._runs.get(run_id)

    async def list_scenario_runs(
        self,
        scenario_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[ScenarioRunResponse]:
        """List scenario runs."""
        await asyncio.sleep(self._query_delay)
        self._operation_count += 1

        runs = list(self._runs.values())

        if scenario_id:
            runs = [r for r in runs if r.scenario_id == scenario_id]

        if filters:
            run_filter = ScenarioRunFilter(**filters)
            if run_filter.run_ids:
                runs = [r for r in runs if r.id in run_filter.run_ids]
            if run_filter.statuses:
                runs = [r for r in runs if r.status in run_filter.statuses]

        runs.sort(key=lambda r: r.started_at or datetime.min, reverse=True)

        start = offset
        end = offset + limit
        return runs[start:end]

    async def update_scenario_run_status(
        self,
        run_id: str,
        status: str,
        error_message: Optional[str] = None
    ) -> bool:
        """Update scenario run status."""
        await asyncio.sleep(self._query_delay)
        if run_id in self._runs:
            run = self._runs[run_id]
            run.status = status
            if error_message:
                run.error_message = error_message
            if status in ["completed", "failed", "cancelled"]:
                run.completed_at = datetime.utcnow()
            self._operation_count += 1
            return True
        return False
