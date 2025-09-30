from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


class ScenariosService:
    """Transitional scenarios domain service delegating to existing async service.

    This provides a stable facade in `libs/services` while we migrate storage/DAO.
    """

    def __init__(self) -> None:
        # Lazy to avoid import-time coupling
        from aurum.api.async_service import AsyncScenarioService  # type: ignore
        from aurum.api.container import provide_service  # type: ignore

        self._provider = provide_service(AsyncScenarioService)
        self._svc: Optional[AsyncScenarioService] = None  # type: ignore[name-defined]

    async def _get(self):
        if self._svc is None:
            self._svc = await self._provider()
        return self._svc

    async def list_scenarios(
        self,
        *,
        tenant_id: str,
        limit: int,
        offset: int,
        name_contains: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Optional[int], Dict[str, Any]]:
        svc = await self._get()
        return await svc.list_scenarios(
            tenant_id=tenant_id,
            limit=limit,
            offset=offset,
            name_contains=name_contains,
        )

    async def get_scenario(self, scenario_id: str, tenant_id: str):
        svc = await self._get()
        return await svc.get_scenario(scenario_id, tenant_id)

    async def create_scenario(self, scenario):
        svc = await self._get()
        return await svc.create_scenario(scenario)

    async def create_scenario_run(self, scenario_id: str, run, tenant_id: str):
        svc = await self._get()
        return await svc.create_scenario_run(scenario_id, run, tenant_id)

    async def list_scenario_runs(
        self,
        *,
        scenario_id: str,
        tenant_id: str,
        offset: int,
        limit: int,
        status_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        svc = await self._get()
        return await svc.list_scenario_runs(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            offset=offset,
            limit=limit,
            status_filter=status_filter,
        )


