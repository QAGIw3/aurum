from __future__ import annotations

"""Service layer for v2 PPA endpoints.

Contracts are sourced from the in-memory/DB-backed ScenarioStore. Valuations are
queried from Trino via existing helpers.
"""

from typing import Any, Dict, List, Optional

from .config import TrinoConfig
from .services.ppa_service import PpaService
from .state import get_settings


class PpaV2Service:
    def __init__(self) -> None:
        self._service = PpaService()

    async def list_contracts(
        self,
        *,
        tenant_id: Optional[str],
        offset: int,
        limit: int,
        counterparty_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self._service.list_contracts(
            tenant_id=tenant_id,
            offset=offset,
            limit=limit,
            counterparty_filter=counterparty_filter,
        )

    async def list_valuations(
        self,
        *,
        tenant_id: Optional[str],
        contract_id: str,
        offset: int,
        limit: int,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return valuation rows for a contract (paged)."""
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        rows, _elapsed = self._service.list_contract_valuation_rows(
            contract_id=contract_id,
            scenario_id=None,
            metric=None,
            tenant_id=tenant_id,
            limit=limit,
            offset=offset,
            trino_cfg=trino_cfg,
        )
        return [
            {
                "valuation_date": str(row.get("asof_date")),
                "present_value": float(row.get("npv") or row.get("value") or 0.0),
                "currency": row.get("metric_currency") or "USD",
            }
            for row in rows
        ]


async def get_ppa_service() -> PpaV2Service:
    return PpaV2Service()
