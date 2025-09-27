from __future__ import annotations

"""Service layer for v2 PPA endpoints.

Contracts are sourced from the in-memory/DB-backed ScenarioStore. Valuations are
queried from Trino via existing helpers.
"""

from datetime import date
from typing import Any, Dict, List, Optional

from .config import TrinoConfig
from .services.ppa_service import PpaService, coerce_float
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
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
            trino_cfg=trino_cfg,
        )

        def _format_date(value: Any) -> Optional[str]:
            if isinstance(value, date):
                return value.isoformat()
            if value is None:
                return None
            return str(value)

        def _maybe_float(value: Any) -> Optional[float]:
            return coerce_float(value) if value is not None else None

        return [
            {
                "valuation_date": _format_date(row.get("asof_date")),
                "period_start": _format_date(row.get("period_start")),
                "period_end": _format_date(row.get("period_end")),
                "metric": row.get("metric"),
                "present_value": (
                    coerce_float(row.get("npv"))
                    if row.get("npv") is not None
                    else _maybe_float(row.get("value"))
                ),
                "cashflow": _maybe_float(row.get("cashflow")),
                "irr": _maybe_float(row.get("irr")),
                "currency": row.get("metric_currency") or "USD",
            }
            for row in rows
        ]


async def get_ppa_service() -> PpaV2Service:
    return PpaV2Service()
