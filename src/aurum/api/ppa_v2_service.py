from __future__ import annotations

"""Service layer for v2 PPA endpoints.

Contracts are sourced from the in-memory/DB-backed ScenarioStore. Valuations are
queried from Trino via existing helpers.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .scenarios.scenario_service import STORE as ScenarioStore
from .service import query_ppa_contract_valuations
from .config import TrinoConfig
from .state import get_settings


class PpaV2Service:
    async def list_contracts(
        self,
        *,
        tenant_id: Optional[str],
        offset: int,
        limit: int,
        counterparty_filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Return paged PPA contract summaries.

        Maps ScenarioStore records into the v2 contract shape. Falls back to
        sensible defaults when fields are not present in terms.
        """

        # ScenarioStore is synchronous; slice for pagination
        records = ScenarioStore.list_ppa_contracts(tenant_id, limit=offset + limit, offset=0)
        page = records[offset : offset + limit]

        items: List[Dict[str, Any]] = []
        for record in page:
            terms = record.terms if isinstance(record.terms, dict) else {}
            counterparty = terms.get("counterparty") or "unknown"
            if counterparty_filter and counterparty_filter.lower() not in str(counterparty).lower():
                continue
            capacity_mw = float(terms.get("capacity_mw") or 0.0)
            price_usd_mwh = float(terms.get("price_usd_mwh") or 0.0)
            start_date = str(terms.get("start_date") or "")
            end_date = str(terms.get("end_date") or "")
            items.append(
                {
                    "contract_id": record.id,
                    "name": str(record.instrument_id or record.id),
                    "counterparty": counterparty,
                    "capacity_mw": capacity_mw,
                    "price_usd_mwh": price_usd_mwh,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )
        return items

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
        rows, _elapsed = query_ppa_contract_valuations(
            trino_cfg,
            ppa_contract_id=contract_id,
            scenario_id=None,
            metric=None,
            limit=limit,
            offset=offset,
            tenant_id=tenant_id,
        )
        items: List[Dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "valuation_date": str(row.get("asof_date")),
                    "present_value": float(row.get("npv") or row.get("value") or 0.0),
                    "currency": "USD",  # default; could be taken from row if present
                }
            )
        return items


async def get_ppa_service() -> PpaV2Service:
    return PpaV2Service()

