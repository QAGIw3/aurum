from __future__ import annotations

"""Service layer for v2 PPA endpoints.

Contracts are sourced from the in-memory/DB-backed ScenarioStore. Valuations are
queried from Trino via existing helpers.
"""

import math
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from .config import TrinoConfig
from .services.ppa_service import PpaService, normalize_currency_code
from aurum.core.settings import get_settings as _core_get_settings


def _format_date(value: Any) -> Optional[str]:
    """Convert assorted date/time payloads into ISO8601 ``YYYY-MM-DD`` strings."""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalised = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            return datetime.fromisoformat(normalised).date().isoformat()
        except ValueError:
            try:
                return date.fromisoformat(text).isoformat()
            except ValueError:
                return None
    return None


def _optional_float(value: Any) -> Optional[float]:
    """Safely coerce numeric payloads to floats, returning ``None`` on failure."""

    if value is None:
        return None
    if isinstance(value, Decimal):
        result = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            result = float(text)
        except ValueError:
            return None
    else:
        try:
            result = float(value)
        except (TypeError, ValueError):
            return None
    if math.isnan(result) or math.isinf(result):
        return None
    return float(result)


def _present_value(row: Dict[str, Any]) -> Optional[float]:
    for key in ("npv", "value"):
        candidate = _optional_float(row.get(key))
        if candidate is not None:
            return candidate
    return None


def _normalise_currency(row: Dict[str, Any]) -> str:
    for key in ("metric_currency", "currency", "metric_unit"):
        candidate = normalize_currency_code(row.get(key))
        if candidate and len(candidate) == 3 and candidate.isalpha():
            return candidate
    return "USD"


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
        raw_contracts = await self._service.list_contracts(
            tenant_id=tenant_id,
            offset=offset,
            limit=limit,
            counterparty_filter=counterparty_filter,
        )

        sanitized: List[Dict[str, Any]] = []
        for contract in raw_contracts:
            capacity = _optional_float(contract.get("capacity_mw"))
            price = _optional_float(contract.get("price_usd_mwh"))
            start_date = _format_date(contract.get("start_date"))
            end_date = _format_date(contract.get("end_date"))

            contract_id = str(contract.get("contract_id") or contract.get("id") or "").strip()
            name = str(contract.get("name") or contract_id).strip() or contract_id
            counterparty_raw = contract.get("counterparty")
            counterparty = str(counterparty_raw).strip() if counterparty_raw is not None else ""
            if not counterparty:
                counterparty = "unknown"

            sanitized.append(
                {
                    "contract_id": contract_id,
                    "name": name,
                    "counterparty": counterparty,
                    "capacity_mw": capacity if capacity is not None else 0.0,
                    "price_usd_mwh": price if price is not None else 0.0,
                    "start_date": start_date,
                    "end_date": end_date,
                }
            )

        return sanitized

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

        return [
            {
                "valuation_date": _format_date(row.get("asof_date")),
                "period_start": _format_date(row.get("period_start")),
                "period_end": _format_date(row.get("period_end")),
                "metric": row.get("metric"),
                "present_value": _present_value(row),
                "cashflow": _optional_float(row.get("cashflow")),
                "irr": _optional_float(row.get("irr")),
                "currency": _normalise_currency(row),
            }
            for row in rows
        ]


async def get_ppa_service() -> PpaV2Service:
    return PpaV2Service()
