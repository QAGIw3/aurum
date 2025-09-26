from __future__ import annotations

"""PPA domain service façade -> forwards to v2 service implementation."""

from typing import Any, Dict, List, Optional


class PpaService:
    async def list_contracts(self, *, tenant_id: Optional[str], offset: int, limit: int, counterparty_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        from aurum.api.ppa_v2_service import PpaV2Service

        return await PpaV2Service().list_contracts(tenant_id=tenant_id, offset=offset, limit=limit, counterparty_filter=counterparty_filter)

    async def list_valuations(self, *, tenant_id: Optional[str], contract_id: str, offset: int, limit: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        from aurum.api.ppa_v2_service import PpaV2Service

        return await PpaV2Service().list_valuations(tenant_id=tenant_id, contract_id=contract_id, offset=offset, limit=limit, start_date=start_date, end_date=end_date)
