from __future__ import annotations

"""PPA domain service with DAO pattern implementation."""

from typing import Any, Dict, List, Optional, Tuple

from .base_service import QueryableServiceInterface
from ..dao.ppa_dao import PpaDao


class PpaService(QueryableServiceInterface):
    """PPA domain service implementing business logic and data access through DAO.
    
    Implements Phase 1.3 service layer decomposition with clear domain boundaries.
    """
    
    def __init__(self):
        self._dao = PpaDao()
    
    # Legacy v2 service compatibility methods
    async def list_contracts(self, *, tenant_id: Optional[str], offset: int, limit: int, counterparty_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        from aurum.api.ppa_v2_service import PpaV2Service

        return await PpaV2Service().list_contracts(tenant_id=tenant_id, offset=offset, limit=limit, counterparty_filter=counterparty_filter)

    async def list_valuations(self, *, tenant_id: Optional[str], contract_id: str, offset: int, limit: int, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Dict[str, Any]]:
        from aurum.api.ppa_v2_service import PpaV2Service

        return await PpaV2Service().list_valuations(tenant_id=tenant_id, contract_id=contract_id, offset=offset, limit=limit, start_date=start_date, end_date=end_date)
    
    # New DAO-based methods that replace service.py functions
    def query_ppa_valuation(
        self,
        *,
        scenario_id: str,
        contract_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query PPA valuation data."""
        return self._dao.query_ppa_valuation(
            scenario_id=scenario_id,
            contract_id=contract_id,
            tenant_id=tenant_id,
        )
    
    def query_ppa_contract_valuations(
        self,
        *,
        scenario_id: str,
        tenant_id: Optional[str] = None,
        limit: int = 100,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query PPA contract valuations data."""
        return self._dao.query_ppa_contract_valuations(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            limit=limit,
        )
    
    # ServiceInterface implementation
    async def invalidate_cache(self) -> Dict[str, int]:
        """Invalidate PPA-specific caches."""
        # Placeholder - implement based on specific caching patterns
        return {"ppa": 0}
    
    # QueryableServiceInterface implementation
    async def query_data(
        self, 
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query PPA data with pagination and filtering."""
        filters = filters or {}
        scenario_id = filters.get("scenario_id")
        if not scenario_id:
            return []
            
        data, _ = self.query_ppa_contract_valuations(
            scenario_id=scenario_id,
            tenant_id=filters.get("tenant_id"),
            limit=limit,
        )
        return data[offset:offset + limit]
