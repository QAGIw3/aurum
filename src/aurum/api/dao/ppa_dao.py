"""PPA Data Access Object - handles PPA valuation calculations."""

from __future__ import annotations

import logging
from calendar import monthrange
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from aurum.core import AurumSettings
from ..config import TrinoConfig
from ..state import get_settings

LOGGER = logging.getLogger(__name__)


class PpaDao:
    """Data Access Object for PPA valuation operations."""
    
    def __init__(self, settings: Optional[AurumSettings] = None):
        self._settings = settings or get_settings()
        self._trino_config = TrinoConfig.from_settings(self._settings)
    
    def _execute_trino_query(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a Trino query and return results."""
        from ..service import _execute_trino_query
        return _execute_trino_query(self._trino_config, sql)
    
    def _coerce_float(self, value: Any, default: float = 0.0) -> float:
        """Coerce value to float with default."""
        from ..service import _coerce_float
        return _coerce_float(value, default)
    
    def _coerce_date(self, value: Any, fallback: date) -> date:
        """Coerce value to date with fallback."""
        from ..service import _coerce_date
        return _coerce_date(value, fallback)
    
    def _month_end(self, day: date) -> date:
        """Get the last day of the month for a given date."""
        from ..service import _month_end
        return _month_end(day)
    
    def _month_offset(self, start: date, end: date) -> int:
        """Calculate month offset between two dates."""
        from ..service import _month_offset
        return _month_offset(start, end)
    
    def _extract_currency(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract currency from row data."""
        from ..service import _extract_currency
        return _extract_currency(row)
    
    def _compute_irr(self, cashflows: List[float], *, tolerance: float = 1e-6, max_iterations: int = 80) -> Optional[float]:
        """Compute internal rate of return for cashflows."""
        from ..service import _compute_irr
        return _compute_irr(cashflows, tolerance=tolerance, max_iterations=max_iterations)
    
    def query_ppa_valuation(
        self,
        *,
        scenario_id: str,
        contract_id: Optional[str] = None,
        tenant_id: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Query PPA valuation data."""
        from ..service import query_ppa_valuation
        return query_ppa_valuation(
            self._trino_config,
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
        from ..service import query_ppa_contract_valuations
        return query_ppa_contract_valuations(
            self._trino_config,
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            limit=limit,
        )