from __future__ import annotations

"""EIA domain service façade -> forwards to v2 service implementation."""

from typing import Any, Dict, List, Optional, Tuple


class EiaService:
    async def list_datasets(self, *, offset: int, limit: int) -> Tuple[List[Dict[str, Any]], int]:
        from aurum.api.eia_v2_service import EiaV2Service

        return await EiaV2Service().list_datasets(offset=offset, limit=limit)

    async def get_series(self, *, series_id: str, start_date: Optional[str], end_date: Optional[str], offset: int, limit: int) -> List[Dict[str, Any]]:
        from aurum.api.eia_v2_service import EiaV2Service

        return await EiaV2Service().get_series(series_id=series_id, start_date=start_date, end_date=end_date, offset=offset, limit=limit)

    async def get_series_dimensions(
        self,
        *,
        series_id: Optional[str] = None,
        frequency: Optional[str] = None,
        area: Optional[str] = None,
        sector: Optional[str] = None,
        dataset: Optional[str] = None,
        unit: Optional[str] = None,
        canonical_unit: Optional[str] = None,
        canonical_currency: Optional[str] = None,
        source: Optional[str] = None,
    ) -> Dict[str, List[str]]:
        from aurum.api.eia_v2_service import EiaV2Service

        return await EiaV2Service().get_series_dimensions(
            series_id=series_id,
            frequency=frequency,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )

