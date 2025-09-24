from __future__ import annotations

"""Service layer for v2 EIA endpoints.

Datasets are sourced from the static EIA catalog. Series points are queried
from Trino using the existing query helpers and optional Redis caching.
"""

from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime

from pydantic import BaseModel, Field

from ..reference import eia_catalog as ref_eia
from .config import CacheConfig, TrinoConfig
from .state import get_settings
from .service import query_eia_series, query_eia_series_dimensions


class DatasetItem(BaseModel):
    dataset_path: str = Field(...)
    title: str = Field(...)
    description: str = Field(...)
    last_updated: Optional[str] = Field(None)


class SeriesPointItem(BaseModel):
    series_id: str
    period: str
    period_start: str
    value: float
    unit: Optional[str] = None


class EiaV2Service:
    async def list_datasets(self, *, offset: int, limit: int) -> Tuple[List[DatasetItem], int]:
        """Return paged list of EIA datasets and the total count."""
        all_datasets: List[DatasetItem] = []
        for ds in ref_eia.iter_datasets():
            all_datasets.append(
                DatasetItem(
                    dataset_path=ds.dataset_path,
                    title=ds.title,
                    description=ds.description or "",
                    last_updated=ds.last_updated,
                )
            )
        total = len(all_datasets)
        start = max(0, int(offset))
        end = start + max(1, int(limit))
        return all_datasets[start:end], total

    async def get_series(
        self,
        *,
        series_id: str,
        start_date: Optional[str],
        end_date: Optional[str],
        offset: int,
        limit: int,
    ) -> List[SeriesPointItem]:
        """Query series points from Trino with pagination and optional date filters."""

        def _to_dt_or_none(value: Optional[str]) -> Optional[datetime]:
            if not value:
                return None
            try:
                return datetime.fromisoformat(value)
            except Exception:
                return None

        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        cache_cfg = CacheConfig.from_settings(settings)

        rows, _elapsed_ms = query_eia_series(
            trino_cfg,
            cache_cfg,
            series_id=series_id,
            frequency=None,
            area=None,
            sector=None,
            dataset=None,
            unit=None,
            canonical_unit=None,
            canonical_currency=None,
            source=None,
            start=_to_dt_or_none(start_date),
            end=_to_dt_or_none(end_date),
            limit=limit,
            offset=offset,
            cursor_after=None,
            cursor_before=None,
            descending=False,
        )

        points: List[SeriesPointItem] = []
        for row in rows:
            points.append(
                SeriesPointItem(
                    series_id=str(row.get("series_id") or series_id),
                    period=str(row.get("period")),
                    period_start=str(row.get("period_start")),
                    value=float(row.get("value") if row.get("value") is not None else 0.0),
                    unit=str(row.get("unit")) if row.get("unit") is not None else None,
                )
            )
        return points

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
        """Return distinct dimension values for EIA series observations."""
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        cache_cfg = CacheConfig.from_settings(settings)

        # Normalize frequency to uppercase when provided
        freq = frequency.upper() if frequency else None
        values, _elapsed_ms = query_eia_series_dimensions(
            trino_cfg,
            cache_cfg,
            series_id=series_id,
            frequency=freq,
            area=area,
            sector=sector,
            dataset=dataset,
            unit=unit,
            canonical_unit=canonical_unit,
            canonical_currency=canonical_currency,
            source=source,
        )
        return values


async def get_eia_service() -> EiaV2Service:
    return EiaV2Service()
