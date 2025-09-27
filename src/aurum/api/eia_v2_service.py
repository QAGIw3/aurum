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
from .database.backend_selector import get_data_backend


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
        # Try backend selector first for ClickHouse/Timescale/Trino
        try:
            backend = get_data_backend(settings)
            api_cfg = getattr(settings, "api", None)
            backend_type = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if backend_type == "trino":
                table = getattr(api_cfg, "eia_series_table_trino", getattr(settings.database, "eia_series_base_table", "iceberg.market.eia_series"))
            elif backend_type == "clickhouse":
                table = getattr(api_cfg, "eia_series_table_clickhouse", "aurum.eia_series")
            elif backend_type == "timescale":
                table = getattr(api_cfg, "eia_series_table_timescale", "market.eia_series")
            else:
                table = "iceberg.market.eia_series"

            where = [f"series_id = '{series_id.replace("'", "''")}'"]
            if start_date:
                try:
                    sd = _to_dt_or_none(start_date)
                    if sd:
                        where.append(f"period_start >= TIMESTAMP '{sd.isoformat(sep=' ')}'")
                except Exception:
                    pass
            if end_date:
                try:
                    ed = _to_dt_or_none(end_date)
                    if ed:
                        where.append(f"period_start <= TIMESTAMP '{ed.isoformat(sep=' ')}'")
                except Exception:
                    pass
            where_clause = (" WHERE " + " AND ".join(where)) if where else ""
            sql = f"SELECT series_id, period, period_start, value, unit FROM {table}{where_clause} ORDER BY period_start LIMIT {int(limit)} OFFSET {int(offset)}"
            result = await backend.execute_query(sql)
            rows = [
                {
                    "series_id": r[0],
                    "period": r[1],
                    "period_start": r[2],
                    "value": r[3],
                    "unit": r[4] if len(r) > 4 else None,
                }
                for r in result.rows
            ]
        except Exception:
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
        # Use selector only when no filters are provided (simple DISTINCTs). Otherwise, use legacy helper.
        if any(v is not None for v in (series_id, frequency, area, sector, dataset, unit, canonical_unit, canonical_currency, source)):
            trino_cfg = TrinoConfig.from_settings(settings)
            cache_cfg = CacheConfig.from_settings(settings)
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

        try:
            backend = get_data_backend(settings)
            api_cfg = getattr(settings, "api", None)
            backend_type = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if backend_type == "trino":
                table = getattr(api_cfg, "eia_series_table_trino", getattr(settings.database, "eia_series_base_table", "iceberg.market.eia_series"))
            elif backend_type == "clickhouse":
                table = getattr(api_cfg, "eia_series_table_clickhouse", "aurum.eia_series")
            elif backend_type == "timescale":
                table = getattr(api_cfg, "eia_series_table_timescale", "market.eia_series")
            else:
                table = "iceberg.market.eia_series"

            dims = ["dataset", "area", "sector", "unit", "canonical_unit", "canonical_currency", "frequency", "source"]
            values: Dict[str, List[str]] = {}
            for dim in dims:
                sql = f"SELECT DISTINCT {dim} FROM {table} WHERE {dim} IS NOT NULL LIMIT 1000"
                result = await backend.execute_query(sql)
                v = []
                for row in result.rows:
                    if row and row[0] is not None:
                        v.append(row[0])
                values[dim] = v
            return values
        except Exception:
            trino_cfg = TrinoConfig.from_settings(settings)
            cache_cfg = CacheConfig.from_settings(settings)
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
