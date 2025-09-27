from __future__ import annotations

"""Service layer for v2 Drought endpoints."""

from datetime import date as _date
from typing import Any, Dict, List, Optional

from .config import TrinoConfig
from .services.drought_service import DroughtService
from .state import get_settings
from .database.backend_selector import get_data_backend


class DroughtV2Service:
    async def list_indices(
        self,
        *,
        dataset: Optional[str],
        index: Optional[str],
        timescale: Optional[str],
        region: Optional[str],  # unused placeholder for future region sets
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        drought_service = DroughtService()

        def _to_date(value: Optional[str]) -> Optional[_date]:
            if not value:
                return None
            try:
                return _date.fromisoformat(value)
            except Exception:
                return None

        # For non-Trino backends, attempt direct query; otherwise use legacy path
        try:
            backend = get_data_backend(settings)
            bt = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if bt == "trino":
                raise RuntimeError("prefer legacy path for trino")
            api_cfg = getattr(settings, "api", None)
            if bt == "clickhouse":
                table = getattr(api_cfg, "drought_index_table_clickhouse", "aurum.drought_index")
                geo_table = getattr(api_cfg, "geographies_table_clickhouse", "aurum.geographies")
            elif bt == "timescale":
                table = getattr(api_cfg, "drought_index_table_timescale", "public.drought_index")
                geo_table = getattr(api_cfg, "geographies_table_timescale", "ref.geographies")
            else:
                table = getattr(api_cfg, "drought_index_table_trino", "environment.drought_index")
                geo_table = getattr(api_cfg, "geographies_table_trino", "ref.geographies")

            where = []
            if region_type:
                where.append(f"region_type = '{region_type}'")
            if region_id:
                where.append(f"region_id = '{region_id}'")
            if dataset:
                where.append(f"dataset = '{dataset}'")
            if index:
                where.append(f"\"index\" = '{index}'")
            if timescale:
                where.append(f"timescale = '{timescale}'")
            if start:
                where.append(f"valid_date >= DATE '{_to_date(start).isoformat() if _to_date(start) else start}'")
            if end:
                where.append(f"valid_date <= DATE '{_to_date(end).isoformat() if _to_date(end) else end}'")
            where_clause = (" WHERE " + " AND ".join(where)) if where else ""
            sql = (
                f"SELECT di.series_id, di.dataset, di.\"index\", di.timescale, di.valid_date, di.as_of, di.value, di.unit, di.poc, di.region_type, di.region_id, g.region_name, g.parent_region_id, di.source_url "
                f"FROM {table} AS di LEFT JOIN {geo_table} AS g ON g.region_type = di.region_type AND g.region_id = di.region_id"
                f"{where_clause} ORDER BY di.valid_date DESC, di.series_id LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            result = await backend.execute_query(sql)
            rows = []
            for r in result.rows:
                rows.append(
                    {
                        "series_id": r[0],
                        "dataset": r[1],
                        "index": r[2],
                        "timescale": r[3],
                        "valid_date": str(r[4]),
                        "as_of": str(r[5]) if len(r) > 5 else None,
                        "value": r[6] if len(r) > 6 else None,
                        "unit": r[7] if len(r) > 7 else None,
                        "poc": r[8] if len(r) > 8 else None,
                        "region_type": r[9] if len(r) > 9 else None,
                        "region_id": r[10] if len(r) > 10 else None,
                        "region_name": r[11] if len(r) > 11 else None,
                        "parent_region_id": r[12] if len(r) > 12 else None,
                        "source_url": r[13] if len(r) > 13 else None,
                        "metadata": None,
                    }
                )
            return rows
        except Exception:
            # Legacy path via Trino helper
            fetch_limit = max(1, int(offset) + int(limit))
            rows, _elapsed = await drought_service.query_indices(
                region_type=region_type,
                region_id=region_id,
                dataset=dataset,
                index_id=index,
                timescale=timescale,
                start_date=_to_date(start),
                end_date=_to_date(end),
                limit=fetch_limit,
                trino_cfg=trino_cfg,
            )
            return rows[int(offset) : int(offset) + int(limit)]

    async def list_usdm(
        self,
        *,
        region_type: Optional[str],
        region_id: Optional[str],
        start: Optional[str],
        end: Optional[str],
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        settings = get_settings()
        trino_cfg = TrinoConfig.from_settings(settings)
        drought_service = DroughtService()

        def _to_date(value: Optional[str]) -> Optional[_date]:
            if not value:
                return None
            try:
                return _date.fromisoformat(value)
            except Exception:
                return None

        try:
            backend = get_data_backend(settings)
            bt = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if bt == "trino":
                raise RuntimeError("prefer legacy path for trino")
            api_cfg = getattr(settings, "api", None)
            if bt == "clickhouse":
                table = getattr(api_cfg, "usdm_area_table_clickhouse", "aurum.usdm_area")
            elif bt == "timescale":
                table = getattr(api_cfg, "usdm_area_table_timescale", "public.usdm_area")
            else:
                table = getattr(api_cfg, "usdm_area_table_trino", "environment.usdm_area")
            where = []
            if region_type:
                where.append(f"region_type = '{region_type}'")
            if region_id:
                where.append(f"region_id = '{region_id}'")
            if start:
                where.append(f"valid_date >= DATE '{_to_date(start).isoformat() if _to_date(start) else start}'")
            if end:
                where.append(f"valid_date <= DATE '{_to_date(end).isoformat() if _to_date(end) else end}'")
            where_clause = (" WHERE " + " AND ".join(where)) if where else ""
            sql = (
                f"SELECT region_type, region_id, valid_date, d0_frac, d1_frac, d2_frac, d3_frac, d4_frac FROM {table}{where_clause} "
                f"ORDER BY valid_date DESC LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            result = await backend.execute_query(sql)
            rows = [
                {
                    "region_type": r[0],
                    "region_id": r[1],
                    "valid_date": str(r[2]),
                    "d0_frac": r[3] if len(r) > 3 else None,
                    "d1_frac": r[4] if len(r) > 4 else None,
                    "d2_frac": r[5] if len(r) > 5 else None,
                    "d3_frac": r[6] if len(r) > 6 else None,
                    "d4_frac": r[7] if len(r) > 7 else None,
                }
                for r in result.rows
            ]
            return rows
        except Exception:
            fetch_limit = max(1, int(offset) + int(limit))
            rows, _elapsed = await drought_service.query_usdm(
                region_type=region_type,
                region_id=region_id,
                start_date=_to_date(start),
                end_date=_to_date(end),
                limit=fetch_limit,
                trino_cfg=trino_cfg,
            )
            return rows[int(offset) : int(offset) + int(limit)]


async def get_drought_service() -> DroughtV2Service:
    return DroughtV2Service()
