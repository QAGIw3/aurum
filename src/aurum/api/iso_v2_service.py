from __future__ import annotations

"""Service layer for v2 ISO endpoints (LMP data)."""

from datetime import datetime, date as _date
from typing import Any, Dict, List, Optional, Tuple

from .config import CacheConfig
from .state import get_settings
from .service import (
    query_iso_lmp_last_24h,
    query_iso_lmp_hourly,
    query_iso_lmp_daily,
)
from .database.backend_selector import get_data_backend


class IsoV2Service:
    async def lmp_last_24h(
        self,
        *,
        iso: str,
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Return last-24h LMP rows for an ISO with paging via slicing."""
        settings = get_settings()
        # Try backend selector first; fall back to Timescale-backed helper
        try:
            backend = get_data_backend(settings)
            api_cfg = getattr(settings, "api", None)
            bt = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if bt == "trino":
                table = getattr(api_cfg, "iso_lmp_last24h_table_trino", "environment.iso_lmp_last_24h")
            elif bt == "clickhouse":
                table = getattr(api_cfg, "iso_lmp_last24h_table_clickhouse", "aurum.iso_lmp_last_24h")
            else:
                table = getattr(api_cfg, "iso_lmp_last24h_table_timescale", "public.iso_lmp_last_24h")
            where = [f"iso_code = '{iso.upper()}'"]
            where_clause = (" WHERE " + " AND ".join(where)) if where else ""
            sql = (
                f"SELECT interval_start, location_id, price_total, uom FROM {table}{where_clause} "
                f"ORDER BY interval_start DESC LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            result = await backend.execute_query(sql)
            items = []
            for r in result.rows:
                ts, loc, val, unit = r[0], r[1], r[2], r[3] if len(r) > 3 else None
                items.append({
                    "timestamp": str(ts),
                    "location_id": str(loc),
                    "value": float(val) if val is not None else 0.0,
                    "unit": unit or "USD/MWh",
                })
            return items
        except Exception:
            cache_cfg: CacheConfig = CacheConfig.from_settings(settings)
            fetch_limit = max(1, int(offset) + int(limit))
            rows, _elapsed = query_iso_lmp_last_24h(
                iso_code=iso,
                market=None,
                location_id=None,
                limit=fetch_limit,
                cache_cfg=cache_cfg,
            )
            return rows[int(offset) : int(offset) + int(limit)]

    async def lmp_hourly(
        self,
        *,
        iso: str,
        date: str,
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """Return hourly LMP aggregates for a given date with paging via slicing."""
        settings = get_settings()
        cache_cfg: CacheConfig = CacheConfig.from_settings(settings)
        # Interpret date as YYYY-MM-DD
        try:
            d = _date.fromisoformat(date)
        except Exception:
            # If parsing fails, default to today to keep endpoint resilient
            d = _date.today()
        start_dt = datetime(d.year, d.month, d.day, 0, 0, 0)
        end_dt = datetime(d.year, d.month, d.day, 23, 59, 59)
        try:
            backend = get_data_backend(settings)
            api_cfg = getattr(settings, "api", None)
            bt = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if bt == "trino":
                table = getattr(api_cfg, "iso_lmp_hourly_table_trino", "environment.iso_lmp_hourly")
            elif bt == "clickhouse":
                table = getattr(api_cfg, "iso_lmp_hourly_table_clickhouse", "aurum.iso_lmp_hourly")
            else:
                table = getattr(api_cfg, "iso_lmp_hourly_table_timescale", "public.iso_lmp_hourly")
            where = [f"iso_code = '{iso.upper()}'"]
            where.append(f"interval_start >= TIMESTAMP '{start_dt.isoformat(sep=' ')}'")
            where.append(f"interval_start <= TIMESTAMP '{end_dt.isoformat(sep=' ')}'")
            where_clause = " WHERE " + " AND ".join(where)
            sql = (
                f"SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, price_max, price_stddev, sample_count "
                f"FROM {table}{where_clause} ORDER BY interval_start DESC LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            result = await backend.execute_query(sql)
            rows = [
                {
                    "iso_code": r[0],
                    "market": r[1],
                    "interval_start": r[2],
                    "location_id": r[3],
                    "currency": r[4],
                    "uom": r[5],
                    "price_avg": r[6],
                    "price_min": r[7],
                    "price_max": r[8],
                    "price_stddev": r[9],
                    "sample_count": r[10],
                }
                for r in result.rows
            ]
            return rows
        except Exception:
            fetch_limit = max(1, int(offset) + int(limit))
            rows, _elapsed = query_iso_lmp_hourly(
                iso_code=iso,
                market=None,
                location_id=None,
                start=start_dt,
                end=end_dt,
                limit=fetch_limit,
                cache_cfg=cache_cfg,
            )
            return rows[int(offset) : int(offset) + int(limit)]

    async def lmp_daily(
        self,
        *,
        iso: str,
        date: str,
        offset: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        settings = get_settings()
        cache_cfg: CacheConfig = CacheConfig.from_settings(settings)
        try:
            d = _date.fromisoformat(date)
        except Exception:
            d = _date.today()
        start_dt = datetime(d.year, d.month, d.day, 0, 0, 0)
        end_dt = datetime(d.year, d.month, d.day, 23, 59, 59)
        try:
            backend = get_data_backend(settings)
            api_cfg = getattr(settings, "api", None)
            bt = getattr(settings.data_backend.backend_type, "value", str(settings.data_backend.backend_type))
            if bt == "trino":
                table = getattr(api_cfg, "iso_lmp_daily_table_trino", "environment.iso_lmp_daily")
            elif bt == "clickhouse":
                table = getattr(api_cfg, "iso_lmp_daily_table_clickhouse", "aurum.iso_lmp_daily")
            else:
                table = getattr(api_cfg, "iso_lmp_daily_table_timescale", "public.iso_lmp_daily")
            where = [f"iso_code = '{iso.upper()}'"]
            where.append(f"interval_start >= TIMESTAMP '{start_dt.isoformat(sep=' ')}'")
            where.append(f"interval_start <= TIMESTAMP '{end_dt.isoformat(sep=' ')}'")
            where_clause = " WHERE " + " AND ".join(where)
            sql = (
                f"SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, price_max, price_stddev, sample_count "
                f"FROM {table}{where_clause} ORDER BY interval_start DESC LIMIT {int(limit)} OFFSET {int(offset)}"
            )
            result = await backend.execute_query(sql)
            rows = [
                {
                    "iso_code": r[0],
                    "market": r[1],
                    "interval_start": r[2],
                    "location_id": r[3],
                    "currency": r[4],
                    "uom": r[5],
                    "price_avg": r[6],
                    "price_min": r[7],
                    "price_max": r[8],
                    "price_stddev": r[9],
                    "sample_count": r[10],
                }
                for r in result.rows
            ]
            return rows
        except Exception:
            fetch_limit = max(1, int(offset) + int(limit))
            rows, _elapsed = query_iso_lmp_daily(
                iso_code=iso,
                market=None,
                location_id=None,
                start=start_dt,
                end=end_dt,
                limit=fetch_limit,
                cache_cfg=cache_cfg,
            )
            return rows[int(offset) : int(offset) + int(limit)]


async def get_iso_service() -> IsoV2Service:
    return IsoV2Service()
