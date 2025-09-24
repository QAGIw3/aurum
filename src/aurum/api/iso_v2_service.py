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
        cache_cfg: CacheConfig = CacheConfig.from_settings(settings)
        # Over-fetch to emulate offset since the underlying query supports LIMIT only
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
