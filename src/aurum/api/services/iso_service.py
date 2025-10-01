from __future__ import annotations

"""ISO domain service with DAO pattern implementation.

Phase 1.3 Service Layer Decomposition: Handles ISO LMP data queries and caching.
Migrated from legacy service.py functions: query_iso_lmp_* family.
"""

import hashlib
import json
import time
from typing import Any, Dict, List, Optional, Tuple

from aurum.telemetry import get_tracer

from .base_service import QueryableServiceInterface
from ..config import CacheConfig
from ..database.timescale_client import get_timescale_client
from ..cache.unified_cache_manager import get_unified_cache_manager
from ..cache.enhanced_cache_manager import CacheNamespace
from ..cache.utils import cache_key as _cache_key, iso_lmp_cache_key, iso_lmp_effective_ttl
from ..logging.structured_logger import get_logger
from ..telemetry.context import log_structured

logger = get_logger(__name__)

try:  # pragma: no cover - optional dependency
    from prometheus_client import Counter as _PromCounter
except Exception:  # pragma: no cover - metrics optional
    _PromCounter = None  # type: ignore[assignment]

if _PromCounter:
    ISO_CACHE_HITS = _PromCounter(
        "aurum_iso_cache_hits_total",
        "ISO cache hits",
    )
    ISO_CACHE_MISSES = _PromCounter(
        "aurum_iso_cache_misses_total",
        "ISO cache misses",
    )
else:  # pragma: no cover - metrics optional
    ISO_CACHE_HITS = None  # type: ignore[assignment]
    ISO_CACHE_MISSES = None  # type: ignore[assignment]


class IsoService(QueryableServiceInterface):
    """ISO domain service implementing business logic and data access through DAO.

    Handles ISO LMP (Locational Marginal Pricing) data with comprehensive caching.
    """

    def __init__(self):
        self._cache_manager = get_unified_cache_manager()
        self._timescale = get_timescale_client()

    async def lmp_last_24h(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get last 24h LMP data with caching."""
        start_time = time.perf_counter()

        # Build cache key using legacy function for compatibility
        params = {
            "operation": "lmp_last_24h",
            "iso_code": iso_code,
            "market": market,
            "location_id": location_id,
            "limit": limit,
        }

        # Use legacy cache key generation for compatibility
        cache_key = _iso_lmp_cache_key("lmp_last_24h", params, "", "iso_lmp")

        try:
            # Try cache first
            cached_result = await self._cache_manager.get(
                cache_key,
                namespace=CacheNamespace.ISO_DATA
            )

            if cached_result is not None:
                if ISO_CACHE_HITS:
                    ISO_CACHE_HITS.inc()
                return cached_result, (time.perf_counter() - start_time)

            # Cache miss - query data
            if ISO_CACHE_MISSES:
                ISO_CACHE_MISSES.inc()

            # Execute directly against Timescale warehouse (materialized view)
            where = ["1=1"]
            params: Dict[str, Any] = {}
            if iso_code:
                where.append("iso_code = %(iso_code)s")
                params["iso_code"] = iso_code.upper()
            if market:
                where.append("market = %(market)s")
                params["market"] = market.upper()
            if location_id:
                where.append("upper(location_id) = upper(%(location_id)s)")
                params["location_id"] = location_id
            sql = (
                "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
                "location_id, location_name, location_type, price_total, price_energy, price_congestion, "
                "price_loss, currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
                "FROM public.iso_lmp_last_24h WHERE " + " AND ".join(where) +
                " ORDER BY interval_start DESC LIMIT %(limit)s"
            )
            params["limit"] = min(limit, 2000)
            results = await self._timescale.execute_query(sql, params)
            query_time = 0.0

            # Cache the result
            ttl_seconds = _iso_lmp_effective_ttl(cache_cfg)
            await self._cache_manager.set(
                cache_key,
                (results, query_time),
                namespace=CacheNamespace.ISO_DATA,
                ttl_seconds=ttl_seconds
            )

            total_duration = time.perf_counter() - start_time
            LOGGER.debug("ISO LMP last 24h query completed",
                        query_time_ms=query_time * 1000,
                        total_time_ms=total_duration * 1000,
                        result_count=len(results))

            return results, total_duration

        except Exception as exc:
            total_duration = time.perf_counter() - start_time
            LOGGER.error("ISO LMP last 24h query failed",
                        error=str(exc),
                        total_time_ms=total_duration * 1000)
            raise

    async def lmp_hourly(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        date: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get hourly LMP data with caching."""
        start_time = time.perf_counter()

        params = {
            "operation": "lmp_hourly",
            "iso_code": iso_code,
            "market": market,
            "location_id": location_id,
            "date": date,
            "limit": limit,
        }

        cache_key = _iso_lmp_cache_key("lmp_hourly", params, "", "iso_lmp")

        try:
            cached_result = await self._cache_manager.get(
                cache_key,
                namespace=CacheNamespace.ISO_DATA
            )

            if cached_result is not None:
                if ISO_CACHE_HITS:
                    ISO_CACHE_HITS.inc()
                return cached_result, (time.perf_counter() - start_time)

            if ISO_CACHE_MISSES:
                ISO_CACHE_MISSES.inc()

            from aurum.api.state import get_settings as _api_get_settings

            cache_cfg = CacheConfig.from_settings(_api_get_settings())
            where = ["1=1"]
            params: Dict[str, Any] = {"limit": min(limit, 2000)}
            if iso_code:
                where.append("iso_code = %(iso_code)s")
                params["iso_code"] = iso_code.upper()
            if market:
                where.append("market = %(market)s")
                params["market"] = market.upper()
            if location_id:
                where.append("upper(location_id) = upper(%(location_id)s)")
                params["location_id"] = location_id
            if date:
                where.append("interval_start::date = DATE %(date)s")
                params["date"] = date
            sql = (
                "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
                "price_max, price_stddev, sample_count FROM public.iso_lmp_hourly WHERE "
                + " AND ".join(where) + " ORDER BY interval_start DESC LIMIT %(limit)s"
            )
            results = await self._timescale.execute_query(sql, params)
            query_time = 0.0

            ttl_seconds = _iso_lmp_effective_ttl(cache_cfg)
            await self._cache_manager.set(
                cache_key,
                (results, query_time),
                namespace=CacheNamespace.ISO_DATA,
                ttl_seconds=ttl_seconds
            )

            total_duration = time.perf_counter() - start_time
            LOGGER.debug("ISO LMP hourly query completed",
                        query_time_ms=query_time * 1000,
                        total_time_ms=total_duration * 1000,
                        result_count=len(results))

            return results, total_duration

        except Exception as exc:
            total_duration = time.perf_counter() - start_time
            LOGGER.error("ISO LMP hourly query failed",
                        error=str(exc),
                        total_time_ms=total_duration * 1000)
            raise

    async def lmp_daily(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get daily LMP data with caching."""
        start_time = time.perf_counter()

        params = {
            "operation": "lmp_daily",
            "iso_code": iso_code,
            "market": market,
            "location_id": location_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
        }

        cache_key = _iso_lmp_cache_key("lmp_daily", params, "", "iso_lmp")

        try:
            cached_result = await self._cache_manager.get(
                cache_key,
                namespace=CacheNamespace.ISO_DATA
            )

            if cached_result is not None:
                if ISO_CACHE_HITS:
                    ISO_CACHE_HITS.inc()
                return cached_result, (time.perf_counter() - start_time)

            if ISO_CACHE_MISSES:
                ISO_CACHE_MISSES.inc()

            from aurum.api.state import get_settings as _api_get_settings

            cache_cfg = CacheConfig.from_settings(_api_get_settings())
            where = ["1=1"]
            params: Dict[str, Any] = {"limit": min(limit, 2000)}
            if iso_code:
                where.append("iso_code = %(iso_code)s")
                params["iso_code"] = iso_code.upper()
            if market:
                where.append("market = %(market)s")
                params["market"] = market.upper()
            if location_id:
                where.append("upper(location_id) = upper(%(location_id)s)")
                params["location_id"] = location_id
            if start_date:
                where.append("interval_start >= %(start)s")
                params["start"] = start_date
            if end_date:
                where.append("interval_start <= %(end)s")
                params["end"] = end_date
            sql = (
                "SELECT iso_code, market, interval_start, location_id, currency, uom, price_avg, price_min, "
                "price_max, price_stddev, sample_count FROM public.iso_lmp_daily WHERE "
                + " AND ".join(where) + " ORDER BY interval_start DESC LIMIT %(limit)s"
            )
            results = await self._timescale.execute_query(sql, params)
            query_time = 0.0

            ttl_seconds = _iso_lmp_effective_ttl(cache_cfg)
            await self._cache_manager.set(
                cache_key,
                (results, query_time),
                namespace=CacheNamespace.ISO_DATA,
                ttl_seconds=ttl_seconds
            )

            total_duration = time.perf_counter() - start_time
            LOGGER.debug("ISO LMP daily query completed",
                        query_time_ms=query_time * 1000,
                        total_time_ms=total_duration * 1000,
                        result_count=len(results))

            return results, total_duration

        except Exception as exc:
            total_duration = time.perf_counter() - start_time
            LOGGER.error("ISO LMP daily query failed",
                        error=str(exc),
                        total_time_ms=total_duration * 1000)
            raise

    async def lmp_negative(
        self,
        *,
        iso_code: Optional[str] = None,
        market: Optional[str] = None,
        location_id: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 500
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Get negative LMP data with caching."""
        start_time = time.perf_counter()

        params = {
            "operation": "lmp_negative",
            "iso_code": iso_code,
            "market": market,
            "location_id": location_id,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
        }

        cache_key = _iso_lmp_cache_key("lmp_negative", params, "", "iso_lmp")

        try:
            cached_result = await self._cache_manager.get(
                cache_key,
                namespace=CacheNamespace.ISO_DATA
            )

            if cached_result is not None:
                if ISO_CACHE_HITS:
                    ISO_CACHE_HITS.inc()
                return cached_result, (time.perf_counter() - start_time)

            if ISO_CACHE_MISSES:
                ISO_CACHE_MISSES.inc()

            from aurum.api.state import get_settings as _api_get_settings

            cache_cfg = CacheConfig.from_settings(_api_get_settings())
            where = ["1=1"]
            params: Dict[str, Any] = {"limit": min(limit, 1000)}
            if iso_code:
                where.append("iso_code = %(iso_code)s")
                params["iso_code"] = iso_code.upper()
            if market:
                where.append("market = %(market)s")
                params["market"] = market.upper()
            if location_id:
                where.append("upper(location_id) = upper(%(location_id)s)")
                params["location_id"] = location_id
            if start_date:
                where.append("delivery_date >= %(start)s")
                params["start"] = start_date
            if end_date:
                where.append("delivery_date <= %(end)s")
                params["end"] = end_date
            sql = (
                "SELECT iso_code, market, delivery_date, interval_start, interval_end, interval_minutes, "
                "location_id, location_name, location_type, price_total, price_energy, price_congestion, price_loss, "
                "currency, uom, settlement_point, source_run_id, ingest_ts, record_hash, metadata "
                "FROM public.iso_lmp_negative_7d WHERE " + " AND ".join(where) +
                " ORDER BY price_total ASC LIMIT %(limit)s"
            )
            results = await self._timescale.execute_query(sql, params)
            query_time = 0.0

            ttl_seconds = _iso_lmp_effective_ttl(cache_cfg)
            await self._cache_manager.set(
                cache_key,
                (results, query_time),
                namespace=CacheNamespace.ISO_DATA,
                ttl_seconds=ttl_seconds
            )

            total_duration = time.perf_counter() - start_time
            LOGGER.debug("ISO LMP negative query completed",
                        query_time_ms=query_time * 1000,
                        total_time_ms=total_duration * 1000,
                        result_count=len(results))

            return results, total_duration

        except Exception as exc:
            total_duration = time.perf_counter() - start_time
            LOGGER.error("ISO LMP negative query failed",
                        error=str(exc),
                        total_time_ms=total_duration * 1000)
            raise

    async def query_data(
        self,
        *,
        offset: int = 0,
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query ISO data with pagination and filtering (QueryableServiceInterface)."""
        # Default implementation - can be extended based on specific needs
        return []

