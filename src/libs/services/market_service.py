from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from .storage import TrinoAnalyticRepo, CacheRepository, RedisCacheRepo
from .services.base_service import BaseTrinoService


class MarketService(BaseTrinoService):
    """Domain service for market-oriented read queries (curves/latest, asof, diffs, scenario output view)."""

    def __init__(self, trino: Optional[TrinoAnalyticRepo] = None, cache: Optional[CacheRepository] = None) -> None:
        super().__init__(trino=trino)
        # Simple K/V cache repo (optional)
        self._cache = cache or RedisCacheRepo(self._settings.redis)
        self._catalog = self._settings.database.trino_catalog
        self._schema = "market"

    async def _cached_query(
        self,
        *,
        cache_key: str,
        query: str,
        ttl_seconds: int,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """Fetch rows using the shared cache-then-query pattern used across handlers."""

        cached = await self._cache.get(cache_key)
        if cached:
            return cached, {}

        rows = await self._trino.execute_query(query)
        await self._cache.set(cache_key, rows, ttl_seconds=ttl_seconds)
        return rows, {}

    async def curves_latest(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        include_debug: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = f"{self._catalog}.{self._schema}.curves_latest"
        query = (
            "SELECT tenant_id, curve_key, tenor_label, tenor_type, contract_month, asof_date, "
            "currency, per_unit, mid, bid, ask, version_hash "
            f"FROM {table} WHERE tenant_id = '{tenant_id}' ORDER BY curve_key, tenor_label OFFSET {offset} LIMIT {limit}"
        )
        cache_key = f"market:curves_latest:{tenant_id}:{offset}:{limit}"
        return await self._cached_query(
            cache_key=cache_key,
            query=query,
            ttl_seconds=self._settings.cache.curve_data_ttl,
        )

    async def curves_asof(
        self,
        *,
        tenant_id: str,
        asof_date: Optional[str],
        offset: int,
        limit: int,
        include_debug: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = f"{self._catalog}.{self._schema}.curves_asof"
        where = [f"tenant_id = '{tenant_id}'"]
        if asof_date:
            where.append(f"asof_date = DATE '{asof_date}'")
        predicate = " AND ".join(where)
        query = (
            "SELECT tenant_id, curve_key, contract_month, asof_date, mid, bid, ask "
            f"FROM {table} WHERE {predicate} ORDER BY curve_key, contract_month, asof_date OFFSET {offset} LIMIT {limit}"
        )
        cache_key = f"market:curves_asof:{tenant_id}:{asof_date}:{offset}:{limit}"
        return await self._cached_query(
            cache_key=cache_key,
            query=query,
            ttl_seconds=self._settings.cache.curve_data_ttl,
        )

    async def curves_asof_diff(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        include_debug: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = f"{self._catalog}.{self._schema}.curves_asof_diff"
        query = (
            "SELECT tenant_id, curve_key, contract_month, asof_date_new, mid_new, asof_date_old, mid_old, mid_diff "
            f"FROM {table} WHERE tenant_id = '{tenant_id}' ORDER BY curve_key, contract_month, asof_date_new OFFSET {offset} LIMIT {limit}"
        )
        cache_key = f"market:curves_asof_diff:{tenant_id}:{offset}:{limit}"
        return await self._cached_query(
            cache_key=cache_key,
            query=query,
            ttl_seconds=self._settings.cache.curve_data_ttl,
        )

    async def scenario_output_view(
        self,
        *,
        tenant_id: str,
        scenario_id: Optional[str],
        metric: Optional[str],
        offset: int,
        limit: int,
        include_debug: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        table = f"{self._catalog}.{self._schema}.scenario_output_view"
        where = [f"tenant_id = '{tenant_id}'"]
        if scenario_id:
            where.append(f"scenario_id = '{scenario_id}'")
        if metric:
            where.append(f"metric = '{metric}'")
        predicate = " AND ".join(where)
        query = (
            "SELECT tenant_id, scenario_id, run_id, curve_key, metric, tenor_label, asof_date, value, band_lower, band_upper, computed_ts "
            f"FROM {table} WHERE {predicate} ORDER BY curve_key, tenor_label, asof_date OFFSET {offset} LIMIT {limit}"
        )
        cache_key = f"market:scenario_output_view:{tenant_id}:{scenario_id}:{metric}:{offset}:{limit}"
        return await self._cached_query(
            cache_key=cache_key,
            query=query,
            ttl_seconds=self._settings.cache.scenario_data_ttl,
        )
