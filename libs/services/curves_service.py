"""Domain service for curves queries with optional caching and DAO integration."""

from __future__ import annotations

import time
from contextlib import nullcontext
from dataclasses import asdict, dataclass
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Protocol,
)

from libs.services.base_service import BaseTrinoService
from libs.services.cache_support import (
    AsyncCacheProtocol,
    CacheReadyResult,
    CachedServiceMixin,
    RepoFetchResult,
)
from libs.services.contracts import (
    CacheDirective,
    ServiceExecutionResult,
)
from libs.storage.trino import TrinoAnalyticRepo


@dataclass(slots=True)
class Curve:
    id: str
    name: str
    description: Optional[str]
    data_points: int
    created_at: str


class CurvesDaoProtocol(Protocol):
    """Protocol describing the subset of DAO behaviour this service uses."""

    async def list_catalog_entries(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], float, Optional[str]]:
        ...

    async def get_curve_diff_details(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> Tuple[Dict[str, Any], float, Optional[str]]:
        ...

    async def export_curves(
        self,
        *,
        asof: Optional[str],
        iso: Optional[str],
        market: Optional[str],
        location: Optional[str],
        product: Optional[str],
        block: Optional[str],
    ) -> Iterable[Dict[str, Any]]:
        ...


class CurvesService(CachedServiceMixin, BaseTrinoService):
    """Curves domain service supporting caching, tracing, and DAO reuse."""

    def __init__(
        self,
        *,
        trino: Optional[TrinoAnalyticRepo] = None,
        dao: Optional[CurvesDaoProtocol] = None,
        cache: Optional[AsyncCacheProtocol] = None,
        tracer: Optional[Any] = None,
        cache_namespace: str = "curves",
    ) -> None:
        super().__init__(trino=trino)
        self._dao = dao or self._build_default_dao()
        self._cache = cache
        self._tracer = tracer
        self._cache_namespace = cache_namespace

    def _build_default_dao(self) -> Optional[CurvesDaoProtocol]:
        try:
            from src.aurum.api.dao.curves_dao import CurvesDao  # type: ignore

            return CurvesDao(self._settings)
        except Exception:  # pragma: no cover - optional dependency
            return None

    def _start_span(self, name: str):
        if self._tracer is None:
            return nullcontext()
        return self._tracer.start_as_current_span(name)

    async def _invoke_dao_or_repo(
        self,
        *,
        span_name: Optional[str],
        dao_method: str,
        fallback: Callable[..., Awaitable[Any]],
        **kwargs: Any,
    ) -> Any:
        """Invoke DAO method when available, otherwise fall back to repo helper."""

        span_ctx = self._start_span(span_name) if span_name else nullcontext()
        async def _call() -> Any:
            if self._dao and hasattr(self._dao, dao_method):
                dao_callable = getattr(self._dao, dao_method)
                return await dao_callable(**kwargs)
            return await fallback(**kwargs)

        with span_ctx:
            return await _call()


    async def _execute_timed_query(
        self,
        *,
        query: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[List[Dict[str, Any]], float]:
        """Execute a Trino query while capturing elapsed milliseconds."""

        start = time.perf_counter()
        # Support repositories that don't accept params for execute_query
        try:
            rows = await self._trino.execute_query(query, params)
        except TypeError:
            rows = await self._trino.execute_query(query)  # type: ignore[misc]
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        return rows, elapsed_ms

    @staticmethod
    def _rows_to_curves(rows: Iterable[Dict[str, Any]]) -> List[Curve]:
        curves: List[Curve] = []
        for row in rows:
            curves.append(
                Curve(
                    id=str(row.get("id", "")),
                    name=str(row.get("name", "")),
                    description=row.get("description"),
                    data_points=int(row.get("data_points", 0) or 0),
                    created_at=str(row.get("created_at", "")),
                )
            )
        return curves

    def _parse_cache_payload(
        self,
        payload: Dict[str, Any],
        *,
        data_key: str,
        singular: bool = False,
    ) -> Tuple[Any, int, Dict[str, Any]]:
        """Generic cache payload parser for list/singleton variants."""
        raw = payload.get(data_key, {} if singular else [])
        rows = [raw] if singular else raw
        converted = self._rows_to_curves(rows)
        if singular:
            return converted[0], 1, payload.get("debug", {})
        return converted, len(converted), payload.get("debug", {})

    async def list_curves(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
        include_debug: bool = False,
        cache_directive: Optional[CacheDirective] = None,
    ) -> Tuple[ServiceExecutionResult[List[Curve]], Dict[str, Any]]:
        """List curves with optional caching and metadata."""

        cache_key = self._cache_key("list", tenant_id, offset, limit, name_filter)
        async def fetch() -> RepoFetchResult:
            return await self._invoke_dao_or_repo(
                span_name="curves.list",
                dao_method="list_catalog_entries",
                fallback=self._list_curves_via_repo,
                tenant_id=tenant_id,
                offset=offset,
                limit=limit,
                name_filter=name_filter,
            )

        def build_result(rows: Iterable[Dict[str, Any]], raw_query: Optional[str]) -> CacheReadyResult[List[Curve]]:
            curves = self._rows_to_curves(rows)
            debug_meta: Dict[str, Any] = {"raw_query": raw_query} if include_debug else {}
            serialized_curves = [asdict(curve) for curve in curves]

            return CacheReadyResult(
                data=curves,
                row_count=len(curves),
                payload_factory=self._payload_factory(
                    data_key="curves",
                    data=serialized_curves,
                    debug=debug_meta or None,
                ),
                debug_payload=debug_meta,
            )

        return await self._execute_cached_operation(
            cache_directive=cache_directive,
            cache_key=cache_key,
            payload_parser=lambda payload: self._parse_cache_payload(payload, data_key="curves", singular=False),
            fetcher=fetch,
            result_builder=build_result,
            backend="trino",
        )

    async def get_curve_diff(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
        cache_directive: Optional[CacheDirective] = None,
    ) -> ServiceExecutionResult[Curve]:
        """Retrieve curve diff metadata."""

        cache_key = self._cache_key("diff", curve_id, from_timestamp, to_timestamp)
        async def fetch() -> RepoFetchResult:
            return await self._invoke_dao_or_repo(
                span_name="curves.diff",
                dao_method="get_curve_diff_details",
                fallback=self._get_curve_diff_via_repo,
                curve_id=curve_id,
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
            )

        def build_result(row: Dict[str, Any], raw_query: Optional[str]) -> CacheReadyResult[Curve]:
            curve = self._rows_to_curves([row])[0]
            serialized_curve = asdict(curve)

            return CacheReadyResult(
                data=curve,
                row_count=1,
                payload_factory=self._payload_factory(
                    data_key="curve",
                    data=serialized_curve,
                    metadata_extra={"raw_query": raw_query} if raw_query else None,
                ),
            )

        result, _ = await self._execute_cached_operation(
            cache_directive=cache_directive,
            cache_key=cache_key,
            payload_parser=lambda payload: self._parse_cache_payload(payload, data_key="curve", singular=True),
            fetcher=fetch,
            result_builder=build_result,
            backend="trino",
        )

        return result

    async def stream_curve_export(
        self,
        *,
        asof: Optional[str] = None,
        iso: Optional[str] = None,
        market: Optional[str] = None,
        location: Optional[str] = None,
        product: Optional[str] = None,
        block: Optional[str] = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream curve export rows."""

        rows = await self._invoke_dao_or_repo(
            span_name=None,
            dao_method="export_curves",
            fallback=self._export_curves_via_repo,
            asof=asof,
            iso=iso,
            market=market,
            location=location,
            product=product,
            block=block,
        )

        async def _iter_in_chunks(iterable: Iterable[Dict[str, Any]], size: int) -> AsyncIterator[Dict[str, Any]]:
            buf: List[Dict[str, Any]] = []
            for item in iterable:
                buf.append(item)
                if len(buf) >= max(1, size):
                    for x in buf:
                        yield x
                    buf.clear()
            for x in buf:
                yield x

        async for item in _iter_in_chunks(rows, chunk_size):
            yield item

    async def _list_curves_via_repo(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], float, Optional[str]]:
        sql = [
            "SELECT id, name, description, data_points, created_at",
            "FROM iceberg.market.curves_catalog",
            "WHERE tenant_id = :tenant_id",
        ]
        params: Dict[str, Any] = {"tenant_id": tenant_id, "offset": offset, "limit": limit}
        if name_filter:
            sql.append("AND name ILIKE :name_filter")
            params["name_filter"] = f"%{name_filter}%"
        sql.append("ORDER BY name OFFSET :offset LIMIT :limit")
        query = "\n".join(sql)

        rows, elapsed_ms = await self._execute_timed_query(query=query, params=params)
        return rows, elapsed_ms, query

    async def _get_curve_diff_via_repo(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> Tuple[Dict[str, Any], float, Optional[str]]:
        sql = (
            "SELECT id, name, description, data_points, created_at "
            "FROM iceberg.market.curves_diff WHERE id = :curve_id "
            "AND from_ts = :from_ts AND to_ts = :to_ts"
        )
        params = {"curve_id": curve_id, "from_ts": from_timestamp, "to_ts": to_timestamp}
        rows, elapsed_ms = await self._execute_timed_query(query=sql, params=params)
        return (rows[0] if rows else {}), elapsed_ms, sql

    async def _export_curves_via_repo(
        self,
        *,
        asof: Optional[str],
        iso: Optional[str],
        market: Optional[str],
        location: Optional[str],
        product: Optional[str],
        block: Optional[str],
    ) -> List[Dict[str, Any]]:
        conditions: List[str] = []
        params: Dict[str, Any] = {}
        if asof:
            conditions.append("asof_date = DATE :asof")
            params["asof"] = asof
        if iso:
            conditions.append("iso = :iso")
            params["iso"] = iso
        if market:
            conditions.append("market = :market")
            params["market"] = market
        if location:
            conditions.append("location = :location")
            params["location"] = location
        if product:
            conditions.append("product = :product")
            params["product"] = product
        if block:
            conditions.append("block = :block")
            params["block"] = block

        predicate = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        query = (
            "SELECT curve_key, tenor_label, tenor_type, contract_month, asof_date, mid, bid, ask, price_type "
            f"FROM iceberg.market.curves_export {predicate} ORDER BY curve_key, tenor_label, asof_date"
        )
        # Execute without params if the repository does not support them
        try:
            return await self._trino.execute_query(query, params)
        except TypeError:
            return await self._trino.execute_query(query)  # type: ignore[misc]


__all__ = ["Curve", "CurvesService"]
