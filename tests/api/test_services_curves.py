from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pytest

from libs.services.cache_support import AsyncCacheProtocol
from libs.services.contracts import CacheDirective, CacheStatus, ServiceExecutionResult
from libs.services.curves_service import Curve, CurvesService


class InMemoryAsyncCache(AsyncCacheProtocol):
    """Minimal in-memory cache used to exercise service caching behaviour."""

    def __init__(self) -> None:
        self._storage: Dict[Tuple[Optional[str], str], Any] = {}

    async def get(self, key: str, *, namespace: Optional[str] = None) -> Optional[Any]:
        return self._storage.get((namespace, key))

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl_seconds: Optional[int] = None,
        namespace: Optional[str] = None,
    ) -> bool:
        self._storage[(namespace, key)] = value
        return True

    async def invalidate(self, key: str, *, namespace: Optional[str] = None) -> int:
        return 1 if self._storage.pop((namespace, key), None) is not None else 0


@dataclass
class _DaoCall:
    name: str
    args: Tuple[Any, ...]
    kwargs: Dict[str, Any]


class StubCurvesDao:
    """DAO stub that records calls and returns configured payloads."""

    def __init__(self, *, rows: List[Dict[str, Any]], elapsed_ms: float = 12.5) -> None:
        self._rows = rows
        self._elapsed = elapsed_ms
        self.calls: List[_DaoCall] = []

    async def list_catalog_entries(
        self,
        *,
        tenant_id: str,
        offset: int,
        limit: int,
        name_filter: Optional[str] = None,
    ) -> Tuple[List[Dict[str, Any]], float, Optional[str]]:
        self.calls.append(
            _DaoCall("list_catalog_entries", tuple(), dict(tenant_id=tenant_id, offset=offset, limit=limit, name_filter=name_filter))
        )
        return self._rows, self._elapsed, "SELECT ..."

    async def get_curve_diff_details(
        self,
        *,
        curve_id: str,
        from_timestamp: str,
        to_timestamp: str,
    ) -> Tuple[Dict[str, Any], float, Optional[str]]:
        self.calls.append(
            _DaoCall(
                "get_curve_diff_details",
                tuple(),
                dict(curve_id=curve_id, from_timestamp=from_timestamp, to_timestamp=to_timestamp),
            )
        )
        row = self._rows[0] if self._rows else {}
        return row, self._elapsed, "SELECT diff ..."

    async def export_curves(
        self,
        *,
        asof: Optional[str],
        iso: Optional[str],
        market: Optional[str],
        location: Optional[str],
        product: Optional[str],
        block: Optional[str],
    ) -> List[Dict[str, Any]]:
        self.calls.append(
            _DaoCall(
                "export_curves",
                tuple(),
                dict(asof=asof, iso=iso, market=market, location=location, product=product, block=block),
            )
        )
        return self._rows


@pytest.mark.asyncio
async def test_list_curves_hits_cache() -> None:
    cached_payload = {
        "curves": [
            {
                "id": "c-1",
                "name": "Cached",
                "description": None,
                "data_points": 1,
                "created_at": "2024-01-01T00:00:00Z",
            }
        ],
        "debug": {},
        "metadata": {"elapsed_ms": 0.5, "backend": "redis"},
    }
    cache = InMemoryAsyncCache()
    await cache.set(
        "curves:list:tenant:0:5:None",
        cached_payload,
        namespace="curves",
        ttl_seconds=30,
    )

    dao = StubCurvesDao(rows=[])
    svc = CurvesService(dao=dao, cache=cache)
    directive = CacheDirective(namespace="curves", ttl_seconds=30)

    result, debug = await svc.list_curves(
        tenant_id="tenant",
        offset=0,
        limit=5,
        name_filter=None,
        include_debug=True,
        cache_directive=directive,
    )

    assert isinstance(result, ServiceExecutionResult)
    assert result.metadata.cache_status is CacheStatus.HIT
    assert debug == {}
    assert [curve.id for curve in result.data] == ["c-1"]
    assert dao.calls == []


@pytest.mark.asyncio
async def test_list_curves_populates_cache() -> None:
    rows = [
        {"id": "c-2", "name": "Fresh", "description": "", "data_points": 3, "created_at": "2024-02-01T00:00:00Z"}
    ]
    cache = InMemoryAsyncCache()
    dao = StubCurvesDao(rows=rows)
    svc = CurvesService(dao=dao, cache=cache)
    directive = CacheDirective(namespace="curves", ttl_seconds=45)

    result, debug = await svc.list_curves(
        tenant_id="tenant",
        offset=0,
        limit=10,
        name_filter=None,
        include_debug=True,
        cache_directive=directive,
    )

    assert result.metadata.cache_status is CacheStatus.MISS
    assert [curve.id for curve in result.data] == ["c-2"]
    assert debug == {"raw_query": "SELECT ..."}
    assert dao.calls and dao.calls[0].name == "list_catalog_entries"

    # Confirm cache populated with serialised payload
    cached = await cache.get("curves:list:tenant:0:10:None", namespace="curves")
    assert cached is not None
    assert cached["curves"][0]["id"] == "c-2"


@pytest.mark.asyncio
async def test_get_curve_diff_miss_then_store() -> None:
    row = {"id": "c-3", "name": "Curve", "description": None, "data_points": 7, "created_at": "2024-03-01"}
    dao = StubCurvesDao(rows=[row])
    cache = InMemoryAsyncCache()
    svc = CurvesService(dao=dao, cache=cache)
    directive = CacheDirective(namespace="curves", ttl_seconds=20)

    result = await svc.get_curve_diff(
        curve_id="c-3",
        from_timestamp="2024-01-01T00:00:00Z",
        to_timestamp="2024-01-31T00:00:00Z",
        cache_directive=directive,
    )

    assert isinstance(result, ServiceExecutionResult)
    assert isinstance(result.data, Curve)
    assert result.metadata.cache_status is CacheStatus.MISS

    cached = await cache.get("curves:diff:c-3:2024-01-01T00:00:00Z:2024-01-31T00:00:00Z", namespace="curves")
    assert cached and cached["curve"]["id"] == "c-3"


def test_api_service_alias() -> None:
    from aurum.api.services import CurvesService as ApiCurvesService

    assert ApiCurvesService is CurvesService


@pytest.mark.asyncio
async def test_stream_curve_export_iterates_chunks() -> None:
    rows = [{"curve_key": "c1"}, {"curve_key": "c2"}, {"curve_key": "c3"}]
    dao = StubCurvesDao(rows=rows)
    svc = CurvesService(dao=dao)

    collected: List[Dict[str, Any]] = []
    async for item in svc.stream_curve_export(chunk_size=2):
        collected.append(item)

    assert collected == rows
*** End of File
