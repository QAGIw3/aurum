from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from aurum.api.services.metadata_service import MetadataService


class StubCache:
    def __init__(self) -> None:
        self.store: dict[str, Any] = {}
        self.set_calls: int = 0

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: Any, ttl_seconds: int | None = None):
        self.store[key] = value
        self.set_calls += 1


@pytest.mark.asyncio
async def test_metadata_service_caches_results():
    service = MetadataService()
    stub_cache = StubCache()

    with patch.object(service, "_cache", stub_cache), patch.object(
        service._dao, "fetch_metadata_dimensions", new=AsyncMock(return_value=[{"dimension": "iso", "value": "PJM"}])
    ) as fetch_mock:
        items, total = await service.list_dimensions(asof=None, offset=0, limit=10)
        assert items
        assert total == 1
        assert fetch_mock.await_count == 1
        assert stub_cache.set_calls == 1

        items_cached, total_cached = await service.list_dimensions(asof=None, offset=0, limit=10)
        assert items_cached == items
        assert total_cached == total
        assert fetch_mock.await_count == 1


@pytest.mark.asyncio
async def test_metadata_dao_retries_on_failure():
    from aurum.api.dao.metadata_dao import MetadataDao

    dao = MetadataDao()

    calls = 0

    async def failing_query(query, params, use_cache=True):
        nonlocal calls
        calls += 1
        if calls < 2:
            raise RuntimeError("transient")
        return [
            {
                "dimension_type": "iso",
                "dimension_value": "PJM",
            }
        ]

    with patch("aurum.api.dao.metadata_dao.get_trino_client", return_value=AsyncMock(execute_query=failing_query)):
        result = await dao.query_dimensions()
        assert result["iso"] == ["PJM"]
        assert calls == 2

