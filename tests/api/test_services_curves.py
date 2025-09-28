"""Tests for Curves service layer implementation."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from aurum.api.cache.unified_cache_manager import UnifiedCacheManager
from aurum.api.contracts import CacheDirective, CacheStatus, CurvesDiffQuery, Pagination, QueryResult, ServiceCallContext
from aurum.api.dao.curves_dao import CurvesDao
from aurum.api.services.curves_service import CurvesService
from datetime import date


class TestCurvesService:
    """Unit tests for the Curves service contract implementation."""

    @pytest.fixture
    def mock_dao(self, monkeypatch):
        dao = AsyncMock(spec=CurvesDao)
        monkeypatch.setattr("aurum.api.services.curves_service.CurvesDao", lambda: dao)
        return dao

    @pytest.fixture
    def curves_service(self):
        return CurvesService()

    @pytest.mark.asyncio
    async def test_query_data_cache_hit(self, curves_service, mock_dao, monkeypatch):
        cached_payload = [{"iso": "CAISO", "value": 50.0}]
        manager = AsyncMock(spec=UnifiedCacheManager)
        manager.get = AsyncMock(return_value=cached_payload)
        manager.config = SimpleNamespace(ttl_seconds=30)
        monkeypatch.setattr("aurum.api.services.curves_service.get_unified_cache_manager", lambda: manager)

        result = await curves_service.query_data(
            filters={"iso": "CAISO"},
            context=ServiceCallContext(cache_directive=CacheDirective(namespace="curves", ttl_seconds=60)),
        )

        assert result.data == cached_payload
        assert result.metadata.cache_status == CacheStatus.HIT
        mock_dao.query_curves.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_query_data_cache_miss_fetches_and_stores(self, curves_service, mock_dao, monkeypatch):
        mock_rows = [{"iso": "CAISO", "value": 55.0}]
        mock_dao.query_curves.return_value = QueryResult(data=mock_rows, elapsed_ms=12.5)
        manager = AsyncMock(spec=UnifiedCacheManager)
        manager.get = AsyncMock(return_value=None)
        manager.set = AsyncMock()
        manager.config = SimpleNamespace(ttl_seconds=45)
        monkeypatch.setattr("aurum.api.services.curves_service.get_unified_cache_manager", lambda: manager)

        result = await curves_service.query_data(
            filters={"iso": "CAISO"},
            pagination=Pagination(limit=10),
            context=ServiceCallContext(cache_directive=CacheDirective(namespace="curves", ttl_seconds=30)),
        )

        mock_dao.query_curves.assert_awaited_once()
        manager.set.assert_awaited()
        assert result.data == mock_rows
        assert result.metadata.cache_status == CacheStatus.MISS
        assert result.metadata.row_count == len(mock_rows)

    @pytest.mark.asyncio
    async def test_query_diff_uses_cache(self, curves_service, mock_dao, monkeypatch):
        diff_rows = [{"iso": "CAISO", "diff": 2.0}]
        mock_dao.query_curves_diff.return_value = QueryResult(data=diff_rows, elapsed_ms=18.0)
        manager = AsyncMock(spec=UnifiedCacheManager)
        manager.get = AsyncMock(return_value=None)
        manager.set = AsyncMock()
        manager.config = SimpleNamespace(ttl_seconds=50)
        monkeypatch.setattr("aurum.api.services.curves_service.get_unified_cache_manager", lambda: manager)

        diff_query = CurvesDiffQuery(asof_a=date(2024, 1, 1), asof_b=date(2024, 1, 2), iso="CAISO")
        result = await curves_service.query_diff(diff=diff_query)

        mock_dao.query_curves_diff.assert_awaited_once()
        manager.set.assert_awaited()
        assert result.data == diff_rows
        assert result.metadata.cache_status == CacheStatus.MISS

    @pytest.mark.asyncio
    async def test_export_data_streams_chunks(self, curves_service, mock_dao):
        mock_chunks = [[{"iso": "CAISO"}], []]
        mock_dao.query_curves.side_effect = [QueryResult(data=chunk, elapsed_ms=1.0) for chunk in mock_chunks]

        results = []
        async for row in curves_service.export_data(chunk_size=1):
            results.append(row)

        assert results == mock_chunks[0]
        assert mock_dao.query_curves.await_count == 2

    def test_legacy_query_curves_bridge(self, curves_service, mock_dao, monkeypatch):
        mock_rows = [{"iso": "CAISO"}]
        mock_dao.query_curves.return_value = QueryResult(data=mock_rows, elapsed_ms=5.0)
        manager = AsyncMock(spec=UnifiedCacheManager)
        manager.get = AsyncMock(return_value=None)
        manager.set = AsyncMock()
        manager.config = SimpleNamespace(ttl_seconds=120)
        monkeypatch.setattr("aurum.api.services.curves_service.get_unified_cache_manager", lambda: manager)

        rows, elapsed = curves_service.query_curves(
            asof=None,
            curve_key=None,
            asset_class=None,
            iso="CAISO",
            location=None,
            market=None,
            product=None,
            block=None,
            tenor_type=None,
            limit=10,
        )

        assert rows == mock_rows
        assert elapsed == 5.0
        mock_dao.query_curves.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalidate_cache(self, curves_service, monkeypatch):
        manager = AsyncMock(spec=UnifiedCacheManager)
        manager.invalidate_pattern = AsyncMock(return_value=0)
        monkeypatch.setattr("aurum.api.services.curves_service.get_unified_cache_manager", lambda: manager)

        result = await curves_service.invalidate_cache()

        manager.invalidate_pattern.assert_awaited_once_with("curves:*")
        assert result == {"curves": 0}