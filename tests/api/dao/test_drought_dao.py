from __future__ import annotations

import asyncio
from datetime import date, datetime

import pytest

from aurum.core import AurumSettings


class _StubClient:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    async def execute_query(self, sql):  # type: ignore[override]
        self.queries.append(sql)
        await asyncio.sleep(0)
        return self.rows


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_indices_parses_metadata(monkeypatch):
    from aurum.api.dao import drought_dao

    rows = [
        {
            "series_id": "S1",
            "dataset": "spi",
            "index": "SPI",
            "timescale": "1M",
            "valid_date": date(2024, 1, 1),
            "metadata": "{\"quality\": \"good\"}",
        }
    ]

    stub = _StubClient(rows)
    monkeypatch.setattr(drought_dao, "get_trino_client", lambda *_a, **_k: stub)

    dao = drought_dao.DroughtDao(AurumSettings())
    result, elapsed = await dao.query_indices(dataset="spi")

    assert result[0]["metadata"] == {"quality": "good"}
    assert elapsed >= 0.0
    assert "drought_index" in stub.queries[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_usdm_parses_metadata(monkeypatch):
    from aurum.api.dao import drought_dao

    rows = [
        {
            "region_type": "county",
            "region_id": "C1",
            "valid_date": date(2024, 1, 1),
            "metadata": "{\"source\": \"noaa\"}",
        }
    ]

    stub = _StubClient(rows)
    monkeypatch.setattr(drought_dao, "get_trino_client", lambda *_a, **_k: stub)

    dao = drought_dao.DroughtDao(AurumSettings())
    result, elapsed = await dao.query_usdm(region_type="county")

    assert result[0]["metadata"] == {"source": "noaa"}
    assert elapsed >= 0.0
    assert "usdm_area" in stub.queries[0]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_query_vector_events_parses_properties(monkeypatch):
    from aurum.api.dao import drought_dao

    rows = [
        {
            "layer": "aqi",
            "event_id": "evt-1",
            "properties": "{\"severity\": \"moderate\"}",
        }
    ]

    stub = _StubClient(rows)
    monkeypatch.setattr(drought_dao, "get_trino_client", lambda *_a, **_k: stub)

    dao = drought_dao.DroughtDao(AurumSettings())
    result, elapsed = await dao.query_vector_events(layer="aqi", start_time=datetime(2024, 1, 1), end_time=datetime(2024, 1, 2))

    assert result[0]["properties"] == {"severity": "moderate"}
    assert elapsed >= 0.0
    assert "vector_events" in stub.queries[0]
