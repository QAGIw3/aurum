from __future__ import annotations

import pytest

from aurum.libs.services.market_service import MarketService


class FakeCache:
    def __init__(self):
        self.store = {}

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value, *, ttl_seconds=None):
        self.store[key] = value
        return True

    async def invalidate(self, key_or_pattern: str) -> int:
        self.store.pop(key_or_pattern, None)
        return 1


class FakeTrino:
    def __init__(self, rows):
        self._rows = rows

    async def execute_query(self, query: str):
        return self._rows


@pytest.mark.asyncio
async def test_curves_latest_service_basic():
    rows = [
        {
            "tenant_id": "t1",
            "curve_key": "K",
            "tenor_label": "M1",
            "tenor_type": "MONTH",
            "contract_month": "2024-01-01",
            "asof_date": "2024-01-02",
            "currency": "USD",
            "per_unit": "MMBtu",
            "mid": 10.0,
            "bid": 9.9,
            "ask": 10.1,
            "version_hash": "abc",
        }
    ]
    svc = MarketService(trino=FakeTrino(rows), cache=FakeCache())
    data, dbg = await svc.curves_latest(tenant_id="t1", offset=0, limit=10)
    assert len(data) == 1
    assert data[0]["curve_key"] == "K"
    assert dbg == {}


@pytest.mark.asyncio
async def test_curves_latest_cache_hit():
    rows = [
        {
            "tenant_id": "t1",
            "curve_key": "K",
            "tenor_label": "M1",
            "tenor_type": "MONTH",
            "contract_month": "2024-01-01",
            "asof_date": "2024-01-02",
            "currency": "USD",
            "per_unit": "MMBtu",
            "mid": 10.0,
            "bid": 9.9,
            "ask": 10.1,
            "version_hash": "abc",
        }
    ]

    class CountingTrino(FakeTrino):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        async def execute_query(self, query: str):
            self.calls += 1
            return await super().execute_query(query)

    cache = FakeCache()
    svc = MarketService(trino=CountingTrino(rows), cache=cache)
    # First call populates cache
    data1, _ = await svc.curves_latest(tenant_id="t1", offset=0, limit=10)
    # Second call should hit cache and not increment trino calls
    data2, _ = await svc.curves_latest(tenant_id="t1", offset=0, limit=10)

    assert data1 == data2
    assert svc._trino.calls == 1


@pytest.mark.asyncio
async def test_curves_asof_cache_hit():
    rows = [
        {"tenant_id": "t1", "curve_key": "K", "contract_month": "2024-01", "asof_date": "2024-01-02", "mid": 10.0}
    ]

    class CountingTrino(FakeTrino):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        async def execute_query(self, query: str):
            self.calls += 1
            return await super().execute_query(query)

    cache = FakeCache()
    svc = MarketService(trino=CountingTrino(rows), cache=cache)
    await svc.curves_asof(tenant_id="t1", asof_date="2024-01-02", offset=0, limit=10)
    await svc.curves_asof(tenant_id="t1", asof_date="2024-01-02", offset=0, limit=10)
    assert svc._trino.calls == 1


@pytest.mark.asyncio
async def test_curves_asof_diff_cache_hit():
    rows = [
        {"tenant_id": "t1", "curve_key": "K", "contract_month": "2024-01", "asof_date_new": "2024-01-02", "mid_new": 10.0,
         "asof_date_old": "2023-12-30", "mid_old": 9.5, "mid_diff": 0.5}
    ]

    class CountingTrino(FakeTrino):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        async def execute_query(self, query: str):
            self.calls += 1
            return await super().execute_query(query)

    cache = FakeCache()
    svc = MarketService(trino=CountingTrino(rows), cache=cache)
    await svc.curves_asof_diff(tenant_id="t1", offset=0, limit=10)
    await svc.curves_asof_diff(tenant_id="t1", offset=0, limit=10)
    assert svc._trino.calls == 1


@pytest.mark.asyncio
async def test_scenario_output_view_cache_hit():
    rows = [
        {"tenant_id": "t1", "scenario_id": "s1", "curve_key": "K", "metric": "mid", "value": 1.23}
    ]

    class CountingTrino(FakeTrino):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        async def execute_query(self, query: str):
            self.calls += 1
            return await super().execute_query(query)

    cache = FakeCache()
    svc = MarketService(trino=CountingTrino(rows), cache=cache)
    await svc.scenario_output_view(tenant_id="t1", scenario_id="s1", metric="mid", offset=0, limit=10)
    await svc.scenario_output_view(tenant_id="t1", scenario_id="s1", metric="mid", offset=0, limit=10)
    assert svc._trino.calls == 1


