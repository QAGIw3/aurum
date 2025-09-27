import importlib

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_list_ppa_contracts_router(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")
    class FakeSvc:
        async def list_contracts(self, **kwargs):
            limit = kwargs.get("limit", 1)
            data = [
                {
                    "contract_id": "C1",
                    "name": "Test PPA",
                    "counterparty": "UtilityX",
                    "capacity_mw": 100.0,
                    "price_usd_mwh": 35.5,
                    "start_date": "2024-01-01",
                    "end_date": "2034-01-01",
                },
                {
                    "contract_id": "C2",
                    "name": "Next PPA",
                    "counterparty": "UtilityY",
                    "capacity_mw": 80.0,
                    "price_usd_mwh": 45.0,
                    "start_date": "2025-01-01",
                    "end_date": "2035-01-01",
                },
            ]
            return data[:limit]

    async def fake_factory():
        return FakeSvc()

    import aurum.api.ppa_v2_service as ppa_service_module

    monkeypatch.setattr(ppa_service_module, "get_ppa_service", fake_factory)
    ppa_router = importlib.reload(importlib.import_module("aurum.api.v2.ppa"))

    app = FastAPI()
    app.include_router(ppa_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/ppa/contracts",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["data"][0]["contract_id"] == "C1"
    assert "ETag" in r.headers


@pytest.mark.unit
def test_list_ppa_valuations_router(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")
    class FakeSvc:
        async def list_valuations(self, **kwargs):
            limit = kwargs.get("limit", 1)
            data = [
                {
                    "valuation_date": "2024-01-01",
                    "period_start": "2024-01-01",
                    "period_end": "2024-01-31",
                    "metric": "NPV",
                    "present_value": 123.45,
                    "cashflow": 100.0,
                    "irr": 0.1,
                    "currency": "USD",
                },
                {
                    "valuation_date": "2024-02-01",
                    "period_start": "2024-02-01",
                    "period_end": "2024-02-29",
                    "metric": "NPV",
                    "present_value": 110.0,
                    "cashflow": 90.0,
                    "irr": 0.12,
                    "currency": "USD",
                },
            ]
            return data[:limit]

    async def fake_factory():
        return FakeSvc()

    import aurum.api.ppa_v2_service as ppa_service_module

    monkeypatch.setattr(ppa_service_module, "get_ppa_service", fake_factory)
    ppa_router = importlib.reload(importlib.import_module("aurum.api.v2.ppa"))

    app = FastAPI()
    app.include_router(ppa_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/ppa/contracts/C1/valuations",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["data"][0]["present_value"] == 123.45
    assert payload["data"][0]["cashflow"] == 100.0
    assert "ETag" in r.headers
