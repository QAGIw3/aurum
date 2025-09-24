import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_list_ppa_contracts_router(monkeypatch):
    from aurum.api.v2 import ppa as ppa_router

    class FakeSvc:
        async def list_contracts(self, **kwargs):
            return [
                {
                    "contract_id": "C1",
                    "name": "Test PPA",
                    "counterparty": "UtilityX",
                    "capacity_mw": 100.0,
                    "price_usd_mwh": 35.5,
                    "start_date": "2024-01-01",
                    "end_date": "2034-01-01",
                }
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(ppa_router, "get_ppa_service", fake_factory)

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
    from aurum.api.v2 import ppa as ppa_router

    class FakeSvc:
        async def list_valuations(self, **kwargs):
            return [
                {
                    "valuation_date": "2024-01-01",
                    "present_value": 123.45,
                    "currency": "USD",
                }
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(ppa_router, "get_ppa_service", fake_factory)

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
    assert "ETag" in r.headers

