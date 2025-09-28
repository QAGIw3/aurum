import importlib
from decimal import Decimal

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_list_ppa_contracts_router(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")
    class FakeInnerService:
        async def list_contracts(self, **kwargs):
            assert kwargs["tenant_id"] == "t1"
            assert kwargs["counterparty_filter"] is None
            # Router performs a lookahead (+1) on the requested limit
            assert kwargs["limit"] == 2
            data = [
                {
                    "contract_id": " C1 ",
                    "name": "  ",
                    "counterparty": " UtilityX  ",
                    "capacity_mw": "100.0",
                    "price_usd_mwh": Decimal("35.5"),
                    "start_date": "2024-01-01T00:00:00Z",
                    "end_date": "not-a-date",
                },
                {
                    "id": "C2",
                    "name": None,
                    "counterparty": "",
                    "capacity_mw": None,
                    "price_usd_mwh": "NaN",
                    "start_date": "",
                    "end_date": "2025-01-01",
                },
            ]
            return data

    import aurum.api.ppa_v2_service as ppa_service_module

    service_instance = ppa_service_module.PpaV2Service()
    service_instance._service = FakeInnerService()

    async def fake_factory():
        return service_instance

    monkeypatch.setattr(ppa_service_module, "get_ppa_service", fake_factory)
    ppa_router = importlib.reload(importlib.import_module("aurum.api.v2.ppa"))

    app = FastAPI()
    app.include_router(ppa_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/ppa/contracts",
        params={"tenant_id": " t1 ", "limit": 1},
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["data"][0]["contract_id"] == "C1"
    assert payload["data"][0]["name"] == "C1"
    assert payload["data"][0]["counterparty"] == "UtilityX"
    assert payload["data"][0]["capacity_mw"] == pytest.approx(100.0)
    assert payload["data"][0]["start_date"] == "2024-01-01"
    assert payload["data"][0]["end_date"] is None
    assert payload["meta"]["tenant_id"] == "t1"
    self_link = payload["links"].get("self", "")
    assert "tenant_id=t1" in self_link
    assert "limit=1" in self_link
    assert "ETag" in r.headers


@pytest.mark.unit
def test_list_ppa_valuations_router(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")
    class FakeSvc:
        async def list_valuations(self, **kwargs):
            assert kwargs["tenant_id"] == "t1"
            assert kwargs["contract_id"] == "C1"
            assert kwargs["start_date"] == "2024-01-01"
            assert kwargs["end_date"] == "2024-01-31"
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
        params={
            "tenant_id": " t1 ",
            "limit": 1,
            "start_date": "2024-01-01T00:00:00Z",
            "end_date": " 2024-01-31 ",
        },
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["data"][0]["valuation_date"] == "2024-01-01"
    assert payload["data"][0]["present_value"] == 123.45
    assert payload["data"][0]["cashflow"] == 100.0
    assert payload["meta"]["tenant_id"] == "t1"
    assert payload["meta"]["start_date"] == "2024-01-01"
    assert payload["meta"]["end_date"] == "2024-01-31"
    self_link = payload["links"].get("self", "")
    assert "tenant_id=t1" in self_link
    assert "start_date=2024-01-01" in self_link
    assert "ETag" in r.headers


@pytest.mark.unit
def test_list_ppa_contracts_rejects_blank_tenant(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")

    call_tracker = {"called": False}

    class FakeSvc:
        async def list_contracts(self, **_kwargs):
            call_tracker["called"] = True
            return []

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
        params={"tenant_id": "   "},
    )

    assert r.status_code == 400
    payload = r.json()
    assert payload["detail"]["parameter"] == "tenant_id"
    assert call_tracker["called"] is False


@pytest.mark.unit
def test_list_ppa_valuations_rejects_reversed_dates(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")

    call_tracker = {"called": False}

    class FakeSvc:
        async def list_valuations(self, **_kwargs):
            call_tracker["called"] = True
            return []

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
        params={
            "tenant_id": "t1",
            "start_date": "2024-02-02",
            "end_date": "2024-02-01",
        },
    )

    assert r.status_code == 400
    payload = r.json()
    assert payload["detail"]["parameter"] == "date_range"
    assert call_tracker["called"] is False


@pytest.mark.unit
def test_list_ppa_valuations_rejects_invalid_start(monkeypatch):
    monkeypatch.setenv("AURUM_API_V2_LIGHT_INIT", "1")

    call_tracker = {"called": False}

    class FakeSvc:
        async def list_valuations(self, **_kwargs):
            call_tracker["called"] = True
            return []

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
        params={
            "tenant_id": "t1",
            "start_date": "not-a-date",
        },
    )

    assert r.status_code == 400
    payload = r.json()
    assert payload["detail"]["parameter"] == "start_date"
    assert call_tracker["called"] is False
