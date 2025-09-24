import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_drought_indices_router(monkeypatch):
    from aurum.api.v2 import drought as drought_router

    class FakeSvc:
        async def list_indices(self, **kwargs):
            return [
                {
                    "series_id": "S1",
                    "dataset": "spi",
                    "index": "SPI",
                    "timescale": "12M",
                    "valid_date": "2024-01-01",
                    "region_type": "county",
                    "region_id": "C1",
                }
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(drought_router, "get_drought_service", fake_factory)

    app = FastAPI()
    app.include_router(drought_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/drought/indices",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers


@pytest.mark.unit
def test_drought_usdm_router(monkeypatch):
    from aurum.api.v2 import drought as drought_router

    class FakeSvc:
        async def list_usdm(self, **kwargs):
            return [
                {
                    "region_type": "county",
                    "region_id": "C1",
                    "valid_date": "2024-01-01",
                    "d0_frac": 0.1,
                }
            ]

    async def fake_factory():
        return FakeSvc()

    monkeypatch.setattr(drought_router, "get_drought_service", fake_factory)

    app = FastAPI()
    app.include_router(drought_router.router)
    client = TestClient(app)

    res = client.get(
        "/v2/drought/usdm",
        params={"tenant_id": "t1", "limit": 1},
    )
    assert res.status_code == 200
    assert "ETag" in res.headers

