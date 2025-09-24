import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


@pytest.mark.unit
def test_invalid_cursor_metadata_dimensions():
    from aurum.api.v2 import metadata as meta_router

    app = FastAPI()
    app.include_router(meta_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/metadata/dimensions",
        params={"tenant_id": "t1", "cursor": "not-a-valid-cursor"},
    )
    assert r.status_code == 400
    assert "Invalid cursor" in str(r.json()).lower()


@pytest.mark.unit
def test_invalid_cursor_eia_series():
    from aurum.api.v2 import eia as eia_router

    app = FastAPI()
    app.include_router(eia_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/ref/eia/series",
        params={"tenant_id": "t1", "series_id": "S", "cursor": "bad"},
    )
    assert r.status_code == 400


@pytest.mark.unit
def test_invalid_cursor_iso_last24h():
    from aurum.api.v2 import iso as iso_router

    app = FastAPI()
    app.include_router(iso_router.router)
    client = TestClient(app)

    r = client.get(
        "/v2/iso/lmp/last-24h",
        params={"tenant_id": "t1", "iso": "PJM", "cursor": "bad"},
    )
    assert r.status_code == 400

