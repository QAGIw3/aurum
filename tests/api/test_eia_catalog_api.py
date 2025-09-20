from __future__ import annotations

import pytest


def test_list_eia_datasets_and_filter():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/eia/datasets")
    assert resp.status_code == 200
    items = resp.json()["data"]
    assert items
    assert any(it["path"] == "natural-gas/stor/wkly" for it in items)

    resp = client.get("/v1/metadata/eia/datasets", params={"prefix": "natural-gas/"})
    assert resp.status_code == 200
    items2 = resp.json()["data"]
    assert all(it["path"].startswith("natural-gas/") for it in items2)


def test_get_eia_dataset_details():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/eia/datasets/natural-gas/stor/wkly")
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["default_frequency"] == "weekly"
    assert any(f["id"] == "duoarea" for f in data["facets"])  # type: ignore[index]

