from __future__ import annotations

import pytest


def test_list_locations_filters_and_prefix():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/locations", params={"iso": "PJM"})
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert any(item["location_id"] == "AECO" for item in data)

    # prefix filter
    resp = client.get("/v1/metadata/locations", params={"iso": "PJM", "prefix": "AE"})
    assert resp.status_code == 200
    data2 = resp.json()["data"]
    assert all(
        item["location_id"].lower().startswith("ae") or (item["location_name"] or "").lower().startswith("ae")
        for item in data2
    )


def test_get_location_by_id():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/locations/PJM/AECO")
    assert resp.status_code == 200
    item = resp.json()["data"]
    assert item["iso"] == "PJM"
    assert item["location_id"] == "AECO"
    assert item["location_type"] == "ZONE"


def test_units_metadata_endpoints():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/units")
    assert resp.status_code == 200
    data = resp.json()["data"]
    assert "currencies" in data and isinstance(data["currencies"], list)
    assert "units" in data and isinstance(data["units"], list)
    assert resp.headers.get("cache-control") == "public, max-age=60"
    # Mapping endpoint
    resp = client.get("/v1/metadata/units/mapping", params={"prefix": "USD"})
    assert resp.status_code == 200
    mappings = resp.json()["data"]
    assert isinstance(mappings, list)
    assert all("units_raw" in m for m in mappings)


def test_calendars_metadata_endpoints():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    # list calendars
    resp = client.get("/v1/metadata/calendars")
    assert resp.status_code == 200
    cals = resp.json()["data"]
    assert any(c["name"] for c in cals)
    name = cals[0]["name"]
    # list blocks
    resp = client.get(f"/v1/metadata/calendars/{name}/blocks")
    assert resp.status_code == 200
    blocks = resp.json()["data"]
    if blocks:
        block = blocks[0]
        # hours for a date
        resp = client.get(f"/v1/metadata/calendars/{name}/hours", params={"block": block, "date": "2024-01-02"})
        assert resp.status_code == 200
        # expand range
        resp = client.get(
            f"/v1/metadata/calendars/{name}/expand",
            params={"block": block, "start": "2024-01-01", "end": "2024-01-02"},
        )
        assert resp.status_code == 200
