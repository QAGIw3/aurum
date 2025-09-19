import types

import pytest


def test_list_curves_basic(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    # stub service.query_curves to avoid Trino dependency
    def fake_query_curves(*args, **kwargs):
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "2025-09",
                "tenor_type": "MONTHLY",
                "contract_month": "2025-09-01",
                "asof_date": "2025-09-12",
                "mid": 42.0,
                "bid": 41.9,
                "ask": 42.1,
                "price_type": "MID",
            }
        ]
        return rows, 5.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)

    client = TestClient(api_app.app)
    resp = client.get("/v1/curves", params={"limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    assert "meta" in payload and "data" in payload
    assert payload["data"] and payload["data"][0]["curve_key"] == "abc"
    assert payload["meta"].get("next_cursor") is None


def test_list_curves_diff(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_curves_diff(*args, **kwargs):
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "2025-09",
                "tenor_type": "MONTHLY",
                "contract_month": "2025-09-01",
                "asof_a": "2025-09-11",
                "mid_a": 41.0,
                "asof_b": "2025-09-12",
                "mid_b": 42.0,
                "diff_abs": 1.0,
                "diff_pct": 1.0/41.0,
            }
        ]
        return rows, 7.0

    monkeypatch.setattr(service, "query_curves_diff", fake_query_curves_diff)
    client = TestClient(api_app.app)
    resp = client.get("/v1/curves/diff", params={"asof_a": "2025-09-11", "asof_b": "2025-09-12", "limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"][0]["diff_abs"] == 1.0


def test_cursor_pagination(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    rows = [
        {
            "curve_key": "a",
            "tenor_label": "2025-01",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-01-01",
            "asof_date": "2025-09-12",
            "mid": 1.0,
            "bid": 1.0,
            "ask": 1.0,
            "price_type": "MID",
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_date": "2025-09-12",
            "mid": 2.0,
            "bid": 2.0,
            "ask": 2.0,
            "price_type": "MID",
        },
    ]

    def fake_query_curves(*args, **kwargs):
        # Return exactly the number of rows requested
        req_limit = kwargs.get("limit", 1)
        return rows[:req_limit], 3.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    # First page limit=1, should include next_cursor
    r1 = client.get("/v1/curves", params={"limit": 1})
    assert r1.status_code == 200
    p1 = r1.json()
    assert len(p1["data"]) == 1
    assert p1["meta"].get("next_cursor")

    # Second page using cursor
    cursor = p1["meta"]["next_cursor"]
    r2 = client.get("/v1/curves", params={"limit": 1, "cursor": cursor})
    assert r2.status_code == 200
    p2 = r2.json()
    assert len(p2["data"]) == 1


def test_dimensions_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*args, **kwargs):
        return {
            "asset_class": ["power"],
            "iso": ["PJM"],
            "market": ["DA"],
            "product": ["ATC"],
            "block": ["ON_PEAK"],
            "tenor_type": ["MONTHLY"],
            "location": ["West"],
        }

    monkeypatch.setattr(service, "query_dimensions", fake_query_dimensions)
    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/dimensions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"]["iso"] == ["PJM"]


def test_health_endpoint():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_strips_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_curves(*args, **kwargs):
        rows = [
            {
                "curve_key": "abc",
                "tenor_label": "Calendar 2025",
                "tenor_type": "CALENDAR",
                "contract_month": None,
                "asof_date": "2025-09-12",
                "mid": 50.0,
                "bid": 49.9,
                "ask": 50.1,
                "price_type": "MID",
            }
        ]
        return rows, 4.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)
    resp = client.get("/v1/curves/strips", params={"type": "CALENDAR", "limit": 1})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"][0]["tenor_type"] == "CALENDAR"


def test_scenarios_create_and_run():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)

    # Create scenario
    payload = {
        "tenant_id": "t1",
        "name": "My Scenario",
        "assumptions": [
            {"driver_type": "policy", "payload": {"policy_name": "RPS", "start_year": 2025}}
        ],
    }
    r = client.post("/v1/scenarios", json=payload)
    assert r.status_code == 201
    scenario_body = r.json()["data"]
    scenario_id = scenario_body["scenario_id"]
    assert scenario_id
    assert scenario_body["tenant_id"] == "t1"
    assert scenario_body["status"] == "CREATED"

    # Run scenario
    r2 = client.post(f"/v1/scenarios/{scenario_id}/run", json={"code_version": "abc"})
    assert r2.status_code == 202
    run_body = r2.json()["data"]
    assert run_body["state"] == "QUEUED"
    assert run_body["scenario_id"] == scenario_id


def test_ppa_valuate():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    payload = {"ppa_contract_id": "ppa1"}
    r = client.post("/v1/ppa/valuate", json=payload)
    assert r.status_code == 200
    assert isinstance(r.json()["data"], list)
