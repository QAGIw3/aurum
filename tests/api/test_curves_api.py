import types

import pytest


def test_list_curves_basic(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    # stub service.query_curves to avoid Trino dependency
    def fake_query_curves(*args, **kwargs):
        assert kwargs.get("cursor_after") is None
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
        assert kwargs.get("cursor_after") is None
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


def test_cursor_pagination_diff(monkeypatch):
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
            "asof_a": "2025-09-01",
            "mid_a": 10.0,
            "asof_b": "2025-09-02",
            "mid_b": 11.0,
            "diff_abs": 1.0,
            "diff_pct": 0.1,
        },
        {
            "curve_key": "b",
            "tenor_label": "2025-02",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-02-01",
            "asof_a": "2025-09-01",
            "mid_a": 12.0,
            "asof_b": "2025-09-02",
            "mid_b": 13.0,
            "diff_abs": 1.0,
            "diff_pct": 1.0/12.0,
        },
    ]

    captured_cursors: list = []

    def fake_query_curves_diff(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        captured_cursors.append(cursor_after)
        req_limit = kwargs.get("limit", 1)
        start = 0
        if cursor_after:
            for idx, item in enumerate(rows):
                if {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                } == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 5.0

    monkeypatch.setattr(service, "query_curves_diff", fake_query_curves_diff)
    client = TestClient(api_app.app)

    r1 = client.get(
        "/v1/curves/diff",
        params={"asof_a": "2025-09-01", "asof_b": "2025-09-02", "limit": 1},
    )
    assert r1.status_code == 200
    body1 = r1.json()
    assert body1["meta"]["next_cursor"]
    assert captured_cursors[0] is None

    cursor = body1["meta"]["next_cursor"]
    r2 = client.get(
        "/v1/curves/diff",
        params={
            "asof_a": "2025-09-01",
            "asof_b": "2025-09-02",
            "limit": 1,
            "cursor": cursor,
        },
    )
    assert r2.status_code == 200
    assert captured_cursors[-1] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
    }


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

    captured_cursors = []

    def fake_query_curves(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        captured_cursors.append(cursor_after)
        req_limit = kwargs.get("limit", 1)
        start = 0
        if cursor_after:
            for idx, item in enumerate(rows):
                current_payload = {
                    "curve_key": item["curve_key"],
                    "tenor_label": item["tenor_label"],
                    "contract_month": item.get("contract_month"),
                    "asof_date": item.get("asof_date"),
                    "price_type": item.get("price_type"),
                }
                if current_payload == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 3.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    # First page limit=1, should include next_cursor
    r1 = client.get("/v1/curves", params={"limit": 1})
    assert r1.status_code == 200
    p1 = r1.json()
    assert len(p1["data"]) == 1
    assert p1["meta"].get("next_cursor")
    assert captured_cursors[0] is None

    # Second page using cursor
    cursor = p1["meta"]["next_cursor"]
    r2 = client.get("/v1/curves", params={"limit": 1, "cursor": cursor})
    assert r2.status_code == 200
    p2 = r2.json()
    assert len(p2["data"]) == 1
    assert captured_cursors[-1] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
        "asof_date": "2025-09-12",
        "price_type": "MID",
    }


def test_cursor_pagination_alias(monkeypatch):
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

    captured = []

    def fake_query_curves(*args, **kwargs):
        captured.append(kwargs.get("cursor_after"))
        req_limit = kwargs.get("limit", 1)
        start = 0
        cursor_after = kwargs.get("cursor_after")
        if cursor_after:
            for idx, row in enumerate(rows):
                snapshot = {
                    "curve_key": row["curve_key"],
                    "tenor_label": row["tenor_label"],
                    "contract_month": row.get("contract_month"),
                    "asof_date": row.get("asof_date"),
                    "price_type": row.get("price_type"),
                }
                if snapshot == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 1.0

    monkeypatch.setattr(service, "query_curves", fake_query_curves)
    client = TestClient(api_app.app)

    first = client.get("/v1/curves", params={"limit": 1})
    cursor = first.json()["meta"]["next_cursor"]
    second = client.get("/v1/curves", params={"limit": 1, "since_cursor": cursor})
    assert second.status_code == 200
    assert captured[-1] == {
        "curve_key": "a",
        "tenor_label": "2025-01",
        "contract_month": "2025-01-01",
        "asof_date": "2025-09-12",
        "price_type": "MID",
    }


def test_dimensions_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*args, **kwargs):
        assert kwargs.get("include_counts") is False
        return {
            "asset_class": ["power"],
            "iso": ["PJM"],
            "market": ["DA"],
            "product": ["ATC"],
            "block": ["ON_PEAK"],
            "tenor_type": ["MONTHLY"],
            "location": ["West"],
        }, None

    monkeypatch.setattr(service, "query_dimensions", fake_query_dimensions)
    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/dimensions")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["data"]["iso"] == ["PJM"]
    assert payload.get("counts") is None


def test_dimensions_endpoint_with_counts(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app
    from aurum.api import service

    def fake_query_dimensions(*args, **kwargs):
        assert kwargs.get("include_counts") is True
        return (
            {
                "asset_class": ["power"],
                "iso": ["PJM"],
                "location": ["West"],
                "market": ["DA"],
                "product": ["ATC"],
                "block": ["ON_PEAK"],
                "tenor_type": ["MONTHLY"],
            },
            {
                "iso": [{"value": "PJM", "count": 10}],
                "market": [{"value": "DA", "count": 8}],
            },
        )

    monkeypatch.setattr(service, "query_dimensions", fake_query_dimensions)
    client = TestClient(api_app.app)
    resp = client.get("/v1/metadata/dimensions", params={"include_counts": "true"})
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["counts"]["iso"][0]["value"] == "PJM"
    assert payload["counts"]["market"][0]["count"] == 8


def test_health_endpoint():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    client = TestClient(api_app.app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ready_endpoint(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setattr(api_app, "_check_trino_ready", lambda cfg: True)
    client = TestClient(api_app.app)
    resp = client.get("/ready")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ready"


def test_ready_endpoint_unavailable(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setattr(api_app, "_check_trino_ready", lambda cfg: False)
    client = TestClient(api_app.app)
    resp = client.get("/ready")
    assert resp.status_code == 503


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
