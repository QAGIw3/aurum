import pytest
from typing import Optional

pytest.importorskip("pydantic", reason="pydantic not installed")


@pytest.fixture(autouse=True)
def reset_store():
    from aurum.api.scenario_service import STORE

    STORE.reset()
    yield
    STORE.reset()


@pytest.fixture
def client():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    test_client = TestClient(api_app.app)
    test_client.headers.update({"X-Aurum-Tenant": "tenant-123"})
    return test_client


def _sample_payload():
    return {
        "tenant_id": "tenant-123",
        "name": "Test Scenario",
        "description": "Example",
        "assumptions": [
            {
                "driver_type": "policy",
                "payload": {"policy_name": "RPS", "start_year": 2030},
            }
        ],
    }


def test_create_and_get_scenario(client):
    resp = client.post("/v1/scenarios", json=_sample_payload())
    assert resp.status_code == 201
    data = resp.json()["data"]
    scenario_id = data["scenario_id"]
    assert data["status"] == "CREATED"
    assert data["assumptions"][0]["driver_type"] == "policy"

    resp_get = client.get(f"/v1/scenarios/{scenario_id}")
    assert resp_get.status_code == 200
    retrieved = resp_get.json()["data"]
    assert retrieved["scenario_id"] == scenario_id
    assert retrieved["name"] == "Test Scenario"

    list_resp = client.get("/v1/scenarios")
    assert list_resp.status_code == 200
    scenarios = list_resp.json()["data"]
    assert any(item["scenario_id"] == scenario_id for item in scenarios)


def test_list_scenarios_cursor_and_status(client):
    for idx in range(3):
        payload = _sample_payload()
        payload["name"] = f"Scenario {idx}"
        client.post("/v1/scenarios", json=payload)

    from aurum.api.scenario_service import STORE

    scenarios = list(STORE._scenarios.values())  # type: ignore[attr-defined]
    if len(scenarios) >= 2:
        scenarios[1].status = "ARCHIVED"

    first_page = client.get("/v1/scenarios", params={"limit": 2})
    assert first_page.status_code == 200
    meta = first_page.json()["meta"]
    assert meta["next_cursor"]

    second_page = client.get("/v1/scenarios", params={"cursor": meta["next_cursor"]})
    assert second_page.status_code == 200
    assert second_page.json()["data"]

    filtered = client.get("/v1/scenarios", params={"status": "ARCHIVED"})
    assert filtered.status_code == 200
    data = filtered.json()["data"]
    assert data and all(item["status"] == "ARCHIVED" for item in data)


def test_run_scenario(client):
    create = client.post("/v1/scenarios", json=_sample_payload())
    scenario_id = create.json()["data"]["scenario_id"]

    run_resp = client.post(
        f"/v1/scenarios/{scenario_id}/run",
        json={"code_version": "v1", "seed": 42},
    )
    assert run_resp.status_code == 202
    run_data = run_resp.json()["data"]
    run_id = run_data["run_id"]
    assert run_data["state"] == "QUEUED"

    get_run = client.get(f"/v1/scenarios/{scenario_id}/runs/{run_id}")
    assert get_run.status_code == 200
    fetched = get_run.json()["data"]
    assert fetched["run_id"] == run_id
    assert fetched["scenario_id"] == scenario_id

    list_runs = client.get(f"/v1/scenarios/{scenario_id}/runs")
    assert list_runs.status_code == 200
    runs = list_runs.json()["data"]
    assert any(item["run_id"] == run_id for item in runs)

    cancel_resp = client.post(f"/v1/scenarios/runs/{run_id}/cancel")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["data"]["state"] == "CANCELLED"


def test_list_runs_state_filter(client):
    scenario_id = client.post("/v1/scenarios", json=_sample_payload()).json()["data"]["scenario_id"]
    run_data = client.post(
        f"/v1/scenarios/{scenario_id}/run",
        json={"code_version": "v1", "seed": 99},
    ).json()["data"]
    run_id = run_data["run_id"]

    client.post(f"/v1/scenarios/runs/{run_id}/state", params={"state": "RUNNING"})
    client.post(f"/v1/scenarios/runs/{run_id}/state", params={"state": "SUCCEEDED"})

    filtered = client.get(f"/v1/scenarios/{scenario_id}/runs", params={"state": "SUCCEEDED"})
    assert filtered.status_code == 200
    data = filtered.json()["data"]
    assert data and data[0]["state"] == "SUCCEEDED"


def test_scenario_outputs_disabled(client):
    resp = client.get("/v1/scenarios/example/outputs")
    assert resp.status_code == 503


def test_scenario_outputs_enabled(monkeypatch, client):
    from aurum.api import service

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    rows = [
        {
            "scenario_id": "scn-1",
            "run_id": "run-001",
            "asof_date": "2025-01-01",
            "curve_key": "curve-a",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-06-01",
            "tenor_label": "2025-06",
            "metric": "mid",
            "value": 42.1,
            "band_lower": 40.0,
            "band_upper": 45.0,
            "attribution": {"policy": 3.0},
            "version_hash": "abc",
        },
        {
            "scenario_id": "scn-1",
            "run_id": "run-001",
            "asof_date": "2025-01-01",
            "curve_key": "curve-b",
            "tenor_type": "MONTHLY",
            "contract_month": "2025-07-01",
            "tenor_label": "2025-07",
            "metric": "mid",
            "value": 41.0,
            "band_lower": None,
            "band_upper": None,
            "attribution": None,
            "version_hash": "def",
        },
    ]

    captured_cursors = []

    def fake_query_scenario_outputs(*args, **kwargs):
        cursor_after = kwargs.get("cursor_after")
        cursor_before = kwargs.get("cursor_before")
        descending = kwargs.get("descending")
        captured_cursors.append((cursor_after, cursor_before, descending))
        assert kwargs.get("tenant_id") == "tenant-123"
        req_limit = kwargs.get("limit", 1)

        if cursor_before:
            idx = 0
            for i, row in enumerate(rows):
                snapshot = {
                    "scenario_id": row["scenario_id"],
                    "curve_key": row["curve_key"],
                    "tenor_label": row.get("tenor_label"),
                    "contract_month": row.get("contract_month"),
                    "metric": row.get("metric"),
                    "run_id": row.get("run_id"),
                }
                if snapshot == cursor_before:
                    idx = i
                    break
            start = max(0, idx - req_limit)
            subset = rows[start:idx]
            if descending:
                subset = list(reversed(subset))
            return subset, 10.0

        start = 0
        if cursor_after:
            for idx, row in enumerate(rows):
                snapshot = {
                    "scenario_id": row["scenario_id"],
                    "curve_key": row["curve_key"],
                    "tenor_label": row.get("tenor_label"),
                    "contract_month": row.get("contract_month"),
                    "metric": row.get("metric"),
                    "run_id": row.get("run_id"),
                }
                if snapshot == cursor_after:
                    start = idx + 1
                    break
        subset = rows[start : start + req_limit]
        return subset, 10.0

    monkeypatch.setattr(service, "query_scenario_outputs", fake_query_scenario_outputs)

    resp = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 1, "metric": "mid"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["meta"]["next_cursor"]
    data = body["data"]
    assert data[0]["curve_key"] == "curve-a"
    assert data[0]["run_id"] == "run-001"
    etag = resp.headers.get("etag")
    assert etag
    cached = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 1, "metric": "mid"},
        headers={"If-None-Match": etag},
    )
    assert cached.status_code == 304
    assert captured_cursors[0][0] is None

    cursor = body["meta"]["next_cursor"]
    resp2 = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 1, "since_cursor": cursor},
    )
    assert resp2.status_code == 200
    assert captured_cursors[-1][0] == {
        "scenario_id": "scn-1",
        "curve_key": "curve-a",
        "tenor_label": "2025-06",
        "contract_month": "2025-06-01",
        "metric": "mid",
        "run_id": "run-001",
    }
    prev_token = resp2.json()["meta"].get("prev_cursor")
    assert prev_token
    back_resp = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 1, "prev_cursor": prev_token},
    )
    assert back_resp.status_code == 200
    back_data = back_resp.json()["data"]
    assert back_data and back_data[0]["curve_key"] == "curve-a"


def test_delete_scenario(client):
    scenario = client.post("/v1/scenarios", json=_sample_payload()).json()["data"]
    scenario_id = scenario["scenario_id"]

    resp = client.delete(f"/v1/scenarios/{scenario_id}")
    assert resp.status_code == 204
    assert client.get(f"/v1/scenarios/{scenario_id}").status_code == 404


def test_delete_scenario_enforces_tenant():
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    primary = TestClient(api_app.app)
    primary.headers.update({"X-Aurum-Tenant": "tenant-primary"})
    payload = _sample_payload()
    payload["tenant_id"] = "tenant-primary"
    scenario = primary.post("/v1/scenarios", json=payload).json()["data"]

    secondary = TestClient(api_app.app)
    secondary.headers.update({"X-Aurum-Tenant": "tenant-secondary"})

    resp = secondary.delete(f"/v1/scenarios/{scenario['scenario_id']}")
    assert resp.status_code == 404
    assert secondary.get(f"/v1/scenarios/{scenario['scenario_id']}").status_code == 404


def test_scenario_metrics_latest(monkeypatch, client):
    from aurum.api import service

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    rows = [
        {
            "tenant_id": "tenant-123",
            "scenario_id": "scn-1",
            "curve_key": "curve-a",
            "metric": "mid",
            "tenor_label": "2025-07",
            "latest_value": 41.5,
            "latest_band_lower": 40.0,
            "latest_band_upper": 43.0,
            "latest_asof_date": "2025-01-01",
        }
    ]

    monkeypatch.setattr(
        service,
        "query_scenario_metrics_latest",
        lambda *args, **kwargs: (rows, 8.0),
    )

    resp = client.get("/v1/scenarios/scn-1/metrics/latest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["data"][0]["metric"] == "mid"


def test_scenario_outputs_requires_tenant(monkeypatch):
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    client_no_tenant = TestClient(api_app.app)
    resp = client_no_tenant.get("/v1/scenarios/scn-1/outputs")
    assert resp.status_code == 400
    assert resp.json()["detail"] == "tenant_id is required"


def test_scenario_outputs_allows_explicit_tenant(monkeypatch):
    from aurum.api import service

    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient
    from aurum.api import app as api_app

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    calls = {}

    def fake_query(*_args, **kwargs):
        calls.update(kwargs)
        return [], 0.0

    monkeypatch.setattr(service, "query_scenario_outputs", fake_query)

    client_local = TestClient(api_app.app)
    resp = client_local.get(
        "/v1/scenarios/scn-1/outputs",
        params={"tenant_id": "tenant-override"},
    )
    assert resp.status_code == 200
    assert calls.get("tenant_id") == "tenant-override"


def test_admin_cache_invalidate_requires_admin(monkeypatch, client):
    from aurum.api import app as api_app

    client.app.dependency_overrides.clear()
    client.app.dependency_overrides[api_app._get_principal] = lambda: {"groups": ["analyst"]}

    resp = client.post("/v1/admin/cache/scenario/scn-123/invalidate")
    assert resp.status_code == 403

    client.app.dependency_overrides.clear()


def test_admin_cache_invalidate_success(monkeypatch, client):
    from aurum.api import app as api_app, service

    captured: dict[str, tuple[Optional[str], str]] = {}

    def fake_invalidate(cache_cfg, tenant_id, scenario_id):
        captured["call"] = (tenant_id, scenario_id)

    client.app.dependency_overrides.clear()
    client.app.dependency_overrides[api_app._get_principal] = lambda: {
        "groups": ["aurum-admins"],
        "tenant": "tenant-admin",
    }
    monkeypatch.setattr(service, "invalidate_scenario_outputs_cache", fake_invalidate)

    resp = client.post("/v1/admin/cache/scenario/scn-321/invalidate")
    assert resp.status_code == 204
    assert captured.get("call") == ("tenant-admin", "scn-321")

    client.app.dependency_overrides.clear()
