import pytest

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


def test_scenario_outputs_disabled(client):
    resp = client.get("/v1/scenarios/example/outputs")
    assert resp.status_code == 503


def test_scenario_outputs_enabled(monkeypatch, client):
    from aurum.api import service

    monkeypatch.setenv("AURUM_API_SCENARIO_OUTPUTS_ENABLED", "1")

    rows = [
        {
            "scenario_id": "scn-1",
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
        captured_cursors.append(cursor_after)
        req_limit = kwargs.get("limit", 1)
        start = 0
        if cursor_after:
            for idx, row in enumerate(rows):
                snapshot = {
                    "scenario_id": row["scenario_id"],
                    "curve_key": row["curve_key"],
                    "tenor_label": row.get("tenor_label"),
                    "contract_month": row.get("contract_month"),
                    "metric": row.get("metric"),
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
    assert captured_cursors[0] is None

    cursor = body["meta"]["next_cursor"]
    resp2 = client.get(
        "/v1/scenarios/scn-1/outputs",
        params={"limit": 1, "cursor": cursor},
    )
    assert resp2.status_code == 200
    assert captured_cursors[-1] == {
        "scenario_id": "scn-1",
        "curve_key": "curve-a",
        "tenor_label": "2025-06",
        "contract_month": "2025-06-01",
        "metric": "mid",
    }
