import pytest


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

    return TestClient(api_app.app)


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
