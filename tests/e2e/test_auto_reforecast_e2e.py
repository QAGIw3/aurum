import os
import uuid
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app
from aurum.core import AurumSettings


pytestmark = pytest.mark.usefixtures("enable_test_default_settings")


def _make_settings() -> AurumSettings:
    settings = AurumSettings()
    # Ensure DB points to test DSN if provided
    dsn = os.getenv("AURUM_APP_DB_DSN", "postgresql://localhost/aurum")
    settings.database_url = dsn
    settings.api.rate_limit.enabled = False
    return settings


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    settings = _make_settings()
    app = create_app(settings)
    return TestClient(app)


def test_auto_reforecast_trigger_rerun_persists_job(client: TestClient):
    tenant_id = str(uuid.uuid4())

    # Create trigger
    trigger_body = {
        "name": "e2e-weather",
        "description": "e2e trigger",
        "conditions": [
            {
                "data_source": "weather",
                "geography": "US",
                "threshold_type": "percentage",
                "threshold_value": 0.05,
                "lookback_hours": 24,
                "min_change_required": True,
                "fields": ["temperature"],
                "metadata": {"fields": ["temperature"]}
            }
        ],
        "forecast_config": {
            "forecast_type": "load",
            "target_variable": "load_mw",
            "geography": "US",
            "start_date": datetime.utcnow().isoformat(),
            "end_date": datetime.utcnow().isoformat(),
            "quantiles": ["P50"],
            "interval": "hourly"
        },
        "priority": 1.0,
        "cooldown_minutes": 5,
        "enabled": True
    }

    resp = client.post(
        "/v2/auto-reforecast/triggers",
        json=trigger_body,
        headers={"X-Aurum-Tenant": tenant_id},
    )
    assert resp.status_code in (200, 201), resp.text
    trigger = resp.json()["data"] if "data" in resp.json() else resp.json()
    trigger_id = trigger["trigger_id"]

    # Trigger rerun
    resp2 = client.post(
        "/v2/auto-reforecast/trigger-forecast-rerun",
        params={
            "data_source": "weather",
            "geography": "US",
            "forecast_type": "load",
            "target_variable": "load_mw",
            "priority": 1.0,
            "trigger_reason": "e2e"
        },
        headers={"X-Aurum-Tenant": tenant_id},
    )
    assert resp2.status_code == 200, resp2.text
    data = resp2.json()["data"] if "data" in resp2.json() else resp2.json()
    job_id = data.get("job_id")

    # List jobs and assert presence
    resp3 = client.get(
        "/v2/auto-reforecast/jobs",
        headers={"X-Aurum-Tenant": tenant_id},
        params={"limit": 50}
    )
    assert resp3.status_code == 200, resp3.text
    jobs_env = resp3.json()
    jobs = jobs_env["data"] if "data" in jobs_env else jobs_env
    assert isinstance(jobs, list)
    assert any(j["job_id"] == job_id for j in jobs if job_id), "Expected job to be recorded"
