"""Contract tests for deterministic pagination patterns in v2 endpoints."""

from __future__ import annotations

import base64
import json
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import List

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC_PATH = REPO_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")

from aurum.api.v2 import scenarios as v2_scenarios
from aurum.api.scenario_models import CreateScenarioRequest


class _StubScenarioService:
    def __init__(self, items: List[ScenarioData]):
        self._items = items
        self.calls = []

    async def list_scenarios(
        self,
        *,
        tenant_id: str,
        limit: int,
        offset: int,
        name_contains: str | None = None,
        **_: object,
    ):
        self.calls.append({
            "tenant_id": tenant_id,
            "limit": limit,
            "offset": offset,
            "name_contains": name_contains,
        })

        end = min(offset + limit, len(self._items))
        sliced = self._items[offset:end]
        total = len(self._items)
        return sliced, total, {"stub": True}


@pytest.fixture
def scenario_client(monkeypatch):
    items = [
        ScenarioData(
            id=f"scenario-{idx}",
            tenant_id="tenant-1",
            name=f"Scenario {idx}",
            description=None,
            status=ScenarioStatus.ACTIVE,
            unique_key=f"scenario-{idx}",
            created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            updated_at=None,
        )
        for idx in range(5)
    ]

    stub_service = _StubScenarioService(items)
    monkeypatch.setattr(v2_scenarios, "get_service", lambda *_: stub_service)

    app = FastAPI()
    app.include_router(v2_scenarios.router)
    client = TestClient(app)
    return client, stub_service


def _decode_cursor(cursor: str) -> dict:
    payload = base64.urlsafe_b64decode(cursor.encode()).decode()
    return json.loads(payload)


def test_v2_scenarios_cursor_pagination_roundtrip(scenario_client):
    client, stub_service = scenario_client

    response = client.get("/v2/scenarios", params={"tenant_id": "tenant-1", "limit": 2})
    assert response.status_code == 200

    body = response.json()
    assert body["meta"]["limit"] == 2
    assert body["meta"]["offset"] == 0
    assert body["meta"]["returned_count"] == 2
    assert body["meta"]["has_more"] is True
    assert "next_cursor" in body["meta"]

    next_cursor = body["meta"]["next_cursor"]
    decoded = _decode_cursor(next_cursor)
    assert decoded["offset"] == 2
    assert decoded["limit"] == 2
    assert decoded["filters"]["tenant_id"] == "tenant-1"

    assert body["links"]["self"].endswith("/v2/scenarios?tenant_id=tenant-1&limit=2")
    assert body["links"]["next"].endswith(f"cursor={next_cursor}")

    # Follow the cursor
    response_cursor = client.get(
        "/v2/scenarios",
        params={"tenant_id": "tenant-1", "cursor": next_cursor, "limit": 2},
    )
    assert response_cursor.status_code == 200
    cursor_body = response_cursor.json()
    assert cursor_body["meta"]["offset"] == 2
    assert cursor_body["meta"]["returned_count"] == 2

    # Ensure the stub recorded deterministic offsets
    assert stub_service.calls[0]["offset"] == 0
    assert stub_service.calls[1]["offset"] == 2


def test_v2_scenarios_cursor_filter_mismatch_rejected(scenario_client):
    client, _ = scenario_client

    response = client.get("/v2/scenarios", params={"tenant_id": "tenant-1", "limit": 1})
    cursor = response.json()["meta"]["next_cursor"]

    mismatch = client.get(
        "/v2/scenarios",
        params={"tenant_id": "tenant-2", "cursor": cursor, "limit": 1},
    )

    assert mismatch.status_code == 400
    assert mismatch.json()["detail"] == "Cursor filters do not match request filters"
