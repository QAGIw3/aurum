from __future__ import annotations

from typing import Any, Dict, Sequence, Tuple

import pytest

from aurum.data import QueryResult


class _StubBackend:
    def __init__(self, rows: Sequence[Tuple[Any, ...]], metadata: Dict[str, Any]):
        self._rows = rows
        self._metadata = metadata
        self.last_query: str | None = None

    async def execute_query(self, query: str, params: Dict[str, Any] | None = None) -> QueryResult:
        self.last_query = query
        columns = ["tenant_id", "id", "name", "data_points", "created_at"]
        return QueryResult(columns=columns, rows=list(self._rows), metadata=self._metadata)


@pytest.fixture(autouse=True)
def _patch_backend(monkeypatch):
    rows = [("tenant-a", "curve-1", "curve-1", 3, "2024-01-02T00:00:00")]
    metadata = {
        "backend": "trino",
        "query_id": "trino-test-id",
        "pool_metrics": {"connections_created": 1},
        "pool_size": 1,
    }
    backend = _StubBackend(rows, metadata)
    monkeypatch.setattr("aurum.api.curves_v2_dao.get_data_backend", lambda _settings: backend)
    return backend


def test_curves_v2_endpoint_returns_paginated_data(api_client, _patch_backend):
    response = api_client.get(
        "/v2/curves",
        params={"tenant_id": "tenant-a", "limit": 1, "debug": "true"},
    )
    assert response.status_code == 200
    payload = response.json()

    assert payload["data"][0]["id"] == "curve-1"
    assert payload["meta"]["tenant_id"] == "tenant-a"
    assert payload["meta"]["returned_count"] == 1
    assert payload["meta"]["next_cursor"] is not None
    assert payload["meta"]["debug"]["backend"] == "trino"
    assert payload["meta"]["debug"]["query_id"] == "trino-test-id"
    assert "pool_metrics" in payload["meta"]["debug"]
    assert "tenant_id = 'tenant-a'" in _patch_backend.last_query


def test_curves_v2_endpoint_omits_debug_when_not_requested(api_client, _patch_backend):
    response = api_client.get(
        "/v2/curves",
        params={"tenant_id": "tenant-a", "limit": 1},
    )
    assert response.status_code == 200
    payload = response.json()

    assert "debug" not in payload["meta"]
