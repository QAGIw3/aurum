from __future__ import annotations

import base64
import json
from typing import Any

import pytest
from fastapi.testclient import TestClient

from aurum.api.app import create_app
from aurum.core import AurumSettings


@pytest.fixture
def api_client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
    settings = AurumSettings.from_env()
    app = create_app(settings)
    return TestClient(app)


def _decode_cursor(cursor: str | None) -> dict[str, Any] | None:
    if not cursor:
        return None
    decoded = base64.b64decode(cursor).decode()
    return json.loads(decoded)


class TestCatalogSeries:
    def test_etag_and_pagination(self, api_client: TestClient) -> None:
        response = api_client.get("/v2/catalog/series", params={"tenant_id": "demo"})
        assert response.status_code in {200, 204, 403}
        if response.status_code != 200:
            pytest.skip("catalog not populated in test environment")
        payload = response.json()
        assert "ETag" in response.headers
        next_cursor = payload["meta"].get("next_cursor")
        if next_cursor:
            decoded = _decode_cursor(next_cursor)
            assert isinstance(decoded, dict)
            follow = api_client.get(
                "/v2/catalog/series",
                params={"tenant_id": "demo", "cursor": next_cursor},
            )
            assert follow.status_code in {200, 403}

    def test_rate_limit_headers(self, api_client: TestClient) -> None:
        headers = {"X-Aurum-Tenant": "demo"}
        limited = None
        for _ in range(10):
            response = api_client.get(
                "/v2/catalog/series",
                params={"tenant_id": "demo"},
                headers=headers,
            )
            if response.status_code == 429:
                limited = response
                break
        if limited is None:
            pytest.skip("rate limit not enforced in local test env")
        # Headers should surface retry semantics from unified limiter
        assert limited.headers.get("Retry-After")
        assert limited.headers.get("X-RateLimit-Limit")
        # Remaining may be 0 or negative depending on implementation
        assert int(limited.headers.get("X-RateLimit-Remaining", "0")) <= 0

    def test_etag_reduces_transfer_size(self, api_client: TestClient) -> None:
        response = api_client.get("/v2/catalog/series", params={"tenant_id": "demo"})
        assert response.status_code in {200, 204, 403}
        if response.status_code != 200:
            pytest.skip("catalog not populated in test environment")
        etag = response.headers.get("ETag")
        assert etag
        fresh_bytes = len(response.content)
        cached = api_client.get(
            "/v2/catalog/series",
            params={"tenant_id": "demo"},
            headers={"If-None-Match": etag},
        )
        assert cached.status_code in {200, 304}
        if cached.status_code != 304:
            pytest.skip("backend did not honour conditional request")
        assert cached.headers.get("ETag") == etag
        assert fresh_bytes > 0
        cached_bytes = len(cached.content)
        savings_ratio = (fresh_bytes - cached_bytes) / fresh_bytes if fresh_bytes else 0
        assert savings_ratio >= 0.8, f"Expected >=80% savings, observed {savings_ratio:.2%}"


class TestSearchEndpoint:
    def test_etag_and_cursor(self, api_client: TestClient) -> None:
        params = {"tenant_id": "demo", "q": "power"}
        response = api_client.get("/v2/search", params=params)
        assert response.status_code in {200, 204, 400, 403}
        if response.status_code != 200:
            pytest.skip("search unavailable or missing tenant fixture")
        etag = response.headers.get("ETag")
        assert etag
        payload = response.json()
        next_cursor = payload["meta"].get("next_cursor")
        if next_cursor:
            secondary = api_client.get("/v2/search", params={**params, "cursor": next_cursor}, headers={"If-None-Match": etag})
            assert secondary.status_code in {200, 304, 400, 403}

    def test_rate_limit_returns_429(self, api_client: TestClient) -> None:
        params = {"tenant_id": "demo", "q": "power"}
        responses = [api_client.get("/v2/search", params=params) for _ in range(8)]
        limited = next((r for r in responses if r.status_code == 429), None)
        if limited is None:
            pytest.skip("rate limit thresholds too high for test")
        assert limited.headers["Retry-After"]
        assert limited.headers["X-RateLimit-Limit"]
        assert limited.headers["X-RateLimit-Remaining"] == "0"
