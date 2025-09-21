from datetime import date
from typing import Any, Dict, List

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import aurum.api.routes as routes
from aurum.core.settings import AurumSettings


@pytest.fixture
def curves_client(monkeypatch) -> tuple[TestClient, List[Dict[str, Any]]]:
    settings = AurumSettings()
    routes.configure_routes(settings)

    app = FastAPI()
    app.state.settings = settings
    app.include_router(routes.router)

    calls: List[Dict[str, Any]] = []

    rows = [
        {
            "curve_key": "CK1",
            "tenor_label": "Jan",
            "tenor_type": "MONTHLY",
            "contract_month": date(2024, 1, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 10.0,
            "bid": 9.5,
            "ask": 10.5,
            "price_type": "MID",
        },
        {
            "curve_key": "CK2",
            "tenor_label": "Feb",
            "tenor_type": "MONTHLY",
            "contract_month": date(2024, 2, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 11.0,
            "bid": 10.5,
            "ask": 11.5,
            "price_type": "MID",
        },
        {
            "curve_key": "CK3",
            "tenor_label": "Mar",
            "tenor_type": "MONTHLY",
            "contract_month": date(2024, 3, 1),
            "asof_date": date(2024, 1, 1),
            "mid": 12.0,
            "bid": 11.5,
            "ask": 12.5,
            "price_type": "MID",
        },
    ]

    def fake_query_curves(*_, **kwargs):
        calls.append(
            {
                "cursor_after": kwargs.get("cursor_after"),
                "cursor_before": kwargs.get("cursor_before"),
                "descending": kwargs.get("descending"),
            }
        )
        return [dict(row) for row in rows], 7.1

    monkeypatch.setattr(routes.service, "query_curves", fake_query_curves)

    client = TestClient(app)
    return client, calls


def test_curves_etag_and_cache_headers(curves_client):
    client, _ = curves_client

    response = client.get("/v1/curves?limit=2")
    assert response.status_code == 200
    assert response.headers["Cache-Control"] == "public, max-age=120"

    etag = response.headers["ETag"]
    second = client.get(
        "/v1/curves?limit=2",
        headers={"If-None-Match": etag},
    )
    assert second.status_code == 304
    assert second.headers["ETag"] == etag


def test_curves_cursor_parameters_passthrough(curves_client):
    client, calls = curves_client

    first = client.get("/v1/curves?limit=2")
    assert first.status_code == 200
    payload = first.json()
    next_cursor = payload["meta"]["next_cursor"]
    assert next_cursor

    second = client.get(f"/v1/curves?limit=2&cursor={next_cursor}")
    assert second.status_code == 200

    assert len(calls) >= 2
    decoded = routes._decode_cursor(next_cursor)
    assert calls[1]["cursor_after"] == decoded
    assert calls[1]["cursor_before"] is None
