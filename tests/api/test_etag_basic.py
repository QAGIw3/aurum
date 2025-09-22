from __future__ import annotations

from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient

from aurum.api.http import respond_with_etag


def create_app() -> FastAPI:
    app = FastAPI()

    @app.get("/resource")
    async def get_resource(request: Request, response: Response):
        payload = {"meta": {"note": "meta is ignored in etag"}, "data": {"value": 42}}
        return respond_with_etag(payload, request, response)

    return app


def test_etag_and_if_none_match_flow():
    app = create_app()
    client = TestClient(app)

    r1 = client.get("/resource")
    assert r1.status_code == 200
    etag = r1.headers.get("ETag")
    assert etag

    r2 = client.get("/resource", headers={"If-None-Match": etag})
    assert r2.status_code == 304
    # No body for 304
    assert r2.content in (b"", None)

