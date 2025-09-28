from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

pytest.importorskip("strawberry")

from aurum.api.graphql.router import AurumGraphQLRouter, schema


def make_graphql_client() -> TestClient:
    app = FastAPI()
    graphql_router = AurumGraphQLRouter(
        schema,
        graphiql=False,
        subscriptions_enabled=False,
        introspection=False,
    )
    app.include_router(graphql_router, prefix="/graphql")
    return TestClient(app)


def test_graphql_requires_tenant_header() -> None:
    client = make_graphql_client()

    response = client.post("/graphql", json={"query": "{ __typename }"})

    assert response.status_code == 400
    assert "tenant" in response.text.lower()


def test_graphql_rejects_invalid_tenant_header() -> None:
    client = make_graphql_client()

    response = client.post(
        "/graphql",
        json={"query": "{ __typename }"},
        headers={"X-Aurum-Tenant": "INVALID TENANT"},
    )

    assert response.status_code == 400


def test_graphql_allows_requests_with_valid_tenant() -> None:
    client = make_graphql_client()

    response = client.post(
        "/graphql",
        json={"query": "{ __typename }"},
        headers={"X-Aurum-Tenant": "tenant-abc"},
    )

    # GraphQL may still report execution errors, but the HTTP status must reflect acceptance
    assert response.status_code == 200
    body = response.json()
    assert "errors" in body or "data" in body
