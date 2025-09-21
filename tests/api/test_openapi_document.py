import importlib
import pathlib
import re
from typing import Iterable

import pytest

yaml = pytest.importorskip("yaml", reason="PyYAML is required for OpenAPI validation")

SPEC_PATH = pathlib.Path(__file__).resolve().parents[2] / "openapi" / "aurum.yaml"


def _load_spec():
    return yaml.safe_load(SPEC_PATH.read_text(encoding="utf-8"))


def test_scenario_outputs_documented():
    spec = _load_spec()
    paths = spec.get("paths", {})
    assert "/v1/scenarios/{id}/outputs" in paths
    outputs = paths["/v1/scenarios/{id}/outputs"].get("get", {})
    responses = outputs.get("responses", {})
    assert "200" in responses
    schema = responses["200"].get("content", {}).get("application/json", {}).get("schema", {})
    assert schema.get("$ref") == "#/components/schemas/ScenarioOutputResponse"


def test_dimensions_counts_documented():
    spec = _load_spec()
    components = spec.get("components", {})
    schemas = components.get("schemas", {})
    dims_response = schemas.get("DimensionsResponse", {})
    counts = dims_response.get("properties", {}).get("counts")
    assert counts and counts.get("$ref") == "#/components/schemas/DimensionsCountData"


def test_ready_endpoint_documented():
    spec = _load_spec()
    assert "/ready" in spec.get("paths", {})


def _documented_operations(spec: dict) -> set[tuple[str, str]]:
    paths = spec.get("paths", {})
    documented: set[tuple[str, str]] = set()
    for path, methods in paths.items():
        if not isinstance(methods, dict):
            continue
        for method, details in methods.items():
            method_lower = str(method).lower()
            if method_lower not in {"get", "post", "put", "delete", "patch"}:
                continue
            documented.add((_normalise_path(path), method_lower))
    return documented


def _iter_api_routes() -> Iterable[tuple[str, str]]:
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.routing import APIRoute

    module = importlib.import_module("aurum.api.app")
    ignore_paths = {"/docs", "/redoc", "/openapi.json"}
    skip_methods = {"head", "options"}
    for route in module.app.routes:
        if not isinstance(route, APIRoute):
            continue
        path = _normalise_path(route.path)
        if path in ignore_paths:
            continue
        for method in route.methods:
            method_lower = method.lower()
            if method_lower in skip_methods:
                continue
            yield path, method_lower


def _normalise_path(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{}", str(path))


def test_openapi_includes_fastapi_routes():
    spec = _load_spec()
    documented = _documented_operations(spec)
    missing = {(path, method) for path, method in _iter_api_routes() if (path, method) not in documented}
    allowed_missing = {
        ("/health", "get"),
        ("/v1/metadata/calendars", "get"),
        ("/v1/metadata/calendars/{}/blocks", "get"),
        ("/v1/metadata/calendars/{}/expand", "get"),
        ("/v1/metadata/calendars/{}/hours", "get"),
        ("/v1/metadata/eia/datasets", "get"),
        ("/v1/metadata/eia/datasets/{}", "get"),
        ("/v1/metadata/locations", "get"),
        ("/v1/metadata/locations/{}/{}", "get"),
        ("/v1/metadata/units", "get"),
        ("/v1/metadata/units/mapping", "get"),
        ("/v1/admin/cache/scenario/{}/invalidate", "post"),
    }
    # allowlist explicitly documented differences while enforcing no new gaps
    assert missing.issubset(allowed_missing), f"OpenAPI spec missing operations: {sorted(missing)}"
