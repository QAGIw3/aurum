import pathlib
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
