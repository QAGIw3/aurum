from __future__ import annotations

import pytest

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from aurum.api.router_registry import get_v1_router_specs, _ensure_v1_deprecation
from aurum.core import AurumSettings


@pytest.fixture
def reset_split_flags(monkeypatch):
    flags = [
        "AURUM_API_V1_SPLIT_EIA",
        "AURUM_API_V1_SPLIT_ISO",
        "AURUM_API_V1_SPLIT_PPA",
        "AURUM_API_V1_SPLIT_DROUGHT",
        "AURUM_API_V1_SPLIT_ADMIN",
        "AURUM_API_V1_SPLIT_METADATA",
    ]
    for flag in flags:
        monkeypatch.delenv(flag, raising=False)
    monkeypatch.setenv("AURUM_API_LIGHT_INIT", "1")
    yield
    for flag in flags:
        monkeypatch.delenv(flag, raising=False)
    monkeypatch.delenv("AURUM_API_LIGHT_INIT", raising=False)


def test_v1_router_specs_deduplicates_split_modules(monkeypatch, reset_split_flags):
    monkeypatch.setenv("AURUM_API_V1_SPLIT_PPA", "1")
    settings = AurumSettings()

    specs = get_v1_router_specs(settings)
    ppa_specs = [spec for spec in specs if spec.name == "aurum.api.v1.ppa"]

    assert len(ppa_specs) == 1


def test_v1_router_specs_curves_default(monkeypatch, reset_split_flags):
    settings = AurumSettings()

    specs = get_v1_router_specs(settings)
    names = [spec.name for spec in specs if spec.name]

    assert "aurum.api.v1.curves" in names
    assert "aurum.api.curves" not in names


def test_v1_router_specs_curves_flag_is_ignored(monkeypatch, reset_split_flags):
    monkeypatch.setenv("AURUM_API_V1_SPLIT_CURVES", "0")
    settings = AurumSettings()

    specs = get_v1_router_specs(settings)
    names = [spec.name for spec in specs if spec.name]

    assert "aurum.api.v1.curves" in names
    assert "aurum.api.curves" not in names


def test_v1_router_specs_unique_when_all_flags_enabled(monkeypatch, reset_split_flags):
    for flag in [
        "AURUM_API_V1_SPLIT_CURVES",  # deprecated but tested for backward compatibility
        "AURUM_API_V1_SPLIT_EIA",
        "AURUM_API_V1_SPLIT_ISO",
        "AURUM_API_V1_SPLIT_PPA",    # deprecated but tested for backward compatibility
        "AURUM_API_V1_SPLIT_DROUGHT",
        "AURUM_API_V1_SPLIT_ADMIN",
        "AURUM_API_V1_SPLIT_METADATA",
    ]:
        monkeypatch.setenv(flag, "1")

    settings = AurumSettings()
    specs = get_v1_router_specs(settings)

    names = [spec.name for spec in specs if spec.name is not None]

    assert len(names) == len(set(names))


def test_v1_router_specs_routes_marked_deprecated(reset_split_flags):
    settings = AurumSettings()
    specs = get_v1_router_specs(settings)

    for spec in specs:
        for route in spec.router.routes:
            if isinstance(route, APIRoute):
                assert route.deprecated is True


def test_ensure_v1_deprecation_adds_headers():
    router = APIRouter()

    @router.get("/v1/demo")
    def demo_route():
        return {"ok": True}

    router = _ensure_v1_deprecation(router)

    app = FastAPI()
    app.include_router(router)

    client = TestClient(app)
    response = client.get("/v1/demo")

    assert response.status_code == 200
    assert response.headers.get("Deprecation", "").lower() == "true"
    assert "Sunset" in response.headers
    assert "X-API-Migration-Guide" in response.headers
