from __future__ import annotations

import pytest

from fastapi import APIRouter, FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from aurum.api.router_registry import (
    get_v1_router_specs,
    get_v2_router_specs,
    _ensure_v1_deprecation,
    _ensure_v2_tenant_dependency,
    _build_specs,
    _try_import_router,
)
from fastapi import APIRouter
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


def test_v2_routers_require_tenant_dependency(monkeypatch, reset_split_flags):
    settings = AurumSettings()
    specs = get_v2_router_specs(settings)
    # All v2 routers should have a dependency callable list and include require_tenant_id marker
    for spec in specs:
        deps = getattr(spec.router, "dependencies", ()) or ()
        assert isinstance(deps, (list, tuple))
        assert len(deps) >= 1


def test_registry_includes_admin_and_offload_routes(monkeypatch, reset_split_flags):
    settings = AurumSettings()
    specs = get_v1_router_specs(settings)
    names = {spec.name for spec in specs if spec.name}
    # Ensure supplemental modules are present (non-exhaustive assertions)
    assert "aurum.api.offload" in names
    assert "aurum.api.runtime_config" in names
    assert "aurum.api.version_management" in names
    assert "aurum.api.database.performance" in names
    assert "aurum.api.rate_limiting.admin_router" in names


def test_import_fallback_attr_names_exercised(monkeypatch):
    """Ensure _try_import_router falls back to non-default attribute names when needed.

    This disables LIGHT_INIT so real imports occur and the fallback path can be exercised
    for modules like aurum.api.offload which export `offload_router` instead of `router`.
    """
    monkeypatch.delenv("AURUM_API_LIGHT_INIT", raising=False)
    monkeypatch.setenv("AURUM_API_V1_SPLIT_EIA", "0")
    monkeypatch.setenv("AURUM_API_V1_SPLIT_ISO", "0")
    monkeypatch.setenv("AURUM_API_V1_SPLIT_PPA", "0")
    monkeypatch.setenv("AURUM_API_V1_SPLIT_DROUGHT", "0")
    monkeypatch.setenv("AURUM_API_V1_SPLIT_ADMIN", "0")

    settings = AurumSettings()
    specs = get_v1_router_specs(settings)
    offload = next((s for s in specs if s.name == "aurum.api.offload"), None)
    assert offload is not None
    # Validate we got a real router with the expected prefix
    prefix = getattr(offload.router, "prefix", "")
    assert prefix == "/v1/admin/offload"


def test_build_specs_skips_duplicates(monkeypatch):
    monkeypatch.setenv("AURUM_API_LIGHT_INIT", "1")
    modules = ("aurum.api.v1.curves", "aurum.api.v1.curves")
    specs = _build_specs(modules, seen=set())
    names = [s.name for s in specs if s.name]
    assert names.count("aurum.api.v1.curves") == 1


def test_try_import_router_logs_warning_for_non_router_module(caplog, monkeypatch):
    monkeypatch.delenv("AURUM_API_LIGHT_INIT", raising=False)
    caplog.set_level("WARNING")
    result = _try_import_router("aurum.api.app")
    # Module exists but doesn't expose a router; expect None and a warning
    assert result is None
    assert any("does not expose an APIRouter" in rec.message for rec in caplog.records)


def test_v2_tenant_dependency_idempotent():
    router = APIRouter()
    r1 = _ensure_v2_tenant_dependency(router)
    r2 = _ensure_v2_tenant_dependency(router)
    assert r1 is r2
    assert getattr(router, "_aurum_v2_tenant_dependency", False) is True


def test_get_v1_specs_seen_skip_paths(monkeypatch):
    monkeypatch.delenv("AURUM_API_LIGHT_INIT", raising=False)
    monkeypatch.setenv("AURUM_API_V1_SPLIT_EIA", "1")

    # Wrap _build_specs to pre-populate seen with entries that will appear in later loops
    import aurum.api.router_registry as rr

    original_build_specs = rr._build_specs

    def wrapped_build_specs(module_paths, *, seen=None):  # type: ignore[override]
        specs = original_build_specs(module_paths, seen=seen)
        if seen is not None:
            seen.add("aurum.api.offload")
            seen.add("aurum.api.v1.eia")
        return specs

    monkeypatch.setattr(rr, "_build_specs", wrapped_build_specs)
    settings = AurumSettings()
    # Should execute without trying to include offload/eia twice due to seen set
    specs = get_v1_router_specs(settings)
    assert isinstance(specs, list)
