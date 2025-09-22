"""Admin/observability endpoint tests, including auth/metrics behavior."""

import importlib.util
import pathlib
import sys
import types

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

_SRC_PATH = None
for parent in pathlib.Path(__file__).resolve().parents:
    if parent.name == "src":
        _SRC_PATH = parent
        break

if _SRC_PATH is None:
    raise RuntimeError("Unable to locate project src directory")

if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

from aurum.core.settings import AurumSettings  # noqa: E402

if "aurum.api" not in sys.modules:
    api_pkg = types.ModuleType("aurum.api")
    api_pkg.__path__ = [str(_SRC_PATH / "aurum" / "api")]
    sys.modules["aurum.api"] = api_pkg


def _load_api_module(name: str):
    if name in sys.modules:
        return sys.modules[name]
    module_path = _SRC_PATH / "aurum" / "api" / f"{name.split('.')[-1]}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module {name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


routes = _load_api_module("aurum.api.routes")
state = _load_api_module("aurum.api.state")
observability_api = _load_api_module("aurum.observability.api")


class _StubMetricsCollector:
    async def collect_metrics(self):
        return []


class _StubTraceCollector:
    async def get_active_traces(self):
        return {}

    async def get_trace_summary(self, trace_id):  # pragma: no cover - not used directly
        return None


class _StubSpan:
    pass


def _build_app(monkeypatch, *, admin_groups):
    settings = AurumSettings()
    auth_cfg = settings.auth.model_copy(update={"admin_groups": admin_groups})
    settings = settings.model_copy(update={"auth": auth_cfg})

    routes.configure_routes(settings)
    state.configure(settings)

    monkeypatch.setattr(observability_api, "get_metrics_collector", lambda: _StubMetricsCollector())
    monkeypatch.setattr(observability_api, "get_trace_collector", lambda: _StubTraceCollector())

    app = FastAPI()
    app.state.settings = settings

    def guard(principal=Depends(routes._get_principal)):
        routes._require_admin(principal)

    app.include_router(observability_api.router, dependencies=[Depends(guard)])
    return app


def test_observability_requires_admin(monkeypatch):
    app = _build_app(monkeypatch, admin_groups=("aurum-admins",))
    client = TestClient(app)

    response = client.get("/v1/observability/health")
    assert response.status_code == 403


def test_observability_allows_admin_principal(monkeypatch):
    app = _build_app(monkeypatch, admin_groups=("aurum-admins",))

    @app.middleware("http")
    async def inject_principal(request, call_next):  # pragma: no cover - executed via client
        request.state.principal = {"groups": ["aurum-admins"]}
        return await call_next(request)

    client = TestClient(app)
    response = client.get("/v1/observability/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] in {"healthy", "unhealthy"}
