"""Unit tests covering API rate limiter behavior and edge cases."""

import importlib.util
import pathlib
import sys
import time
import types
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

SRC_DIR = pathlib.Path(__file__).resolve().parents[2]
PACKAGE_ROOT = SRC_DIR / "aurum"
sys.path.insert(0, str(SRC_DIR))

if "aurum" not in sys.modules:
    aurum_pkg = types.ModuleType("aurum")
    aurum_pkg.__path__ = [str(PACKAGE_ROOT)]
    sys.modules["aurum"] = aurum_pkg

if "aurum.api" not in sys.modules:
    api_pkg = types.ModuleType("aurum.api")
    api_pkg.__path__ = [str(PACKAGE_ROOT / "api")]
    sys.modules["aurum.api"] = api_pkg

_module_cache: dict[str, types.ModuleType] = {}


def _load_module(name: str, path: pathlib.Path, *, package: bool = False) -> types.ModuleType:
    if name in _module_cache:
        return _module_cache[name]
    search_locations = [str(path.parent)] if package else None
    spec = importlib.util.spec_from_file_location(
        name,
        path,
        submodule_search_locations=search_locations,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    _module_cache[name] = module
    return module


_load_module("aurum.core.enums", PACKAGE_ROOT / "core" / "enums.py")
_load_module("aurum.core.models", PACKAGE_ROOT / "core" / "models.py")
_load_module("aurum.core.pagination", PACKAGE_ROOT / "core" / "pagination.py")
_load_module("aurum.core.settings", PACKAGE_ROOT / "core" / "settings.py")
_load_module("aurum.core", PACKAGE_ROOT / "core" / "__init__.py", package=True)
_load_module("aurum.api.config", PACKAGE_ROOT / "api" / "config.py")
_load_module("aurum.api.rate_limiting", PACKAGE_ROOT / "api" / "rate_limiting" / "__init__.py", package=True)
ratelimit_module = _load_module(
    "aurum.api.rate_limiting.sliding_window",
    PACKAGE_ROOT / "api" / "rate_limiting" / "sliding_window.py",
)
RateLimitConfig = ratelimit_module.RateLimitConfig
RateLimitMiddleware = ratelimit_module.RateLimitMiddleware


def _build_client() -> TestClient:
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        cache_cfg=SimpleNamespace(redis_url=None),
        rl_cfg=RateLimitConfig(rps=1, burst=0),
    )

    @app.get("/ping")
    async def ping():  # pragma: no cover - exercised via client
        return {"ok": True}

    return TestClient(app)


def test_rate_limit_headers_include_remaining_and_reset():
    client = _build_client()

    first = client.get("/ping")
    assert first.status_code == 200
    assert first.headers["X-RateLimit-Limit"] == "1"
    assert first.headers["X-RateLimit-Remaining"] == "0"
    reset_value = float(first.headers["X-RateLimit-Reset"])
    assert 0 < reset_value <= 1.0

    second = client.get("/ping")
    assert second.status_code == 429
    assert second.headers["Retry-After"] == second.headers["X-RateLimit-Reset"]
    assert float(second.headers["Retry-After"]) >= 0.0


def test_rate_limit_recovers_after_window():
    client = _build_client()

    client.get("/ping")
    client.get("/ping")

    time.sleep(1.1)

    third = client.get("/ping")
    assert third.status_code == 200
    assert third.headers["X-RateLimit-Remaining"] in {"0", "1"}
