import builtins
import importlib.util
import pathlib
import sys
import types

SRC_DIR = None
for parent in pathlib.Path(__file__).resolve().parents:
    if parent.name == "src":
        SRC_DIR = parent
        break

if SRC_DIR is None:
    raise RuntimeError("Unable to locate project src directory")

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from aurum.core.settings import AurumSettings, RedisMode  # noqa: E402

if "aurum.api" not in sys.modules:
    api_pkg = types.ModuleType("aurum.api")
    api_pkg.__path__ = [str(SRC_DIR / "aurum" / "api")]
    sys.modules["aurum.api"] = api_pkg


def _load_api_module(name: str):
    if name in sys.modules:
        return sys.modules[name]
    module_path = SRC_DIR / "aurum" / "api" / f"{name.split('.')[-1]}.py"
    spec = importlib.util.spec_from_file_location(name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module {name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CacheConfig = _load_api_module("aurum.api.config").CacheConfig
service = _load_api_module("aurum.api.service")


def _settings_with_redis(**updates) -> AurumSettings:
    settings = AurumSettings()
    redis_cfg = settings.redis.model_copy(update=updates)
    return settings.model_copy(update={"redis": redis_cfg})


def test_cache_config_parses_sentinel_endpoints():
    settings = _settings_with_redis(
        mode=RedisMode.SENTINEL,
        sentinel_endpoints=("redis-host", "redis-2:26400"),
        sentinel_master="aurum-master",
    )
    cfg = CacheConfig.from_settings(settings)
    assert cfg.mode == RedisMode.SENTINEL.value
    assert cfg.sentinel_endpoints == (("redis-host", 26379), ("redis-2", 26400))
    assert cfg.sentinel_master == "aurum-master"


def test_cache_config_parses_cluster_nodes():
    settings = _settings_with_redis(
        mode=RedisMode.CLUSTER,
        cluster_nodes=("node-a:7000", "node-b:7001"),
    )
    cfg = CacheConfig.from_settings(settings)
    assert cfg.cluster_nodes == ("node-a:7000", "node-b:7001")


def test_maybe_redis_client_returns_none_when_dependency_missing(monkeypatch):
    cfg = CacheConfig(redis_url="redis://localhost:6379/0")
    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name.startswith("redis"):
            raise ModuleNotFoundError("redis not installed")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    assert service._maybe_redis_client(cfg) is None


def test_maybe_redis_client_returns_none_for_incomplete_sentinel():
    cfg = CacheConfig(mode="sentinel", sentinel_endpoints=(), sentinel_master=None)
    assert service._maybe_redis_client(cfg) is None
