"""Root test configuration.

Ensures the real ``src/aurum`` package is imported before any tests dynamically
load modules under the ``aurum.*`` namespace to avoid shadowing by DAG stubs.
"""

import os
import sys
from pathlib import Path

# Prepend src to sys.path and import the top-level aurum package early
_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_PATH = _REPO_ROOT / "src"
if str(_SRC_PATH) not in sys.path:
    sys.path.insert(0, str(_SRC_PATH))

# Ensure repository root is on sys.path so "tests" imports resolve as a package
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Hint to API factory to avoid heavy init paths where supported
os.environ.setdefault("AURUM_API_LIGHT_INIT", "1")

import aurum  # noqa: F401  # ensure package is registered in sys.modules

# Global skip configuration for heavy/optional suites
import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "external: requires external services or heavy deps")
    config.addinivalue_line("markers", "airflow: requires Apache Airflow present")


def pytest_collection_modifyitems(config, items):
    skip_external = pytest.mark.skip(reason="external tests skipped by default; set RUN_EXTERNAL_TESTS=1 to enable")
    skip_airflow = pytest.mark.skip(reason="airflow-dependent tests skipped by default; set RUN_AIRFLOW_TESTS=1 to enable")

    run_external = (os.getenv("RUN_EXTERNAL_TESTS", "0").lower() in {"1", "true", "yes"})
    run_airflow = (os.getenv("RUN_AIRFLOW_TESTS", "0").lower() in {"1", "true", "yes"})
    core_only = (os.getenv("RUN_CORE_ONLY", "0").lower() in {"1", "true", "yes"})

    skip_non_core = pytest.mark.skip(reason="non-core test skipped; set RUN_CORE_ONLY=0 to include")

    for item in items:
        # Skip by markers
        if "external" in item.keywords and not run_external:
            item.add_marker(skip_external)
        if "airflow" in item.keywords and not run_airflow:
            item.add_marker(skip_airflow)

        # Heuristic: skip tests under directories that clearly depend on Airflow or external infra
        nodeid = item.nodeid
        if not run_airflow and (
            "/airflow_factory/" in nodeid or "/airflow/" in nodeid or "/workflow/" in nodeid
        ):
            item.add_marker(skip_airflow)
        if not run_external and (
            "/integration/" in nodeid or "/e2e/" in nodeid or "/external/" in nodeid or "/kafka/" in nodeid
        ):
            item.add_marker(skip_external)

        # Core-only: restrict to core subsets (api, cli, contract). Skip others.
        if core_only:
            core_paths = (
                "/tests/api/",
                "/src/aurum/tests/api/",
                "/tests/cli/",
                "/tests/contract/",
            )
            if not any(p in nodeid for p in core_paths):
                item.add_marker(skip_non_core)

        # If v2-only is enabled, skip any tests that explicitly target v1 endpoints or modules
        try:
            from aurum.core import get_settings  # local import to avoid early settings init
            if getattr(get_settings(), "enable_v2_only", False):
                if "/v1/" in nodeid or "aurum.api.v1" in nodeid:
                    item.add_marker(pytest.mark.skip(reason="v2-only mode: v1 tests are skipped"))
        except Exception:
            # If settings import fails in this phase, do nothing
            pass

# pytest-asyncio compatibility: some tests use deprecated decorator access
try:
    import pytest_asyncio  # type: ignore
    if not hasattr(pytest_asyncio, "asyncio"):
        # Provide alias attribute for older style usage: @pytest_asyncio.asyncio
        pytest_asyncio.asyncio = pytest.mark.asyncio  # type: ignore[attr-defined]
except Exception:
    pass

# Import common fixtures and utilities
from tests.common import create_airflow_stub, reset_state, settings_override

# Create airflow stub for DAG factory tests
create_airflow_stub()

# Common fixtures available to all tests
__all__ = ["reset_state", "settings_override"]

# Helpers to control API init mode in tests that need full router mounting
@pytest.fixture
def full_init(monkeypatch):
    """Disable LIGHT_INIT for the duration of a test to mount all routers.

    Use by adding `full_init` to the test function signature.
    """
    monkeypatch.setenv("AURUM_API_LIGHT_INIT", "0")
    yield
    monkeypatch.delenv("AURUM_API_LIGHT_INIT", raising=False)


def disable_light_init_env() -> None:
    """Imperative helper to disable LIGHT_INIT when not using fixtures."""
    os.environ["AURUM_API_LIGHT_INIT"] = "0"
