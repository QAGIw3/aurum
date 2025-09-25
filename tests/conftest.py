import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
for path in (ROOT, SRC):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

# Ensure older pytest-asyncio style decorator remains available
try:
    import pytest
    import pytest_asyncio

    if not hasattr(pytest_asyncio, "asyncio"):
        pytest_asyncio.asyncio = pytest.mark.asyncio  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - defensive import shim
    pass

# Opt-in fixture to enable default settings auto-init in selected test modules.
# Usage in a module: `pytestmark = pytest.mark.usefixtures("enable_test_default_settings")`
import os
import pytest


@pytest.fixture(scope="module")
def enable_test_default_settings():
    """Set AURUM_TEST_DEFAULT_SETTINGS=1 for all tests in a module.

    Apply with `pytestmark = pytest.mark.usefixtures("enable_test_default_settings")`
    at the top of a test module to opt-in.
    """
    prev = os.environ.get("AURUM_TEST_DEFAULT_SETTINGS")
    os.environ["AURUM_TEST_DEFAULT_SETTINGS"] = "1"
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("AURUM_TEST_DEFAULT_SETTINGS", None)
        else:
            os.environ["AURUM_TEST_DEFAULT_SETTINGS"] = prev
