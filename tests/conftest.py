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
