"""Skeletons for Phase 1 dependency helpers (not collected by pytest).

Verifies `get_settings` fallback does not raise when API state is configured.
"""

from __future__ import annotations

from types import SimpleNamespace

from starlette.requests import Request

from aurum.api import state as api_state
from aurum.api.deps import get_settings
from aurum.core import AurumSettings


def _fake_request_with_state(settings: AurumSettings | None) -> Request:
    scope = {"type": "http"}
    request = Request(scope)
    request.app = SimpleNamespace(state=SimpleNamespace(settings=settings))  # type: ignore[attr-defined]
    return request


def example_usage() -> None:
    # Configure API state as in app startup
    s = AurumSettings.from_env()
    api_state.configure(s)
    # No settings on app.state; fallback to api.state
    req = _fake_request_with_state(None)
    got = get_settings(req)
    assert got is s


if __name__ == "__main__":
    example_usage()

