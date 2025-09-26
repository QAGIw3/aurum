from __future__ import annotations

import importlib


def test_admin_guard_installed_and_disabled_by_default():
    api_app = importlib.import_module("aurum.api.app")
    app = api_app.app
    assert app is not None
    # Ensure AdminRouteGuard middleware is present and disabled by default
    guard = next((mw for mw in app.user_middleware if mw.cls.__name__ == "AdminRouteGuard"), None)
    assert guard is not None
    assert guard.kwargs.get("enabled") is False

