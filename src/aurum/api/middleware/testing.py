from __future__ import annotations

"""Testing utilities for the middleware manager."""

from typing import Iterable, Optional

from fastapi import FastAPI

from aurum.core import AurumSettings
from .manager import MiddlewareManager


def build_test_app(
    *,
    settings: Optional[AurumSettings] = None,
    enable: Optional[Iterable[str]] = None,
    disable: Optional[Iterable[str]] = None,
) -> FastAPI:
    settings = settings or AurumSettings.from_env()
    app = FastAPI()
    app.state.settings = settings

    manager = MiddlewareManager()
    manager.add_defaults(settings)

    if disable:
        for name in disable:
            manager.set_enabled(name, False)
    if enable:
        for name in enable:
            manager.set_enabled(name, True)

    manager.apply(app, settings)
    return app


__all__ = ["build_test_app"]


