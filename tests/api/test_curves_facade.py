from __future__ import annotations

import importlib


async def _noop(*args, **kwargs):
    pass


class _FakeCurvesService:
    def __init__(self, *a, **k):
        self._calls = []

    async def list_curves(self, *, offset: int, limit: int, name_filter: str | None = None):
        self._calls.append(("list", offset, limit, name_filter))
        # return simple objects with model_dump
        class _Item:
            def __init__(self):
                self.id = "c1"
                self.name = "Curve 1"
                self.description = None
                self.data_points = 42
                self.created_at = None

            def model_dump(self):
                return {
                    "id": self.id,
                    "name": self.name,
                    "description": self.description,
                    "data_points": self.data_points,
                    "created_at": self.created_at,
                }

        return [_Item()]

    async def get_curve_diff(self, *, curve_id: str, from_timestamp: str, to_timestamp: str):
        class _Item:
            def __init__(self):
                self.id = curve_id
                self.name = curve_id
                self.description = f"Diff {from_timestamp}->{to_timestamp}"
                self.data_points = 3
                self.created_at = None

            def model_dump(self):
                return {
                    "id": self.id,
                    "name": self.name,
                    "description": self.description,
                    "data_points": self.data_points,
                    "created_at": self.created_at,
                }

        return _Item()


import pytest


@pytest.mark.asyncio
async def test_curves_facade_list_and_diff(monkeypatch):
    # Monkeypatch the underlying v2 service that the façade imports internally
    mod = importlib.import_module("aurum.api.curves_v2_service")
    monkeypatch.setattr(mod, "CurvesV2Service", lambda: _FakeCurvesService())

    from aurum.api.services import CurvesService

    svc = CurvesService()
    # list_curves returns at least one item with model_dump
    res = await svc.list_curves(offset=0, limit=10, name_filter=None)
    assert res and hasattr(res[0], "model_dump")

    diff = await svc.get_curve_diff(curve_id="curve-x", from_timestamp="2025-01-01", to_timestamp="2025-01-02")
    assert diff and hasattr(diff, "model_dump")
