from __future__ import annotations

import asyncio
import pytest

from libs.services.curves_service import CurvesService


class FakeTrino:
    def __init__(self, rows):
        self._rows = rows

    async def execute_query(self, query: str):
        return self._rows


@pytest.mark.asyncio
async def test_list_curves_basic():
    rows = [
        {"id": "c1", "name": "A", "description": None, "data_points": 10, "created_at": "2025-01-01T00:00:00Z"},
        {"id": "c2", "name": "B", "description": "desc", "data_points": 5, "created_at": "2025-01-02T00:00:00Z"},
    ]
    svc = CurvesService(trino=FakeTrino(rows))
    curves, debug = await svc.list_curves(
        tenant_id="t1", offset=0, limit=10, name_filter=None, include_debug=False
    )
    assert len(curves) == 2
    assert curves[0].id == "c1"
    assert curves[1].name == "B"
    assert debug == {}


@pytest.mark.asyncio
async def test_get_curve_diff():
    rows = [
        {"id": "c1", "name": "A", "description": None, "data_points": 10, "created_at": "2025-01-01T00:00:00Z"}
    ]
    svc = CurvesService(trino=FakeTrino(rows))
    curve = await svc.get_curve_diff(curve_id="c1", from_timestamp="2025-01-01T00:00:00Z", to_timestamp="2025-01-02T00:00:00Z")
    assert curve.id == "c1"
    assert curve.data_points == 10


