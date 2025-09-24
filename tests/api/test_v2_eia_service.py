import types
import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_datasets_pagination(monkeypatch):
    from aurum.api.eia_v2_service import EiaV2Service

    # Mock the reference catalog iterator
    class _DS:
        def __init__(self, path):
            self.dataset_path = path
            self.title = f"Title {path}"
            self.description = f"Desc {path}"
            self.last_updated = "2024-01-01"

    datasets = [_DS(f"cat/{i}") for i in range(5)]

    import aurum.api.eia_v2_service as svc_mod
    monkeypatch.setattr(svc_mod.ref_eia, "iter_datasets", lambda: datasets)

    svc = EiaV2Service()
    page1, total = await svc.list_datasets(offset=0, limit=2)
    assert total == 5
    assert len(page1) == 2
    page2, _ = await svc.list_datasets(offset=2, limit=2)
    assert [d.dataset_path for d in page2] == ["cat/2", "cat/3"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_series_uses_pagination(monkeypatch):
    from aurum.api.eia_v2_service import EiaV2Service

    rows = [
        {"series_id": "S", "period": f"P{i}", "period_start": f"2024-01-0{i+1}", "value": float(i), "unit": "MW"}
        for i in range(5)
    ]

    import aurum.api.eia_v2_service as svc_mod

    def fake_query_eia_series(*args, **kwargs):
        limit = int(kwargs.get("limit", 10))
        offset = int(kwargs.get("offset", 0))
        return rows[offset : offset + limit], 1.0

    monkeypatch.setattr(svc_mod, "query_eia_series", fake_query_eia_series)

    svc = EiaV2Service()
    page = await svc.get_series(series_id="S", start_date=None, end_date=None, offset=1, limit=2)
    assert [p.period for p in page] == ["P1", "P2"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_dimensions_returns_values(monkeypatch):
    from aurum.api.eia_v2_service import EiaV2Service

    values = {
        "dataset": ["d1"],
        "area": ["a1"],
        "sector": ["s1"],
        "unit": ["u1"],
        "canonical_unit": ["cu"],
        "canonical_currency": ["USD"],
        "frequency": ["M"],
        "source": ["eia"],
    }

    import aurum.api.eia_v2_service as svc_mod
    monkeypatch.setattr(svc_mod, "query_eia_series_dimensions", lambda *a, **k: (values, 1.0))

    svc = EiaV2Service()
    out = await svc.get_series_dimensions()
    assert out["dataset"] == ["d1"]
