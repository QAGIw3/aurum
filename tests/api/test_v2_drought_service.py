import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_indices_slicing(monkeypatch):
    from aurum.api.drought_v2_service import DroughtV2Service

    rows = [
        {
            "series_id": f"S{i}",
            "dataset": "spi",
            "index": "SPI",
            "timescale": "12M",
            "valid_date": f"2024-01-{i+1:02d}",
            "region_type": "county",
            "region_id": f"C{i}",
        }
        for i in range(10)
    ]

    import aurum.api.drought_v2_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "query_drought_indices", lambda *a, **k: (rows, 1.0)
    )

    svc = DroughtV2Service()
    page = await svc.list_indices(
        dataset=None,
        index=None,
        timescale=None,
        region=None,
        region_type=None,
        region_id=None,
        start=None,
        end=None,
        offset=3,
        limit=4,
    )
    assert page == rows[3:7]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_usdm_slicing(monkeypatch):
    from aurum.api.drought_v2_service import DroughtV2Service

    rows = [
        {
            "region_type": "county",
            "region_id": f"C{i}",
            "valid_date": f"2024-01-{i+1:02d}",
            "d0_frac": 0.1 * i,
        }
        for i in range(12)
    ]

    import aurum.api.drought_v2_service as svc_mod
    monkeypatch.setattr(
        svc_mod, "query_drought_usdm", lambda *a, **k: (rows, 1.0)
    )

    svc = DroughtV2Service()
    page = await svc.list_usdm(
        region_type=None,
        region_id=None,
        start=None,
        end=None,
        offset=5,
        limit=3,
    )
    assert page == rows[5:8]

