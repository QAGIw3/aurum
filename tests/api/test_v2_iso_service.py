import pytest


@pytest.mark.unit
@pytest.mark.asyncio
async def test_lmp_last_24h_slicing(monkeypatch):
    from aurum.api.iso_v2_service import IsoV2Service

    rows = [
        {"timestamp": f"2024-01-01T00:0{i}:00Z", "location_id": f"L{i}", "value": float(i), "uom": "USD/MWh"}
        for i in range(5)
    ]

    import aurum.api.iso_v2_service as svc_mod
    monkeypatch.setattr(svc_mod, "query_iso_lmp_last_24h", lambda **k: (rows, 1.0))

    svc = IsoV2Service()
    page = await svc.lmp_last_24h(iso="PJM", offset=2, limit=2)
    assert page == rows[2:4]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_lmp_hourly_slicing(monkeypatch):
    from aurum.api.iso_v2_service import IsoV2Service

    rows = [
        {"interval_start": f"2024-01-01T{str(i).zfill(2)}:00:00Z", "location_id": f"L{i}", "price_avg": float(i)}
        for i in range(10)
    ]

    import aurum.api.iso_v2_service as svc_mod
    monkeypatch.setattr(svc_mod, "query_iso_lmp_hourly", lambda **k: (rows, 1.0))

    svc = IsoV2Service()
    page = await svc.lmp_hourly(iso="PJM", date="2024-01-01", offset=3, limit=3)
    assert page == rows[3:6]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_lmp_daily_slicing(monkeypatch):
    from aurum.api.iso_v2_service import IsoV2Service

    rows = [
        {"interval_start": f"2024-01-01T{str(i).zfill(2)}:00:00Z", "location_id": f"L{i}", "price_avg": float(i)}
        for i in range(24)
    ]

    import aurum.api.iso_v2_service as svc_mod
    monkeypatch.setattr(svc_mod, "query_iso_lmp_daily", lambda **k: (rows, 1.0))

    svc = IsoV2Service()
    page = await svc.lmp_daily(iso="PJM", date="2024-01-01", offset=5, limit=5)
    assert page == rows[5:10]
