from datetime import datetime, timedelta, timezone

import pytest

from aurum.streaming import CurvePoint, MarketDataEvent, RealTimeMarketDataEngine


@pytest.mark.asyncio
async def test_real_time_engine_interpolation_and_alerts():
    engine = RealTimeMarketDataEngine()
    curve_id = "AURUM:DA"

    historical = [
        CurvePoint(tenor="2024-01", price=45.0, timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc), source="historical"),
        CurvePoint(tenor="2024-02", price=46.0, timestamp=datetime(2024, 2, 1, tzinfo=timezone.utc), source="historical"),
        CurvePoint(tenor="2024-03", price=50.0, timestamp=datetime(2024, 3, 1, tzinfo=timezone.utc), source="historical"),
    ]

    await engine.add_historical_curve(curve_id, historical)

    event1 = MarketDataEvent(
        curve_id=curve_id,
        tenor="2024-02",
        price=46.0,
        timestamp=datetime.now(timezone.utc) - timedelta(seconds=1),
        vendor="vendor-a",
    )

    report1 = await engine.ingest_event(event1)
    assert report1.reconciliation is not None
    assert report1.reconciliation.items
    assert not report1.alerts

    event2 = MarketDataEvent(
        curve_id=curve_id,
        tenor="2024-02",
        price=52.0,
        timestamp=datetime.now(timezone.utc),
        vendor="vendor-a",
    )

    report2 = await engine.ingest_event(event2)
    assert report2.alerts, "Price spike should trigger default alert rule"
    snapshot = report2.snapshot
    assert snapshot.points
    assert snapshot.interpolated, "Historical mapping should yield interpolated points"
    assert snapshot.statistics["latency_ms"] >= 0
