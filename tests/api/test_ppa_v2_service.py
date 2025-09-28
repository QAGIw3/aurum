from datetime import date, datetime
from decimal import Decimal

import pytest

import aurum.api.ppa_v2_service as svc_module


@pytest.mark.asyncio
@pytest.mark.unit
async def test_list_valuations_hardens_payload(monkeypatch):
    sentinel_settings = object()
    monkeypatch.setattr(svc_module, "get_settings", lambda: sentinel_settings)
    monkeypatch.setattr(
        svc_module.TrinoConfig,
        "from_settings",
        classmethod(lambda cls, settings: object()),
    )

    service = svc_module.PpaV2Service()

    rows = [
        {
            "asof_date": datetime(2024, 1, 5, 12, 0),
            "period_start": "2024-01-01",
            "period_end": date(2024, 1, 31),
            "metric": "NPV",
            "npv": "123.45",
            "cashflow": " 99.10 ",
            "irr": "0.101",
            "metric_currency": " usd/mwh ",
        },
        {
            "asof_date": "not-a-date",
            "period_start": None,
            "period_end": "2024/02/29",
            "metric": None,
            "npv": "not-a-number",
            "value": "456.7",
            "cashflow": "",
            "irr": None,
            "metric_currency": None,
            "metric_unit": "eur/MWh",
        },
        {
            "asof_date": date(2024, 3, 1),
            "period_start": date(2024, 3, 1),
            "period_end": date(2024, 3, 31),
            "metric": "NPV",
            "npv": None,
            "value": None,
            "cashflow": 0,
            "irr": 0,
            "metric_currency": "",
        },
    ]

    class FakeInnerService:
        def list_contract_valuation_rows(self, **kwargs):
            assert kwargs["contract_id"] == "C1"
            assert kwargs["tenant_id"] == "tenant-123"
            return rows, 0.0

    service._service = FakeInnerService()

    payload = await service.list_valuations(
        tenant_id="tenant-123",
        contract_id="C1",
        offset=0,
        limit=10,
    )

    assert len(payload) == 3

    first = payload[0]
    assert first["valuation_date"] == "2024-01-05"
    assert first["period_start"] == "2024-01-01"
    assert first["period_end"] == "2024-01-31"
    assert first["present_value"] == pytest.approx(123.45)
    assert isinstance(first["cashflow"], float)
    assert first["cashflow"] == pytest.approx(99.10)
    assert first["irr"] == pytest.approx(0.101)
    assert first["currency"] == "USD"

    second = payload[1]
    assert second["valuation_date"] is None
    assert second["period_start"] is None
    assert second["period_end"] is None
    assert second["present_value"] == pytest.approx(456.7)
    assert second["cashflow"] is None
    assert second["irr"] is None
    assert second["currency"] == "EUR"

    third = payload[2]
    assert third["valuation_date"] == "2024-03-01"
    assert third["period_start"] == "2024-03-01"
    assert third["period_end"] == "2024-03-31"
    assert third["present_value"] is None
    assert third["cashflow"] == 0.0
    assert isinstance(third["cashflow"], float)
    assert third["irr"] == 0.0
    assert isinstance(third["irr"], float)
    assert third["currency"] == "USD"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_list_valuations_handles_edge_cases(monkeypatch):
    sentinel_settings = object()
    monkeypatch.setattr(svc_module, "get_settings", lambda: sentinel_settings)
    monkeypatch.setattr(
        svc_module.TrinoConfig,
        "from_settings",
        classmethod(lambda cls, settings: object()),
    )

    service = svc_module.PpaV2Service()

    rows = [
        {
            "asof_date": " 2024-04-01T13:45:30+00:00 ",
            "period_start": "2024-04-01 00:00:00",
            "period_end": "2024-04-30Z",
            "metric": "NPV",
            "npv": Decimal("321.09"),
            "cashflow": Decimal("10"),
            "irr": "NaN",
            "metric_currency": "cad per mwh",
        },
        {
            "asof_date": "2024-04-15T00:00:00Z",
            "period_start": "",
            "period_end": "n/a",
            "metric": "IRR",
            "npv": "Infinity",
            "value": " -123.45 ",
            "cashflow": "nan",
            "irr": Decimal("0.05"),
            "currency": "jpy ",
        },
        {
            "asof_date": "not-a-date",
            "period_start": "2024-05-01",
            "period_end": "2024-05-31",
            "metric": "CASHFLOW",
            "npv": " ",
            "cashflow": 0,
            "irr": "0",
            "metric_currency": " ",
            "currency": None,
            "metric_unit": "123",
        },
    ]

    class FakeInnerService:
        def list_contract_valuation_rows(self, **kwargs):
            assert kwargs["contract_id"] == "C-edge"
            assert kwargs["tenant_id"] == "tenant-edge"
            return rows, 0.0

    service._service = FakeInnerService()

    payload = await service.list_valuations(
        tenant_id="tenant-edge",
        contract_id="C-edge",
        offset=0,
        limit=10,
    )

    assert len(payload) == 3

    first = payload[0]
    assert first["valuation_date"] == "2024-04-01"
    assert first["period_start"] == "2024-04-01"
    assert first["period_end"] == "2024-04-30"
    assert first["present_value"] == pytest.approx(321.09)
    assert isinstance(first["cashflow"], float) and first["cashflow"] == pytest.approx(10.0)
    assert first["irr"] is None
    assert first["currency"] == "CAD"

    second = payload[1]
    assert second["valuation_date"] == "2024-04-15"
    assert second["period_start"] is None
    assert second["period_end"] is None
    assert second["present_value"] == pytest.approx(-123.45)
    assert second["cashflow"] is None
    assert second["irr"] == pytest.approx(0.05)
    assert second["currency"] == "JPY"

    third = payload[2]
    assert third["valuation_date"] is None
    assert third["period_start"] == "2024-05-01"
    assert third["period_end"] == "2024-05-31"
    assert third["present_value"] is None
    assert third["cashflow"] == 0.0
    assert third["irr"] == pytest.approx(0.0)
    assert third["currency"] == "USD"
