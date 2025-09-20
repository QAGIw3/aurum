from __future__ import annotations

import json
from datetime import date

import pytest

from aurum.api import service


def test_build_sql_diff_without_filters_uses_where():
    sql = service._build_sql_diff(  # type: ignore[attr-defined]
        asof_a=date(2024, 1, 1),
        asof_b=date(2024, 1, 2),
        curve_key=None,
        asset_class=None,
        iso=None,
        location=None,
        market=None,
        product=None,
        block=None,
        tenor_type=None,
        limit=10,
        offset=0,
        cursor_after=None,
    )
    # Should not contain an AND without a WHERE
    assert " FROM iceberg.market.curve_observation AND " not in sql
    # Should include a WHERE clause with asof_date filter
    assert " FROM iceberg.market.curve_observation WHERE asof_date IN (DATE '2024-01-01', DATE '2024-01-02')" in sql


def test_build_sql_diff_with_filter_adds_and():
    sql = service._build_sql_diff(  # type: ignore[attr-defined]
        asof_a=date(2024, 1, 1),
        asof_b=date(2024, 1, 2),
        curve_key="FOO",
        asset_class=None,
        iso=None,
        location=None,
        market=None,
        product=None,
        block=None,
        tenor_type=None,
        limit=10,
        offset=0,
        cursor_after=None,
    )
    assert " WHERE curve_key = 'FOO' AND asof_date IN " in sql


def test_scenario_output_cache_invalidation(monkeypatch):
    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows
            self.description = [
                ("tenant_id",),
                ("scenario_id",),
                ("run_id",),
                ("asof_date",),
                ("curve_key",),
                ("tenor_type",),
                ("contract_month",),
                ("tenor_label",),
                ("metric",),
                ("value",),
                ("band_lower",),
                ("band_upper",),
                ("attribution",),
                ("version_hash",),
            ]

        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
            return False

        def cursor(self):
            return FakeCursor(self._rows)

    class FakeRedis:
        def __init__(self) -> None:
            self.store: dict[str, str] = {}
            self.sets: dict[str, set[str]] = {}

        def get(self, key: str):
            return self.store.get(key)

        def setex(self, key: str, _ttl: int, value: str) -> None:
            self.store[key] = value

        def sadd(self, key: str, member: str) -> None:
            self.sets.setdefault(key, set()).add(member)

        def expire(self, key: str, _ttl: int) -> None:
            return None

        def smembers(self, key: str):
            members = self.sets.get(key, set())
            return {member.encode("utf-8") for member in members}

        def delete(self, *keys: str):
            for key in keys:
                self.store.pop(key, None)
                self.sets.pop(key, None)

    fake_rows = [
        (
            "tenant-1",
            "scn-1",
            "run-1",
            date(2025, 1, 1),
            "curve-a",
            "MONTHLY",
            date(2025, 1, 1),
            "2025-01",
            "mid",
            42.0,
            None,
            None,
            json.dumps({"component": "policy", "delta": 0.1}),
            "hash",
        )
    ]

    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda _cfg: fake_redis)
    monkeypatch.setattr(service, "_require_trino", lambda: lambda **_kw: FakeConnection(fake_rows))

    cfg = service.CacheConfig(redis_url="redis://localhost", ttl_seconds=60)
    rows, _elapsed = service.query_scenario_outputs(
        service.TrinoConfig(),
        cfg,
        tenant_id="tenant-1",
        scenario_id="scn-1",
        curve_key=None,
        tenor_type=None,
        metric=None,
        limit=10,
    )
    assert rows
    assert rows[0]["run_id"] == "run-1"
    index_key = f"{cfg.namespace}:scenario-outputs:index:tenant-1:scn-1" if cfg.namespace else "scenario-outputs:index:tenant-1:scn-1"
    assert index_key in fake_redis.sets
    cache_keys = list(fake_redis.store.keys())
    assert cache_keys

    service.invalidate_scenario_outputs_cache(cfg, tenant_id="tenant-1", scenario_id="scn-1")
    assert not fake_redis.store
    assert index_key not in fake_redis.sets


def test_query_scenario_outputs_rejects_missing_tenant():
    cache_cfg = service.CacheConfig(redis_url=None, ttl_seconds=60)
    with pytest.raises(ValueError, match="tenant_id is required"):
        service.query_scenario_outputs(
            service.TrinoConfig(),
            cache_cfg,
            tenant_id="",
            scenario_id="scn-1",
            curve_key=None,
            tenor_type=None,
            metric=None,
            limit=10,
        )


def test_query_ppa_valuation_builds_cashflows(monkeypatch):
    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows
            self.description = [
                ("contract_month",),
                ("asof_date",),
                ("tenor_label",),
                ("metric",),
                ("value",),
                ("metric_currency",),
                ("metric_unit",),
                ("metric_unit_denominator",),
                ("curve_key",),
                ("tenor_type",),
                ("run_id",),
            ]

        def execute(self, *_args, **_kwargs) -> None:
            return None

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[override]
            return False

        def cursor(self):
            return FakeCursor(self._rows)

    sample_rows = [
        (date(2025, 1, 1), date(2025, 1, 1), "2025-01", "mid", 60.0, "USD", "USD/MWh", "MWh", "curve-a", "MONTHLY", "run-1"),
        (date(2025, 2, 1), date(2025, 2, 1), "2025-02", "mid", 55.0, "USD", "USD/MWh", "MWh", "curve-a", "MONTHLY", "run-1"),
    ]

    monkeypatch.setattr(service, "_require_trino", lambda: lambda **_kw: FakeConnection(sample_rows))

    options = {
        "ppa_price": 50.0,
        "volume_mwh": 10.0,
        "discount_rate": 0.12,
        "upfront_cost": 100.0,
    }

    rows, _elapsed = service.query_ppa_valuation(
        service.TrinoConfig(),
        scenario_id="scn-1",
        tenant_id="tenant-1",
        options=options,
    )

    cashflows = [row for row in rows if row["metric"] == "cashflow"]
    assert len(cashflows) == 2
    assert cashflows[0]["value"] == pytest.approx(100.0)
    assert cashflows[0]["currency"] == "USD"
    assert cashflows[0]["curve_key"] == "curve-a"
    assert cashflows[0]["run_id"] == "run-1"

    discount_rate = options["discount_rate"]
    monthly_rate = (1 + discount_rate) ** (1 / 12) - 1
    expected_npv = -options["upfront_cost"]
    expected_npv += 100.0 / (1 + monthly_rate)
    expected_npv += 50.0 / (1 + monthly_rate) ** 2
    npv_row = next(row for row in rows if row["metric"] == "NPV")
    assert npv_row["value"] == pytest.approx(round(expected_npv, 4))

    irr_row = next(row for row in rows if row["metric"] == "IRR")
    expected_irr = round(service._compute_irr([-options["upfront_cost"], 100.0, 50.0]) or 0.0, 6)
    assert irr_row["value"] == expected_irr
