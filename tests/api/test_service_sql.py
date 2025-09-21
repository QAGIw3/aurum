from __future__ import annotations

import importlib
import json
import os
import time
import uuid
from datetime import date, datetime
import sys

import pytest

from aurum.api import service


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}
        self.get_called = 0

    def get(self, key: str):
        self.get_called += 1
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


SCENARIO_PIPELINE_ENABLED = os.getenv("AURUM_ENABLE_SCENARIO_PIPELINE_TESTS", "0").lower() in {
    "1",
    "true",
    "yes",
}


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


def test_build_sql_eia_series_with_filters():
    sql = service._build_sql_eia_series(  # type: ignore[attr-defined]
        series_id="EBA.PJME-ALL.NG.H",
        frequency="hourly",
        area="PJM",
        sector="ALL",
        dataset="EBA",
        unit="MW",
        source="EIA",
        start=datetime(2024, 1, 1, 0, 0, 0),
        end=datetime(2024, 1, 2, 0, 0, 0),
        limit=10,
        offset=0,
        cursor_after=None,
        cursor_before=None,
        descending=False,
    )
    assert "series_id = 'EBA.PJME-ALL.NG.H'" in sql
    assert "upper(frequency) = 'HOURLY'" in sql
    assert "period_start >= TIMESTAMP '2024-01-01 00:00:00" in sql
    assert "ORDER BY series_id ASC" in sql


def test_build_sql_eia_series_cursor_after():
    cursor = {
        "series_id": "EBA.PJME-ALL.NG.H",
        "period_start": "2024-01-01 01:00:00",
        "period": "2024-01-01T01:00:00",
    }
    sql = service._build_sql_eia_series(  # type: ignore[attr-defined]
        series_id=None,
        frequency=None,
        area=None,
        sector=None,
        dataset=None,
        unit=None,
        source=None,
        start=None,
        end=None,
        limit=5,
        offset=0,
        cursor_after=cursor,
        cursor_before=None,
        descending=False,
    )
    assert "ORDER BY series_id ASC" in sql
    assert "LIMIT 5 OFFSET 0" in sql
    assert "WHERE 1=1 AND" in sql


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

    metrics_index = "aurum:scenario-metrics:index:tenant-1:scn-1"
    fake_redis.sets[metrics_index] = {"metric-cache"}
    fake_redis.store["metric-cache"] = "{}"

    service.invalidate_scenario_outputs_cache(cfg, tenant_id="tenant-1", scenario_id="scn-1")
    assert not fake_redis.store
    assert index_key not in fake_redis.sets
    assert metrics_index not in fake_redis.sets


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


def test_query_scenario_metrics_latest_uses_cache(monkeypatch):
    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows
            self.description = [
                ("tenant_id",),
                ("scenario_id",),
                ("curve_key",),
                ("metric",),
                ("tenor_label",),
                ("latest_value",),
                ("latest_band_lower",),
                ("latest_band_upper",),
                ("latest_asof_date",),
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

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor(self._rows)

    fake_rows = [
        (
            "tenant-1",
            "scn-1",
            "curve-a",
            "mid",
            "2025-07",
            41.5,
            40.0,
            43.0,
            date(2025, 1, 1),
        )
    ]

    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda _cfg: fake_redis)
    monkeypatch.setattr(service, "_require_trino", lambda: lambda **_kw: FakeConnection(fake_rows))

    cache_cfg = service.CacheConfig(redis_url="redis://localhost", ttl_seconds=30)
    rows, elapsed = service.query_scenario_metrics_latest(
        service.TrinoConfig(),
        cache_cfg,
        scenario_id="scn-1",
        tenant_id="tenant-1",
        metric=None,
        limit=10,
    )
    assert rows and elapsed > 0
    assert fake_redis.store

    # Second invocation should hit the cache and return instantly
    rows_cached, elapsed_cached = service.query_scenario_metrics_latest(
        service.TrinoConfig(),
        cache_cfg,
        scenario_id="scn-1",
        tenant_id="tenant-1",
        metric=None,
        limit=10,
    )
    expected_cached = []
    for record in rows:
        record_copy = record.copy()
        value = record_copy.get("latest_asof_date")
        if isinstance(value, date):
            record_copy["latest_asof_date"] = value.isoformat()
        expected_cached.append(record_copy)
    assert rows_cached == expected_cached
    assert elapsed_cached == 0.0
    assert fake_redis.get_called >= 1


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


@pytest.mark.scenario_pipeline
@pytest.mark.skipif(not SCENARIO_PIPELINE_ENABLED, reason="Set AURUM_ENABLE_SCENARIO_PIPELINE_TESTS=1 to run pipeline smoke test")
def test_scenario_pipeline_smoke_invalidates_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("fastapi", reason="fastapi not installed")
    from fastapi.testclient import TestClient

    from aurum.api import scenario_service
    import aurum.scenarios.worker as worker_module

    monkeypatch.setenv("AURUM_SCENARIO_REQUESTS_ENABLED", "1")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    monkeypatch.setenv("AURUM_API_AUTH_DISABLED", "1")
    monkeypatch.delenv("AURUM_APP_DB_DSN", raising=False)

    store = scenario_service.InMemoryScenarioStore()
    monkeypatch.setattr(scenario_service, "STORE", store, raising=False)
    monkeypatch.setattr(worker_module, "ScenarioStore", store, raising=False)

    if "aurum.api.app" in sys.modules:
        app_module = importlib.reload(sys.modules["aurum.api.app"])
    else:
        app_module = importlib.import_module("aurum.api.app")
    monkeypatch.setattr(app_module, "ScenarioStore", store, raising=False)

    fake_redis = FakeRedis()
    monkeypatch.setattr(service, "_maybe_redis_client", lambda _cfg: fake_redis)

    original_invalidate = service.invalidate_scenario_outputs_cache
    invalidation_calls: list[tuple[object, str]] = []

    def tracking_invalidate(cache_cfg, tenant_id, scenario_id):
        invalidation_calls.append((tenant_id, scenario_id))
        return original_invalidate(cache_cfg, tenant_id, scenario_id)

    monkeypatch.setattr(service, "invalidate_scenario_outputs_cache", tracking_invalidate)

    saved_outputs: list[dict[str, object]] = []

    def fake_write_scenario_output(frame):
        saved_outputs.extend(frame.to_dict("records"))

    monkeypatch.setattr(worker_module, "write_scenario_output", fake_write_scenario_output)

    def fake_emit(scenario, run):
        assumptions_payload = []
        for assumption in scenario.assumptions:
            payload_raw = assumption.payload if isinstance(assumption.payload, dict) else {}
            assumptions_payload.append(
                {
                    "assumption_id": assumption.version or str(uuid.uuid4()),
                    "type": assumption.driver_type.value,
                    "payload": json.dumps(payload_raw),
                    "version": assumption.version,
                }
            )
        message = {
            "scenario_id": scenario.id,
            "tenant_id": scenario.tenant_id,
            "requested_by": "api",
            "asof_date": None,
            "curve_def_ids": [],
            "assumptions": assumptions_payload,
            "submitted_ts": int(time.time() * 1_000_000),
        }
        scenario_request = worker_module.ScenarioRequest.from_message(message)
        store.update_run_state(run.run_id, state="RUNNING", tenant_id=scenario.tenant_id)
        settings = worker_module.WorkerSettings.from_env()
        outputs = worker_module._build_outputs(scenario, scenario_request, run_id=run.run_id, settings=settings)
        worker_module._append_to_iceberg(outputs)
        store.update_run_state(run.run_id, state="SUCCEEDED", tenant_id=scenario.tenant_id)

    monkeypatch.setattr(scenario_service, "_maybe_emit_scenario_request", fake_emit, raising=False)

    client = TestClient(app_module.app)

    scenario_payload = {
        "tenant_id": "tenant-1",
        "name": "Pipeline smoke",
        "description": "integration",
        "assumptions": [
            {"driver_type": "policy", "payload": {"policy_name": "RPS"}},
        ],
    }

    resp = client.post("/v1/scenarios", json=scenario_payload)
    assert resp.status_code == 201
    scenario_id = resp.json()["data"]["scenario_id"]

    run_resp = client.post(f"/v1/scenarios/{scenario_id}/run", json={})
    assert run_resp.status_code == 202
    run_id = run_resp.json()["data"]["run_id"]
    assert run_id

    outputs_resp = client.get(
        f"/v1/scenarios/{scenario_id}/outputs",
        params={"tenant_id": "tenant-1", "limit": 10},
    )
    assert outputs_resp.status_code == 200
    payload_first = outputs_resp.json()
    assert payload_first["data"]
    first_value = payload_first["data"][0]["value"]
    assert saved_outputs
    assert fake_redis.store

    monkeypatch.setenv("AURUM_SCENARIO_BASE_VALUE", "65")
    second_run = client.post(f"/v1/scenarios/{scenario_id}/run", json={})
    assert second_run.status_code == 202

    assert any(call[1] == scenario_id for call in invalidation_calls)
    assert not fake_redis.store
    assert not fake_redis.sets

    second_outputs = client.get(
        f"/v1/scenarios/{scenario_id}/outputs",
        params={"tenant_id": "tenant-1", "limit": 10},
    )
    assert second_outputs.status_code == 200
    payload_second = second_outputs.json()
    assert payload_second["data"]
    second_value = payload_second["data"][0]["value"]
    assert second_value != first_value
