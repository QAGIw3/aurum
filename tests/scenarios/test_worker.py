from __future__ import annotations

import sys
import types
from datetime import date, datetime, timezone

import pytest


def _ensure_opentelemetry():
    if "opentelemetry" in sys.modules:
        return

    span = types.SimpleNamespace(set_attribute=lambda *args, **kwargs: None, is_recording=lambda: False)

    trace_module = types.ModuleType("opentelemetry.trace")
    trace_module.get_current_span = lambda: span
    trace_module.get_tracer_provider = lambda: None
    trace_module.get_tracer = lambda *_args, **_kwargs: None
    trace_module.set_tracer_provider = lambda *_args, **_kwargs: None

    propagate_module = types.ModuleType("opentelemetry.propagate")
    propagate_module.inject = lambda *_args, **_kwargs: None

    resources_module = types.ModuleType("opentelemetry.sdk.resources")

    class _Resource:
        @staticmethod
        def create(attrs):
            return attrs

    resources_module.Resource = _Resource

    sdk_trace_module = types.ModuleType("opentelemetry.sdk.trace")

    class _TracerProvider:
        def __init__(self, resource=None, sampler=None):
            self.resource = resource
            self.sampler = sampler

        def add_span_processor(self, _processor):
            return None

    sdk_trace_module.TracerProvider = _TracerProvider

    trace_export_module = types.ModuleType("opentelemetry.sdk.trace.export")

    class _BatchSpanProcessor:
        def __init__(self, _exporter):
            pass

    trace_export_module.BatchSpanProcessor = _BatchSpanProcessor

    sampling_module = types.ModuleType("opentelemetry.sdk.trace.sampling")

    class _TraceIdRatioBased:
        def __init__(self, _ratio):
            self.ratio = _ratio

    sampling_module.TraceIdRatioBased = _TraceIdRatioBased

    otlp_trace_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc.trace_exporter")

    class _OTLPSpanExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_trace_module.OTLPSpanExporter = _OTLPSpanExporter

    logs_module = types.ModuleType("opentelemetry._logs")
    logs_module.set_logger_provider = lambda *_args, **_kwargs: None

    sdk_logs_module = types.ModuleType("opentelemetry.sdk._logs")

    class _LoggerProvider:
        def __init__(self, **_kwargs):
            pass

        def add_log_record_processor(self, _processor):
            return None

    sdk_logs_module.LoggerProvider = _LoggerProvider

    sdk_logs_export_module = types.ModuleType("opentelemetry.sdk._logs.export")

    class _BatchLogRecordProcessor:
        def __init__(self, _exporter):
            pass

    sdk_logs_export_module.BatchLogRecordProcessor = _BatchLogRecordProcessor

    otlp_logs_module = types.ModuleType("opentelemetry.exporter.otlp.proto.grpc._log_exporter")

    class _OTLPLogExporter:
        def __init__(self, **_kwargs):
            pass

    otlp_logs_module.OTLPLogExporter = _OTLPLogExporter

    logging_instr_module = types.ModuleType("opentelemetry.instrumentation.logging")

    class _LoggingInstrumentor:
        def instrument(self, **_kwargs):
            return None

    logging_instr_module.LoggingInstrumentor = _LoggingInstrumentor

    requests_instr_module = types.ModuleType("opentelemetry.instrumentation.requests")

    class _RequestsInstrumentor:
        def instrument(self, **_kwargs):
            return None

    requests_instr_module.RequestsInstrumentor = _RequestsInstrumentor

    fastapi_instr_module = types.ModuleType("opentelemetry.instrumentation.fastapi")
    fastapi_instr_module.FastAPIInstrumentor = None

    psycopg_instr_module = types.ModuleType("opentelemetry.instrumentation.psycopg")
    psycopg_instr_module.PsycopgInstrumentation = None

    base_module = types.ModuleType("opentelemetry")
    base_module.trace = trace_module
    base_module.propagate = propagate_module

    sys.modules["opentelemetry"] = base_module
    sys.modules["opentelemetry.trace"] = trace_module
    sys.modules["opentelemetry.propagate"] = propagate_module
    sys.modules["opentelemetry.sdk.resources"] = resources_module
    sys.modules["opentelemetry.sdk.trace"] = sdk_trace_module
    sys.modules["opentelemetry.sdk.trace.export"] = trace_export_module
    sys.modules["opentelemetry.sdk.trace.sampling"] = sampling_module
    sys.modules["opentelemetry.exporter.otlp.proto.grpc.trace_exporter"] = otlp_trace_module
    sys.modules["opentelemetry._logs"] = logs_module
    sys.modules["opentelemetry.sdk._logs"] = sdk_logs_module
    sys.modules["opentelemetry.sdk._logs.export"] = sdk_logs_export_module
    sys.modules["opentelemetry.exporter.otlp.proto.grpc._log_exporter"] = otlp_logs_module
    sys.modules["opentelemetry.instrumentation.logging"] = logging_instr_module
    sys.modules["opentelemetry.instrumentation.requests"] = requests_instr_module
    sys.modules["opentelemetry.instrumentation.fastapi"] = fastapi_instr_module
    sys.modules["opentelemetry.instrumentation.psycopg"] = psycopg_instr_module


_ensure_opentelemetry()

pytest.importorskip("pydantic", reason="pydantic not installed")

from aurum.api.scenario_service import ScenarioRecord
from aurum.scenarios.models import DriverType, ScenarioAssumption
from aurum.scenarios import worker


def test_request_from_message_converts_days():
    payload = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-1",
        "asof_date": 20000,
        "curve_def_ids": ["curve-a", "curve-b"],
        "assumptions": [],
    }
    req = worker.ScenarioRequest.from_message(payload)
    assert req.scenario_id == "scn-1"
    assert req.tenant_id == "tenant-1"
    assert req.asof_date == date(2024, 10, 4)
    assert req.curve_def_ids == ["curve-a", "curve-b"]


def test_build_outputs_uses_assumptions(monkeypatch):
    scenario = ScenarioRecord(
        id="scn-1",
        tenant_id="tenant-1",
        name="Demo",
        description=None,
        assumptions=[
            ScenarioAssumption(driver_type=DriverType.POLICY, payload={"policy_name": "Clean"}),
            ScenarioAssumption(driver_type=DriverType.LOAD_GROWTH, payload={"annual_growth_pct": 2.5}),
        ],
        status="CREATED",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    request = worker.ScenarioRequest(
        scenario_id="scn-1",
        tenant_id="tenant-1",
        assumptions=[],
        asof_date=date(2025, 1, 1),
        curve_def_ids=None,
    )
    monkeypatch.setattr(worker, "_load_base_mid", lambda *args, **kwargs: 60.0)
    fixed_ts = datetime(2025, 1, 2, 12, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(worker, "_current_ts", lambda: fixed_ts)

    settings = worker.WorkerSettings(
        base_value=50.0,
        policy_weight=0.1,
        load_weight=0.05,
        tenor_multipliers={"MONTHLY": 1.0, "QUARTER": 1.1, "CALENDAR": 1.2},
    )

    outputs = worker._build_outputs(scenario, request, run_id="run-xyz", settings=settings)
    assert [record["tenor_type"] for record in outputs] == ["MONTHLY", "QUARTER", "CALENDAR"]

    monthly = outputs[0]
    assert monthly["scenario_id"] == "scn-1"
    assert monthly["tenant_id"] == "tenant-1"
    assert monthly["run_id"] == "run-xyz"
    assert monthly["tenor_label"] == "2025-01"
    assert monthly["value"] == pytest.approx(60.625, rel=1e-6)
    assert monthly["computed_ts"] == fixed_ts

    quarter = outputs[1]
    assert quarter["tenor_label"] == "Q1 2025"
    assert quarter["value"] == pytest.approx(66.6875, rel=1e-6)

    calendar = outputs[2]
    assert calendar["tenor_label"] == "Calendar 2025"
    assert calendar["value"] == pytest.approx(72.75, rel=1e-6)
    assert calendar["run_id"] == "run-xyz"

    for record in outputs:
        assert record["band_lower"] < record["value"] < record["band_upper"]
        assert record["_ingest_ts"] == fixed_ts

    components = {item["component"]: item["delta"] for item in monthly["attribution"]}
    assert pytest.approx(components["policy"], rel=1e-6) == 0.5
    assert pytest.approx(components["load_growth"], rel=1e-6) == 0.125


def test_to_avro_payload_converts_types():
    computed_ts = datetime.now(timezone.utc)
    record = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-1",
        "run_id": "run-abc",
        "asof_date": date(2025, 1, 1),
        "curve_key": "curve-a",
        "tenor_type": "MONTHLY",
        "contract_month": date(2025, 1, 1),
        "tenor_label": "2025-01",
        "metric": "mid",
        "value": 55.0,
        "band_lower": 50.0,
        "band_upper": 60.0,
        "attribution": [{"component": "policy", "delta": 0.1}],
        "version_hash": "abcd",
        "computed_ts": computed_ts,
    }
    payload = worker._to_avro_payload(record)
    assert payload["asof_date"] == worker._date_to_days(date(2025, 1, 1))
    assert payload["contract_month"] == worker._date_to_days(date(2025, 1, 1))
    assert isinstance(payload["computed_ts"], int)
    assert payload["attribution"][0]["component"] == "policy"
    assert payload["run_id"] == "run-abc"


def test_worker_settings_from_env(monkeypatch):
    monkeypatch.setenv("AURUM_SCENARIO_POLICY_WEIGHT", "0.2")
    monkeypatch.setenv("AURUM_SCENARIO_LOAD_WEIGHT", "0.1")
    monkeypatch.setenv("AURUM_SCENARIO_BASE_VALUE", "42")
    monkeypatch.setenv("AURUM_SCENARIO_TENOR_WEIGHTS", "MONTHLY:1.0,QUARTER:1.5,CALENDAR:2.0")

    settings = worker.WorkerSettings.from_env()
    assert settings.policy_weight == pytest.approx(0.2)
    assert settings.load_weight == pytest.approx(0.1)
    assert settings.base_value == pytest.approx(42.0)
    assert settings.tenor_multipliers["QUARTER"] == pytest.approx(1.5)
    assert settings.tenor_multipliers["CALENDAR"] == pytest.approx(2.0)


def test_build_outputs_extended_driver_effects(monkeypatch):
    scenario = ScenarioRecord(
        id="scn-2",
        tenant_id="tenant-9",
        name="Extended",
        description=None,
        assumptions=[
            ScenarioAssumption(
                driver_type=DriverType.LOAD_GROWTH,
                payload={"annual_growth_pct": 4.0, "start_year": 2024, "end_year": 2026},
            ),
            ScenarioAssumption(
                driver_type=DriverType.FUEL_CURVE,
                payload={
                    "fuel": "natural_gas",
                    "reference_series": "henry_hub",
                    "basis_points_adjustment": 100,
                },
            ),
            ScenarioAssumption(
                driver_type=DriverType.FLEET_CHANGE,
                payload={
                    "fleet_id": "fleet-1",
                    "change_type": "retire",
                    "effective_date": "2025-12-01",
                    "capacity_mw": 120,
                },
            ),
        ],
        status="CREATED",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    request = worker.ScenarioRequest(
        scenario_id="scn-2",
        tenant_id="tenant-9",
        assumptions=[],
        asof_date=date(2025, 6, 1),
        curve_def_ids=None,
    )

    monkeypatch.setattr(worker, "_load_base_mid", lambda *args, **kwargs: None)
    fixed_ts = datetime(2025, 6, 2, tzinfo=timezone.utc)
    monkeypatch.setattr(worker, "_current_ts", lambda: fixed_ts)

    settings = worker.WorkerSettings(
        base_value=45.0,
        policy_weight=0.1,
        load_weight=0.05,
        tenor_multipliers={"MONTHLY": 1.0},
    )

    outputs = worker._build_outputs(scenario, request, run_id="run-123", settings=settings)
    assert len(outputs) == 1
    record = outputs[0]
    assert record["value"] == pytest.approx(44.4263, rel=1e-6)

    attribution = {item["component"]: item["delta"] for item in record["attribution"]}
    assert attribution["load_growth"] == pytest.approx(0.1333, rel=1e-4)
    assert attribution["fuel_curve"] == pytest.approx(0.6430, rel=1e-4)
    assert attribution["fleet_change"] == pytest.approx(-1.35, rel=1e-6)


def test_driver_effects_use_reference_curves(monkeypatch):
    scenario = ScenarioRecord(
        id="scn-ref",
        tenant_id="tenant-ref",
        name="Ref Scenario",
        description=None,
        assumptions=[
            ScenarioAssumption(
                driver_type=DriverType.LOAD_GROWTH,
                payload={
                    "annual_growth_pct": 5.0,
                    "start_year": 2024,
                    "reference_curve_key": "curve-load",
                },
            ),
            ScenarioAssumption(
                driver_type=DriverType.FUEL_CURVE,
                payload={
                    "fuel": "natural_gas",
                    "reference_series": "curve-fuel",
                    "basis_points_adjustment": 0,
                },
            ),
        ],
        status="CREATED",
        created_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )

    request = worker.ScenarioRequest(
        scenario_id="scn-ref",
        tenant_id="tenant-ref",
        assumptions=[],
        asof_date=date(2025, 1, 1),
        curve_def_ids=["curve-scenario"],
    )

    def fake_load_base_mid(curve_key, *_args, **_kwargs):
        if curve_key == "curve-scenario":
            return 60.0
        if curve_key == "curve-load":
            return 80.0
        if curve_key == "curve-fuel":
            return 100.0
        return 50.0

    monkeypatch.setattr(worker, "_load_base_mid", fake_load_base_mid)
    monkeypatch.setattr(worker, "_reference_factor", lambda *_args, **_kwargs: 0.1)

    settings = worker.WorkerSettings(
        base_value=60.0,
        load_weight=0.05,
        tenor_multipliers={"MONTHLY": 1.0},
    )

    outputs = worker._build_outputs(scenario, request, settings=settings)
    value = outputs[0]["value"]
    assert value == pytest.approx(60.8333, rel=1e-4)
