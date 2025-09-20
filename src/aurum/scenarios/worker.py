from __future__ import annotations

import hashlib
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Iterable, Optional, Sequence

import pandas as pd

from aurum.api.scenario_service import STORE as ScenarioStore
from aurum.parsers.iceberg_writer import write_scenario_output
from aurum.scenarios.models import DriverType

LOGGER = logging.getLogger(__name__)
EPOCH = date(1970, 1, 1)
_EPOCH_OFFSET_DAYS = 3


@dataclass
class ScenarioRequest:
    scenario_id: str
    tenant_id: str
    assumptions: list[dict]
    asof_date: Optional[date] = None
    curve_def_ids: list[str] | None = field(default=None)

    @classmethod
    def from_message(cls, payload: dict) -> "ScenarioRequest":
        scenario_id = str(payload.get("scenario_id") or "")
        tenant_id = str(payload.get("tenant_id") or "")
        assumptions = payload.get("assumptions") or []
        curve_defs_raw = payload.get("curve_def_ids")
        curve_def_ids: list[str] | None
        if isinstance(curve_defs_raw, Sequence) and not isinstance(curve_defs_raw, (str, bytes)):
            curve_def_ids = [str(item) for item in curve_defs_raw]
        elif curve_defs_raw:
            curve_def_ids = [str(curve_defs_raw)]
        else:
            curve_def_ids = None

        asof_raw = payload.get("asof_date")
        asof_date: Optional[date] = None
        if isinstance(asof_raw, date):
            asof_date = asof_raw
        elif isinstance(asof_raw, int):
            asof_date = EPOCH + timedelta(days=asof_raw + _EPOCH_OFFSET_DAYS)
        elif isinstance(asof_raw, str) and asof_raw:
            try:
                asof_date = date.fromisoformat(asof_raw)
            except ValueError:
                asof_date = None

        return cls(
            scenario_id=scenario_id,
            tenant_id=tenant_id,
            assumptions=assumptions,
            asof_date=asof_date,
            curve_def_ids=curve_def_ids,
        )


def _load_avro_schema(path: str):  # pragma: no cover - integration path
    from confluent_kafka import avro  # type: ignore

    return avro.load(path)


def _producer(bootstrap: str, schema_registry_url: str):  # pragma: no cover - integration path
    from confluent_kafka.avro import AvroProducer  # type: ignore

    return AvroProducer({"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url})


def _consumer(bootstrap: str, group_id: str, schema_registry_url: str):  # pragma: no cover - integration path
    """Create an AvroConsumer to decode records using Schema Registry."""
    try:
        from confluent_kafka.avro import AvroConsumer  # type: ignore
    except Exception as exc:  # pragma: no cover - import failure at runtime only
        raise RuntimeError("AvroConsumer not available - install confluent-kafka[avro]") from exc

    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "schema.registry.url": schema_registry_url,
    }
    return AvroConsumer(conf)


def _schema_path(filename: str) -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(base, "kafka", "schemas", filename)


def _date_to_days(value: date) -> int:
    return (value - EPOCH).days - _EPOCH_OFFSET_DAYS


def _timestamp_micros(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def _resolve_curve_key(request: ScenarioRequest, scenario_id: str, tenant_id: str) -> str:
    if request.curve_def_ids:
        return request.curve_def_ids[0]
    seed = f"{tenant_id}:{scenario_id}:{request.asof_date or ''}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]


def _compute_value(scenario, request: ScenarioRequest) -> float:
    base = 50.0
    policy_terms = []
    load_terms = []
    for assumption in getattr(scenario, "assumptions", []) or []:
        payload = getattr(assumption, "payload", {}) or {}
        if getattr(assumption, "driver_type", None) == DriverType.POLICY:
            policy_terms.append(len(str(payload.get("policy_name", ""))) * 0.1)
        if getattr(assumption, "driver_type", None) == DriverType.LOAD_GROWTH:
            try:
                load_terms.append(float(payload.get("annual_growth_pct", 0.0)) * 0.05)
            except (TypeError, ValueError):
                continue
    uplift = sum(policy_terms) + sum(load_terms)
    return round(base + uplift, 4)


def _build_attribution(scenario) -> list[dict[str, float]]:
    attribution: list[dict[str, float]] = []
    for idx, assumption in enumerate(getattr(scenario, "assumptions", []) or [], start=1):
        driver = getattr(assumption, "driver_type", None)
        name = driver.value if isinstance(driver, DriverType) else str(driver)
        attribution.append({"component": name, "delta": round(0.05 * idx, 4)})
    return attribution


def _build_outputs(scenario, request: ScenarioRequest) -> list[dict[str, object]]:
    asof = request.asof_date or date.today()
    contract_month = asof.replace(day=1)
    computed_ts = datetime.now(timezone.utc)
    curve_key = _resolve_curve_key(request, scenario.id, scenario.tenant_id)
    value = _compute_value(scenario, request)
    band_lower = round(value * 0.98, 4)
    band_upper = round(value * 1.02, 4)
    attribution = _build_attribution(scenario)
    version_seed = f"{scenario.id}:{curve_key}:{value:.4f}:{asof.isoformat()}"
    version_hash = hashlib.sha256(version_seed.encode("utf-8")).hexdigest()[:16]
    record = {
        "asof_date": asof,
        "scenario_id": scenario.id,
        "tenant_id": scenario.tenant_id,
        "curve_key": curve_key,
        "tenor_type": "MONTHLY",
        "contract_month": contract_month,
        "tenor_label": contract_month.strftime("%Y-%m"),
        "metric": "mid",
        "value": value,
        "band_lower": band_lower,
        "band_upper": band_upper,
        "attribution": attribution or None,
        "version_hash": version_hash,
        "computed_ts": computed_ts,
        "_ingest_ts": computed_ts,
    }
    return [record]


def _append_to_iceberg(records: Iterable[dict[str, object]]) -> None:
    payload = list(records)
    if not payload:
        return
    frame = pd.DataFrame(payload)
    try:
        write_scenario_output(frame)
    except Exception as exc:  # pragma: no cover - integration error path
        LOGGER.warning("Failed to append scenario outputs to Iceberg: %s", exc)


def _to_avro_payload(record: dict[str, object]) -> dict[str, object]:
    asof_date = record["asof_date"]
    contract_month = record.get("contract_month")
    computed_ts = record["computed_ts"]
    attribution = record.get("attribution")
    if isinstance(attribution, list):
        avro_attr = [
            {"component": str(item.get("component")), "delta": float(item.get("delta", 0.0))}
            for item in attribution
        ]
    else:
        avro_attr = None
    return {
        "scenario_id": record["scenario_id"],
        "tenant_id": record["tenant_id"],
        "asof_date": _date_to_days(asof_date),
        "curve_key": record["curve_key"],
        "tenor_type": record["tenor_type"],
        "contract_month": _date_to_days(contract_month) if isinstance(contract_month, date) else None,
        "tenor_label": record["tenor_label"],
        "metric": record["metric"],
        "value": float(record["value"]),
        "band_lower": float(record["band_lower"]) if record.get("band_lower") is not None else None,
        "band_upper": float(record["band_upper"]) if record.get("band_upper") is not None else None,
        "attribution": avro_attr,
        "version_hash": record["version_hash"],
        "computed_ts": _timestamp_micros(computed_ts),
    }


def _maybe_update_run_state(run_id: Optional[str], tenant_id: Optional[str], state: str) -> None:
    if not run_id or not tenant_id:
        return
    try:  # pragma: no cover - integration path
        ScenarioStore.update_run_state(run_id, state=state, tenant_id=tenant_id)
    except Exception as exc:
        LOGGER.debug("Failed to update run %s to %s: %s", run_id, state, exc)


def run_worker():  # pragma: no cover - integration entrypoint
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is required")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    request_topic = os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")
    output_topic = os.getenv("AURUM_SCENARIO_OUTPUT_TOPIC", "aurum.scenario.output.v1")
    group_id = os.getenv("AURUM_SCENARIO_WORKER_GROUP", "aurum-scenario-worker")

    cons = _consumer(bootstrap, group_id, schema_registry_url)
    prod = _producer(bootstrap, schema_registry_url)
    value_schema = _load_avro_schema(_schema_path("scenario.output.v1.avsc"))

    cons.subscribe([request_topic])

    try:
        while True:
            msg = cons.poll(1.0)
            if msg is None:
                continue
            if msg.error():  # type: ignore[attr-defined]
                continue
            payload = msg.value()  # type: ignore[assignment]
            if not isinstance(payload, dict):
                cons.commit(msg)
                continue

            scenario_req = ScenarioRequest.from_message(payload)
            if not scenario_req.scenario_id or not scenario_req.tenant_id:
                LOGGER.warning("Scenario request missing identifiers: %s", payload)
                cons.commit(msg)
                continue

            headers = dict(msg.headers() or [])  # type: ignore[attr-defined]
            run_id: Optional[str] = None
            if headers:
                val = headers.get("run_id") or headers.get(b"run_id")
                if isinstance(val, bytes):
                    run_id = val.decode("utf-8")
                elif isinstance(val, str):
                    run_id = val

            _maybe_update_run_state(run_id, scenario_req.tenant_id, "RUNNING")

            delay = float(os.getenv("AURUM_SCENARIO_SIM_DELAY_SEC", "0.1"))
            if delay:
                time.sleep(delay)

            try:
                scenario = ScenarioStore.get_scenario(
                    scenario_req.scenario_id,
                    tenant_id=scenario_req.tenant_id,
                )
                if scenario is None:
                    raise RuntimeError(
                        f"Scenario {scenario_req.scenario_id} not found for tenant {scenario_req.tenant_id}"
                    )

                outputs = _build_outputs(scenario, scenario_req)
                for output in outputs:
                    avro_payload = _to_avro_payload(output)
                    prod.produce(topic=output_topic, value=avro_payload, value_schema=value_schema)
                prod.flush(2)

                _append_to_iceberg(outputs)
                _maybe_update_run_state(run_id, scenario_req.tenant_id, "SUCCEEDED")
            except Exception as exc:  # pragma: no cover - integration failure path
                LOGGER.error("Scenario worker failed for scenario %s: %s", scenario_req.scenario_id, exc)
                _maybe_update_run_state(run_id, scenario_req.tenant_id, "FAILED")
            finally:
                cons.commit(msg)
    finally:
        cons.close()
