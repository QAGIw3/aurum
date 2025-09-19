from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable, Optional

from aurum.api.scenario_service import STORE as ScenarioStore


@dataclass
class ScenarioRequest:
    scenario_id: str
    tenant_id: str
    assumptions: list[dict]
    asof_date: Optional[date] = None
    curve_def_ids: list[str] = None


def _now_ts_micros() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1_000_000)


def _load_avro_schema(path: str):  # pragma: no cover - integration path
    from confluent_kafka import avro  # type: ignore

    return avro.load(path)


def _producer(bootstrap: str, schema_registry_url: str):  # pragma: no cover - integration path
    from confluent_kafka.avro import AvroProducer  # type: ignore

    return AvroProducer({"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry_url})


def _consumer(bootstrap: str, group_id: str):  # pragma: no cover - integration path
    from confluent_kafka import Consumer  # type: ignore

    conf = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    return Consumer(conf)


def _schema_path(filename: str) -> str:
    base = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    return os.path.join(base, "kafka", "schemas", filename)


def run_worker():  # pragma: no cover - integration entrypoint
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS is required")
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    request_topic = os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1")
    output_topic = os.getenv("AURUM_SCENARIO_OUTPUT_TOPIC", "aurum.scenario.output.v1")
    group_id = os.getenv("AURUM_SCENARIO_WORKER_GROUP", "aurum-scenario-worker")

    cons = _consumer(bootstrap, group_id)
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
            try:
                req = json.loads(msg.value().decode("utf-8"))  # type: ignore[union-attr]
            except Exception:
                cons.commit(msg)  # skip bad message
                continue

            scenario_id = req.get("scenario_id")
            tenant_id = req.get("tenant_id")
            # If run_id is provided in headers, mark state transitions via API store
            headers = dict(msg.headers() or [])  # type: ignore[attr-defined]
            run_id = None
            if headers:
                val = headers.get("run_id") or headers.get(b"run_id")
                if isinstance(val, bytes):
                    run_id = val.decode("utf-8")
                elif isinstance(val, str):
                    run_id = val
            if run_id:
                try:
                    ScenarioStore.update_run_state(run_id, state="RUNNING")
                except Exception:
                    pass
            # mark run RUNNING
            # In this simple example, we cannot map run_id from request; API could augment request to include run_id
            # Process: simulate computation delay
            time.sleep(float(os.getenv("AURUM_SCENARIO_SIM_DELAY_SEC", "0.1")))

            # Emit a single dummy output record to the topic
            out = {
                "scenario_id": scenario_id,
                "tenant_id": tenant_id,
                "asof_date": int(date.today().strftime("%s")) // 86400,  # days since epoch
                "curve_key": "demo",
                "tenor_type": "MONTHLY",
                "contract_month": None,
                "tenor_label": "2025-01",
                "metric": "mid",
                "value": 0.0,
                "band_lower": None,
                "band_upper": None,
                "attribution": None,
                "version_hash": "dev",
                "computed_ts": _now_ts_micros(),
            }
            try:
                prod.produce(topic=output_topic, value=out, value_schema=value_schema)
                prod.flush(2)
            except Exception:
                pass

            if run_id:
                try:
                    ScenarioStore.update_run_state(run_id, state="SUCCEEDED")
                except Exception:
                    pass

            cons.commit(msg)
    finally:
        cons.close()
