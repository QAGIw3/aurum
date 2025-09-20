#!/usr/bin/env python3
"""Emit a single scenario request message for smoke validation."""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path

try:
    from confluent_kafka import avro  # type: ignore
    from confluent_kafka.avro import AvroProducer  # type: ignore
except ModuleNotFoundError as exc:  # pragma: no cover - optional dep
    raise SystemExit("confluent-kafka[avro] is required to run the scenario smoke producer") from exc


SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "kafka" / "schemas"


def _load_schema(name: str):
    return avro.load(str(SCHEMA_ROOT / name))


def _days_since_epoch(value: date) -> int:
    return (value - date(1970, 1, 1)).days


def _micros(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp() * 1_000_000)


def _build_record(
    *,
    scenario_id: str,
    tenant_id: str,
    requested_by: str,
    curve_def_ids: list[str] | None,
    assumptions: list[dict[str, str]] | None,
    asof: date | None,
) -> dict[str, object]:
    return {
        "scenario_id": scenario_id,
        "tenant_id": tenant_id,
        "requested_by": requested_by,
        "asof_date": _days_since_epoch(asof) if asof else None,
        "curve_def_ids": curve_def_ids or [],
        "assumptions": assumptions or [],
        "submitted_ts": _micros(datetime.now(timezone.utc)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Emit a scenario request to Kafka for smoke testing")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    parser.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    parser.add_argument("--topic", default=os.getenv("AURUM_SCENARIO_REQUEST_TOPIC", "aurum.scenario.request.v1"))
    parser.add_argument("--scenario-id", default=None, help="Scenario identifier (defaults to generated UUID)")
    parser.add_argument("--tenant-id", default="00000000-0000-0000-0000-000000000001")
    parser.add_argument("--requested-by", default="smoke")
    parser.add_argument("--curve-def", action="append", dest="curve_defs", help="Curve definition IDs to include")
    parser.add_argument(
        "--policy-name",
        default="Policy Smoke",
        help="Policy name to encode in the first assumption payload",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the record without producing to Kafka",
    )
    args = parser.parse_args()

    scenario_id = args.scenario_id or f"smoke-{int(time.time())}"
    assumption_payload = json.dumps(
        {
            "policy_name": args.policy_name,
            "start_year": datetime.now().year + 1,
        }
    )
    assumptions = [
        {
            "assumption_id": f"asm-{scenario_id}",
            "type": "policy",
            "payload": assumption_payload,
            "version": "v1",
        }
    ]

    record = _build_record(
        scenario_id=scenario_id,
        tenant_id=args.tenant_id,
        requested_by=args.requested_by,
        curve_def_ids=args.curve_defs,
        assumptions=assumptions,
        asof=date.today(),
    )

    if args.dry_run:
        print(json.dumps(record, indent=2))
        return 0

    value_schema = _load_schema("scenario.request.v1.avsc")
    producer = AvroProducer(
        {
            "bootstrap.servers": args.bootstrap,
            "schema.registry.url": args.schema_registry,
        },
        default_value_schema=value_schema,
    )

    producer.produce(topic=args.topic, value=record)
    producer.flush(5)
    print(f"Produced scenario request for scenario_id={scenario_id} to topic={args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
