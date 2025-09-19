#!/usr/bin/env python
"""Produce a single ISO LMP Avro record to Kafka using Schema Registry."""
from __future__ import annotations

import argparse
import io
import json
import os
import struct
from datetime import datetime, timezone
from pathlib import Path

from aurum.compat import requests

try:  # pragma: no cover - optional dep for CLI usage
    from fastavro import parse_schema, schemaless_writer
except Exception as exc:  # pragma: no cover
    raise SystemExit("fastavro is required: pip install fastavro") from exc


def load_schema() -> dict:
    schema_path = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"
    return json.loads(schema_path.read_text(encoding="utf-8"))


def fetch_schema_id(registry_url: str, subject: str) -> int:
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return int(resp.json()["id"])


def build_producer(bootstrap: str):
    from kafka import KafkaProducer  # lazy import

    return KafkaProducer(bootstrap_servers=bootstrap)


def build_record() -> dict[str, object]:
    now = datetime.now(timezone.utc)
    start = now.replace(minute=0, second=0, microsecond=0)
    return {
        "iso_code": "PJM",
        "market": "DAY_AHEAD",
        "delivery_date": (start.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
        "interval_start": int(start.timestamp() * 1_000_000),
        "interval_end": int((start.replace(hour=start.hour + 1)).timestamp() * 1_000_000),
        "interval_minutes": 60,
        "location_id": "TEST_NODE",
        "location_name": "Test Node",
        "location_type": "NODE",
        "price_total": 42.0,
        "price_energy": 40.0,
        "price_congestion": 1.0,
        "price_loss": 1.0,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": "RTO",
        "source_run_id": "test-run",
        "ingest_ts": int(now.timestamp() * 1_000_000),
        "record_hash": "pjm-test-node-42",
        "metadata": {"source": "produce_test_record"},
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Produce a test ISO LMP record to Kafka")
    p.add_argument("--topic", default="aurum.iso.pjm.lmp.v1")
    p.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    return p.parse_args()


def main() -> int:
    args = parse_args()
    subject = f"{args.topic}-value"

    schema_dict = load_schema()
    parsed_schema = parse_schema(schema_dict)
    schema_id = fetch_schema_id(args.registry, subject)
    producer = build_producer(args.bootstrap)

    record = build_record()
    buf = io.BytesIO()
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    schemaless_writer(buf, parsed_schema, record)
    producer.send(args.topic, value=buf.getvalue())
    producer.flush()
    print(f"Produced record to {args.topic} with schema id {schema_id}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


