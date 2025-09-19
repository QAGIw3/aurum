#!/usr/bin/env python
"""Ingest ISO-NE web services LMP and publish Avro to Kafka."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from aurum.compat import requests

try:  # pragma: no cover - optional dependency
    from fastavro import parse_schema, schemaless_writer
except ImportError:  # pragma: no cover - handled by _require_fastavro
    parse_schema = schemaless_writer = None  # type: ignore[misc]


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"


def _require_fastavro() -> None:
    if parse_schema is None or schemaless_writer is None:  # type: ignore[truthy-function]
        raise ModuleNotFoundError(
            "The 'fastavro' package is required for ISO-NE ingestion. Install it via 'pip install fastavro'."
        )


def fetch_json(url: str, params: dict[str, str], auth: tuple[str, str] | None) -> dict:
    resp = requests.get(url, params=params, auth=auth, timeout=60)
    resp.raise_for_status()
    return resp.json()


def to_record(item: dict[str, object], *, market: str) -> dict[str, object]:
    start = datetime.fromisoformat(str(item["begin"]).replace("Z", "+00:00"))
    end = datetime.fromisoformat(str(item["end"]).replace("Z", "+00:00"))
    lmp = float(item.get("lmp", 0))
    energy = float(item.get("energy", 0)) if item.get("energy") is not None else None
    cong = float(item.get("congestion", 0)) if item.get("congestion") is not None else None
    loss = float(item.get("loss", 0)) if item.get("loss") is not None else None
    ptid = str(item.get("ptid") or item.get("locationId") or "")
    name = item.get("name") or None
    ltype = str(item.get("locationType") or "NODE").upper()
    return {
        "iso_code": "ISONE",
        "market": "DAY_AHEAD" if market.upper().startswith("DA") else "REAL_TIME",
        "delivery_date": (start.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
        "interval_start": int(start.timestamp() * 1_000_000),
        "interval_end": int(end.timestamp() * 1_000_000),
        "interval_minutes": int((end - start).total_seconds() // 60),
        "location_id": ptid,
        "location_name": name,
        "location_type": ltype,
        "price_total": lmp,
        "price_energy": energy,
        "price_congestion": cong,
        "price_loss": loss,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": name,
        "source_run_id": None,
        "ingest_ts": int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000),
        "record_hash": hashlib.sha256(
            "|".join([str(start), ptid, str(lmp)]).encode("utf-8")
        ).hexdigest(),
        "metadata": None,
    }


def load_schema() -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    parsed = parse_schema(schema)  # type: ignore[arg-type]
    return parsed, schema


def fetch_schema_id(registry_url: str, subject: str) -> int:
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return int(resp.json()["id"])


def build_producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=bootstrap)


def produce(producer, topic: str, parsed_schema: dict[str, object], schema_id: int, items: Iterable[dict[str, object]], *, market: str) -> int:
    _require_fastavro()
    count = 0
    for item in items:
        rec = to_record(item, market=market)
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        schemaless_writer(buf, parsed_schema, rec)
        producer.send(topic, value=buf.getvalue())
        count += 1
    producer.flush()
    return count


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest ISO-NE web services LMP into Kafka")
    p.add_argument("--url", required=True, help="ISO-NE endpoint returning JSON")
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    p.add_argument("--market", required=True)
    p.add_argument("--topic", default="aurum.iso.isone.lmp.v1")
    p.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--username")
    p.add_argument("--password")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    params = {"start": args.start, "end": args.end, "market": args.market}
    auth = (args.username, args.password) if args.username and args.password else None
    payload = fetch_json(args.url, params, auth)
    # Expect an array under 'items' or payload directly iterable
    items = payload.get("items") if isinstance(payload, dict) else payload
    if not items:
        print("No ISO-NE items; exiting")
        return 0
    parsed_schema, _ = load_schema()
    subject = f"{args.topic}-value"
    schema_id = fetch_schema_id(args.schema_registry, subject)
    producer = build_producer(args.bootstrap_servers)
    count = produce(producer, args.topic, parsed_schema, schema_id, items, market=args.market)
    print(f"Published {count} ISO-NE LMP records to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
