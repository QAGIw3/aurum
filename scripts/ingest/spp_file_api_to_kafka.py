#!/usr/bin/env python
"""Ingest SPP Marketplace LMP reports and publish Avro records to Kafka."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import struct
from datetime import datetime, timedelta, timezone
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
            "The 'fastavro' package is required for SPP ingestion. Install it via 'pip install fastavro'."
        )


def list_files(
    base_url: str,
    report: str,
    date_str: str,
    token: str | None,
    *,
    timeout_seconds: float = 60.0,
) -> list[str]:
    url = f"{base_url.rstrip('/')}/v1/files/{report}?startDate={date_str}&endDate={date_str}"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(url, headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    payload = resp.json()
    files = payload.get("files") if isinstance(payload, dict) else payload
    return [f["url"] for f in files or []]


def download_csv(url: str, token: str | None, *, timeout_seconds: float = 60.0) -> bytes:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    resp = requests.get(url, headers=headers, timeout=timeout_seconds)
    resp.raise_for_status()
    return resp.content


def iter_rows(csv_bytes: bytes) -> Iterable[dict[str, str]]:
    text = csv_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield {k.strip(): (v.strip() if v is not None else "") for k, v in row.items()}


def to_record(row: dict[str, str], *, iso: str, interval_minutes: int) -> dict[str, object]:
    stamp = row.get("Interval") or row.get("StartTime")
    start_dt = datetime.fromisoformat(stamp.replace("Z", "+00:00")) if stamp else datetime.utcnow()
    end_dt = start_dt + timedelta(minutes=interval_minutes)
    lmp = float(row.get("LMP", "0") or 0)
    energy = float(row.get("Energy", "0") or 0)
    congestion = float(row.get("Congestion", "0") or 0)
    loss = float(row.get("Loss", "0") or 0)
    loc = row.get("Settlement Location") or row.get("Bus Name") or row.get("Location") or ""

    return {
        "iso_code": iso,
        "market": row.get("Market") or "REAL_TIME",
        "delivery_date": (start_dt.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
        "interval_start": int(start_dt.timestamp() * 1_000_000),
        "interval_end": int(end_dt.timestamp() * 1_000_000),
        "interval_minutes": interval_minutes,
        "location_id": row.get("Settlement Location Id") or loc,
        "location_name": loc or None,
        "location_type": (row.get("Location Type") or "NODE").upper(),
        "price_total": lmp,
        "price_energy": energy,
        "price_congestion": congestion,
        "price_loss": loss,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": loc or None,
        "source_run_id": row.get("Version") or None,
        "ingest_ts": int(datetime.utcnow().timestamp() * 1_000_000),
        "record_hash": hashlib.sha256(
            "|".join([stamp or "", loc, str(lmp)]).encode("utf-8")
        ).hexdigest(),
        "metadata": None,
    }


def load_schema() -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    parsed = parse_schema(schema)  # type: ignore[arg-type]
    return parsed, schema


def _write_json_output(records: list[dict[str, object]], output_path: str) -> None:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(records, indent=2), encoding="utf-8")


def fetch_schema_id(registry_url: str, subject: str) -> int:
    url = f"{registry_url.rstrip('/')}/subjects/{subject}/versions/latest"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return int(resp.json()["id"])


def build_producer(bootstrap: str):
    from kafka import KafkaProducer

    return KafkaProducer(bootstrap_servers=bootstrap)


def produce(
    producer,
    topic: str,
    parsed_schema: dict[str, object],
    schema_id: int,
    records: Iterable[dict[str, object]],
) -> int:
    _require_fastavro()
    count = 0
    for rec in records:
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        schemaless_writer(buf, parsed_schema, rec)
        producer.send(topic, value=buf.getvalue())
        count += 1
    producer.flush()
    return count


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest SPP file API report into Kafka")
    p.add_argument("--base-url", required=True, help="SPP file API base url")
    p.add_argument("--report", required=True, help="Report name, e.g. RTBM-LMPBYLT")
    p.add_argument("--date", required=True, help="ISO date YYYY-MM-DD")
    p.add_argument("--interval-minutes", type=int, default=5)
    p.add_argument("--topic", default="aurum.iso.spp.lmp.v1")
    p.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--token")
    p.add_argument(
        "--http-timeout",
        type=float,
        default=60.0,
        help="Timeout (seconds) applied to SPP API HTTP requests",
    )
    p.add_argument(
        "--output-json",
        help="Optional path to write normalized SPP records as a JSON array",
    )
    p.add_argument(
        "--no-kafka",
        action="store_true",
        help="Skip producing to Kafka (useful when staging for SeaTunnel)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    files = list_files(
        args.base_url,
        args.report,
        args.date,
        args.token,
        timeout_seconds=args.http_timeout,
    )
    if not files:
        print("No SPP files listed; exiting")
        if args.output_json:
            _write_json_output([], args.output_json)
        return 0

    subject = f"{args.topic}-value"
    producer = None
    parsed_schema = None
    schema_id: int | None = None

    if not args.no_kafka:
        parsed_schema, _ = load_schema()
        schema_id = fetch_schema_id(args.schema_registry, subject)
        producer = build_producer(args.bootstrap_servers)

    all_records: list[dict[str, object]] = []
    for file_url in files:
        csv_bytes = download_csv(file_url, args.token, timeout_seconds=args.http_timeout)
        for row in iter_rows(csv_bytes):
            all_records.append(to_record(row, iso="SPP", interval_minutes=args.interval_minutes))

    if args.output_json:
        _write_json_output(all_records, args.output_json)

    if not all_records:
        print("No SPP rows parsed; exiting")
        return 0

    if args.no_kafka:
        target = args.output_json or ""
        print(f"Staged {len(all_records)} SPP LMP records to {target}")
        return 0

    if parsed_schema is None or schema_id is None or producer is None:
        raise RuntimeError("Kafka producer not initialised")

    total = produce(producer, args.topic, parsed_schema, schema_id, all_records)
    print(f"Published {total} SPP LMP records to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
