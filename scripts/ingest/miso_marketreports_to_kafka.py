#!/usr/bin/env python
"""Ingest MISO Market Reports CSV (DA/RT) and publish Avro records to Kafka."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import struct
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator

from aurum.airflow_utils import metrics
from aurum.compat import requests

try:  # pragma: no cover - optional dependency
    from fastavro import parse_schema, schemaless_writer
except ImportError:  # pragma: no cover - handled by _require_fastavro
    parse_schema = schemaless_writer = None  # type: ignore[misc]


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "ingest.error.v1.avsc"


def _require_fastavro() -> None:
    if parse_schema is None or schemaless_writer is None:  # type: ignore[truthy-function]
        raise ModuleNotFoundError(
            "The 'fastavro' package is required for MISO ingestion. Install it via 'pip install fastavro'."
        )


def fetch_csv(url: str, *, max_retries: int = 5, backoff: float = 2.0) -> bytes:
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        resp = requests.get(url, timeout=60)
        if resp.status_code == 429:
            time.sleep(backoff * attempt)
            continue
        resp.raise_for_status()
        return resp.content
    raise RuntimeError("Exceeded retries fetching MISO report")


def iter_rows(csv_bytes: bytes) -> Iterator[dict[str, str]]:
    text = csv_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        norm = {k.strip(): (v.strip() if v is not None else "") for k, v in row.items()}
        yield norm


def _parse_dt(value: str) -> datetime:
    # Try ISO first, then common MISO patterns
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in ("%m/%d/%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    raise ValueError(f"Unrecognized datetime: {value}")


def _get(row: dict[str, str], *keys: str, default: str = "") -> str:
    for k in keys:
        if k in row and row[k] != "":
            return row[k]
    return default


def to_record(row: dict[str, str], *, market: str) -> dict[str, object]:
    # Typical columns: CPNode, CPNode ID, Name, Time, LMP, MCC, MLC
    ts_str = _get(row, "Time", "GMT Mkt Interval", "Local Time")
    start_dt = _parse_dt(ts_str)
    # Interval length guess: 5 minutes for RT, 60 for DA
    interval_minutes = 60 if market.upper().startswith("DA") or market.upper() == "DA" else 5
    end_dt = start_dt + timedelta(minutes=interval_minutes)

    lmp = float(_get(row, "LMP", "LMP ($/MWh)", default="0") or 0)
    mcc = float(_get(row, "MCC", "Congestion", default="0") or 0)
    mlc = float(_get(row, "MLC", "Loss", default="0") or 0)
    cpn = _get(row, "CPNode", "CPNode Name", "Name") or _get(row, "CPNode ID", default="")
    cpn_id = _get(row, "CPNode ID", default=cpn)

    record = {
        "iso_code": "MISO",
        "market": "DAY_AHEAD" if market.upper().startswith("DA") else "REAL_TIME",
        "delivery_date": (start_dt.date() - datetime(1970, 1, 1, tzinfo=timezone.utc).date()).days,
        "interval_start": int(start_dt.timestamp() * 1_000_000),
        "interval_end": int(end_dt.timestamp() * 1_000_000),
        "interval_minutes": interval_minutes,
        "location_id": cpn_id,
        "location_name": cpn or None,
        "location_type": "NODE",
        "price_total": lmp,
        "price_energy": lmp - mcc - mlc,
        "price_congestion": mcc,
        "price_loss": mlc,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": cpn or None,
        "source_run_id": _get(row, "Version", "SystemVersion", default=None) or None,
        "ingest_ts": int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000),
        "record_hash": hashlib.sha256(
            "|".join([ts_str, cpn_id, str(lmp)]).encode("utf-8")
        ).hexdigest(),
        "metadata": None,
    }
    return record


def load_schema() -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    parsed = parse_schema(schema)  # type: ignore[arg-type]
    return parsed, schema


def load_dlq_schema() -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema = json.loads(DLQ_SCHEMA_PATH.read_text(encoding="utf-8"))
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


def produce(producer, topic: str, parsed_schema: dict[str, object], schema_id: int, rows: Iterable[dict[str, str]], *, market: str) -> int:
    _require_fastavro()
    count = 0
    for row in rows:
        rec = to_record(row, market=market)
        rec["ingest_ts"] = int(datetime.utcnow().timestamp() * 1_000_000)
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        schemaless_writer(buf, parsed_schema, rec)
        producer.send(topic, value=buf.getvalue())
        count += 1
    producer.flush()
    return count


def publish_error(
    producer,
    topic: str | None,
    dlq_schema: dict[str, object] | None,
    schema_id: int | None,
    source: str,
    message: str,
    context: dict[str, object],
) -> None:
    if not topic or not dlq_schema or schema_id is None:
        return
    _require_fastavro()
    metrics.increment("ingest.dlq_events", tags={"source": source})
    record = {
        "source": source,
        "error_message": message,
        "context": {k: str(v) for k, v in context.items()},
        "ingest_ts": int(datetime.utcnow().timestamp() * 1_000_000),
    }
    buf = io.BytesIO()
    buf.write(b"\x00")
    buf.write(struct.pack(">I", schema_id))
    schemaless_writer(buf, dlq_schema, record)
    producer.send(topic, value=buf.getvalue())
    producer.flush()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest MISO market reports into Kafka")
    p.add_argument("--url", required=True, help="CSV URL for the report")
    p.add_argument("--market", required=True, help="DA or RT")
    p.add_argument("--topic", default="aurum.iso.miso.lmp.v1")
    p.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--dlq-topic", default=os.getenv("AURUM_DLQ_TOPIC"))
    p.add_argument("--dlq-subject", default="aurum.ingest.error.v1")
    p.add_argument("--max-retries", type=int, default=5)
    p.add_argument("--backoff-seconds", type=float, default=2.0)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    parsed_schema, _ = load_schema()
    subject = f"{args.topic}-value"
    schema_id = fetch_schema_id(args.schema_registry, subject)
    producer = build_producer(args.bootstrap_servers)
    dlq_schema = dlq_schema_id = None
    if args.dlq_topic:
        dlq_schema, _ = load_dlq_schema()
        dlq_schema_id = fetch_schema_id(args.schema_registry, args.dlq_subject)

    try:
        csv_bytes = fetch_csv(args.url, max_retries=args.max_retries, backoff=args.backoff_seconds)
    except Exception as exc:
        publish_error(producer, args.dlq_topic, dlq_schema, dlq_schema_id, "miso_marketreports_to_kafka", str(exc), {"url": args.url})
        print(f"Failed to fetch MISO report: {exc}")
        return 1

    rows = list(iter_rows(csv_bytes))
    if not rows:
        publish_error(producer, args.dlq_topic, dlq_schema, dlq_schema_id, "miso_marketreports_to_kafka", "No rows parsed", {"url": args.url})
        print("No MISO rows parsed; exiting")
        return 0

    count = produce(producer, args.topic, parsed_schema, schema_id, rows, market=args.market)
    print(f"Published {count} MISO LMP records to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
