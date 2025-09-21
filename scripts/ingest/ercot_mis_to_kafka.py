#!/usr/bin/env python
"""Ingest ERCOT MIS LMP CSV (zip) and publish Avro records to Kafka."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import struct
import time
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from aurum.airflow_utils import metrics
from aurum.compat import requests

try:  # pragma: no cover - optional runtime dependency
    from fastavro import parse_schema, schemaless_writer
except ImportError:  # pragma: no cover - handled by _require_fastavro
    parse_schema = schemaless_writer = None  # type: ignore[misc]


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "ingest.error.v1.avsc"


def _require_fastavro() -> None:
    if parse_schema is None or schemaless_writer is None:  # type: ignore[truthy-function]
        raise ModuleNotFoundError(
            "The 'fastavro' package is required for ERCOT ingestion. Install it via 'pip install fastavro'."
        )


def _to_timestamp(date_str: str, hour: int, interval: int) -> datetime:
    date = datetime.strptime(date_str, "%Y-%m-%d")
    start_hour = hour - 1
    minutes = (interval - 1) * 15
    return date + timedelta(hours=start_hour, minutes=minutes)


def fetch_zip(
    url: str,
    params: dict[str, str],
    headers: dict[str, str],
    max_retries: int = 5,
    backoff: float = 2.0,
    timeout_seconds: float = 60.0,
) -> bytes:
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        resp = requests.get(url, params=params, headers=headers, timeout=timeout_seconds)
        if resp.status_code == 429:
            time.sleep(backoff * attempt)
            continue
        resp.raise_for_status()
        return resp.content
    raise RuntimeError("Exceeded retries fetching ERCOT MIS zip")


def extract_csv_bytes(data: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".csv"):
                return zf.read(name)
    raise ValueError("Zip archive does not contain a CSV file")


def iter_rows(csv_bytes: bytes) -> Iterable[dict[str, str]]:
    text = csv_bytes.decode("utf-8", errors="ignore")
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        yield {key.strip(): (value.strip() if value is not None else "") for key, value in row.items()}


def to_record(row: dict[str, str]) -> dict[str, object]:
    start_dt = _to_timestamp(row["DeliveryDate"], int(row["DeliveryHour"]), int(row["DeliveryInterval"]))
    end_dt = start_dt + timedelta(minutes=15)
    price = float(row.get("SettlementPointPrice", "0") or 0)
    record = {
        "iso_code": "ERCOT",
        "market": row.get("MarketType") or "RTM",
        "delivery_date": (start_dt.date() - datetime(1970, 1, 1).date()).days,
        "interval_start": int(start_dt.timestamp() * 1_000_000),
        "interval_end": int(end_dt.timestamp() * 1_000_000),
        "interval_minutes": 15,
        "location_id": row.get("SettlementPoint") or "",
        "location_name": row.get("SettlementPoint") or None,
        "location_type": "NODE",
        "price_total": price,
        "price_energy": float(row.get("SettlementPointLMP", "0") or 0),
        "price_congestion": float(row.get("Congestion", "0") or 0),
        "price_loss": float(row.get("Loss", "0") or 0),
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": row.get("SettlementPoint") or None,
        "source_run_id": row.get("SystemVersion") or None,
        "ingest_ts": int(datetime.utcnow().timestamp() * 1_000_000),
        "record_hash": hashlib.sha256(
            "|".join(
                [
                    row.get("DeliveryDate", ""),
                    row.get("DeliveryHour", ""),
                    row.get("DeliveryInterval", ""),
                    row.get("SettlementPoint", ""),
                    str(price),
                ]
            ).encode("utf-8")
        ).hexdigest(),
        "metadata": None,
    }
    return record


def load_schema(path: Path) -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema_dict = json.loads(path.read_text(encoding="utf-8"))
    parsed = parse_schema(schema_dict)  # type: ignore[arg-type]
    return parsed, schema_dict


def load_dlq_schema() -> tuple[dict[str, object], dict[str, object]]:
    _require_fastavro()
    schema_dict = json.loads(DLQ_SCHEMA_PATH.read_text(encoding="utf-8"))
    parsed = parse_schema(schema_dict)  # type: ignore[arg-type]
    return parsed, schema_dict


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


def produce_records(
    producer,
    topic: str,
    parsed_schema: dict[str, object],
    schema_id: int,
    records: Iterable[dict[str, object]],
) -> int:
    _require_fastavro()
    count = 0
    for record in records:
        record["ingest_ts"] = int(datetime.utcnow().timestamp() * 1_000_000)
        buffer = io.BytesIO()
        buffer.write(b"\x00")
        buffer.write(struct.pack(">I", schema_id))
        schemaless_writer(buffer, parsed_schema, record)
        producer.send(topic, value=buffer.getvalue())
        count += 1
    producer.flush()
    return count


def publish_error(producer, topic: str | None, dlq_schema: dict[str, object] | None, schema_id: int | None, source: str, message: str, context: dict[str, object]) -> None:
    if producer is None or not topic or not dlq_schema or schema_id is None:
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
    parser = argparse.ArgumentParser(description="Ingest ERCOT MIS LMP data into Kafka")
    parser.add_argument("--url", required=True, help="ERCOT MIS URL for the CSV zip payload")
    parser.add_argument("--topic", default="aurum.iso.ercot.lmp.v1")
    parser.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument("--subject", help="Override schema subject (default <topic>-value)")
    parser.add_argument("--bearer-token", help="Bearer token for ERCOT MIS (if required)")
    parser.add_argument("--dlq-topic", default=os.environ.get("AURUM_DLQ_TOPIC"))
    parser.add_argument("--dlq-subject", default="aurum.ingest.error.v1")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-seconds", type=float, default=2.0)
    parser.add_argument(
        "--http-timeout",
        type=float,
        default=60.0,
        help="Timeout (seconds) applied to ERCOT MIS HTTP requests",
    )
    parser.add_argument(
        "--output-json",
        help="Optional path to write normalized ERCOT records as a JSON array",
    )
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Skip producing to Kafka (useful when staging for SeaTunnel)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {}
    if args.bearer_token:
        headers["Authorization"] = f"Bearer {args.bearer_token}"

    subject = args.subject or f"{args.topic}-value"

    producer = None
    parsed_schema = None
    schema_id: int | None = None
    dlq_schema = None
    dlq_schema_id: int | None = None

    if not args.no_kafka:
        parsed_schema, _ = load_schema(SCHEMA_PATH)
        schema_id = fetch_schema_id(args.schema_registry, subject)
        producer = build_producer(args.bootstrap_servers)
        if args.dlq_topic:
            dlq_schema, _ = load_dlq_schema()
            dlq_schema_id = fetch_schema_id(args.schema_registry, args.dlq_subject)

    try:
        zip_bytes = fetch_zip(
            args.url,
            params={},
            headers=headers,
            max_retries=args.max_retries,
            backoff=args.backoff_seconds,
            timeout_seconds=args.http_timeout,
        )
        csv_bytes = extract_csv_bytes(zip_bytes)
    except Exception as exc:
        publish_error(producer, args.dlq_topic, dlq_schema, dlq_schema_id, "ercot_mis_to_kafka", str(exc), {"url": args.url})
        print(f"Failed to fetch ERCOT MIS: {exc}")
        return 1

    rows = list(iter_rows(csv_bytes))
    records = [to_record(row) for row in rows]

    if args.output_json:
        _write_json_output(records, args.output_json)

    if not records:
        publish_error(producer, args.dlq_topic, dlq_schema, dlq_schema_id, "ercot_mis_to_kafka", "No rows parsed", {"url": args.url})
        print("No ERCOT rows parsed; exiting")
        return 0

    if args.no_kafka:
        target = args.output_json or ""
        print(f"Staged {len(records)} ERCOT LMP records to {target}")
        return 0

    if parsed_schema is None or schema_id is None or producer is None:
        raise RuntimeError("Kafka producer not initialised")

    count = produce_records(producer, args.topic, parsed_schema, schema_id, records)
    print(f"Published {count} ERCOT LMP records to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
