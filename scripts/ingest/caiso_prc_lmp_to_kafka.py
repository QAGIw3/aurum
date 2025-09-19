#!/usr/bin/env python
"""Ingest CAISO PRC_LMP data and publish to Kafka using Avro."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import os
import struct
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Iterable, Iterator

from aurum.compat import requests

try:  # pragma: no cover - optional dependency
    from fastavro import parse_schema, schemaless_writer
except ImportError:  # pragma: no cover - handled by _require_fastavro
    parse_schema = schemaless_writer = None  # type: ignore[misc]
from xml.etree import ElementTree as ET


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "ingest.error.v1.avsc"
DEFAULT_QUERYNAME = "PRC_LMP"


def _require_fastavro() -> None:
    if parse_schema is None or schemaless_writer is None:  # type: ignore[truthy-function]
        raise ModuleNotFoundError(
            "The 'fastavro' package is required for CAISO ingestion. Install it via 'pip install fastavro'."
        )


def _candidate_params(start: str, end: str, market: str) -> list[dict[str, str]]:
    # Build multiple datetime encodings commonly accepted by CAISO OASIS.
    # Accepts incoming ISO-ish strings and generates variants.
    def normalize(s: str) -> list[str]:
        s = s.replace("Z", "-0000")
        # try with seconds and without seconds, with hyphens and compact
        variants = set()
        try:
            # parse with fromisoformat after replacing offset colon for compat
            cleaned = s.replace("+00:00", "-0000").replace("-00:00", "-0000")
            dt = datetime.fromisoformat(cleaned.replace("-0000", "+00:00"))
            base = dt.strftime("%Y-%m-%dT%H:%M")
            base_sec = dt.strftime("%Y-%m-%dT%H:%M:%S")
            compact = dt.strftime("%Y%m%dT%H:%M")
            variants.update({f"{base}-0000", f"{base_sec}-0000", f"{compact}-0000"})
        except Exception:
            variants.add(s)
        return list(variants)

    starts = normalize(start)
    ends = normalize(end)
    out: list[dict[str, str]] = []
    for sd in starts:
        for ed in ends:
            out.append(
                {
                    "queryname": DEFAULT_QUERYNAME,
                    "resultformat": "6",
                    "version": "1",
                    "startdatetime": sd,
                    "enddatetime": ed,
                    "market_run_id": market,
                }
            )
    return out


def fetch_zip_content(
    url: str,
    params: dict[str, str],
    *,
    max_retries: int = 5,
    backoff_seconds: float = 2.0,
    debug: bool = False,
) -> bytes:
    # Try multiple datetime encodings until a non-INVALID_REQUEST payload is returned.
    session = requests.Session()
    candidates = [params]
    if "startdatetime" in params and "enddatetime" in params:
        candidates = _candidate_params(
            params["startdatetime"],
            params["enddatetime"],
            params.get("market_run_id", "RTPD"),
        )

    last_content = b""
    for p in candidates:
        attempts = 0
        while attempts < max_retries:
            attempts += 1
            if debug:
                logging.warning("CAISO OASIS attempt %s params=%s", attempts, p)
            resp = session.get(url, params=p, timeout=60)
            if resp.status_code == 429:
                time.sleep(backoff_seconds * attempts)
                continue
            resp.raise_for_status()
            content = resp.content
            last_content = content
            try:
                with zipfile.ZipFile(io.BytesIO(content)) as zf:
                    names = zf.namelist()
                    if not names:
                        time.sleep(backoff_seconds)
                        continue
                    invalid = any(name.upper().startswith("INVALID_REQUEST") for name in names)
                    if invalid:
                        # try next candidate or back off
                        time.sleep(backoff_seconds)
                        break
                    return content
            except zipfile.BadZipFile:
                time.sleep(backoff_seconds)
                continue
    return last_content


def extract_xml_bytes(archive: bytes) -> bytes:
    with zipfile.ZipFile(io.BytesIO(archive)) as zf:
        for name in zf.namelist():
            if name.lower().endswith(".xml"):
                return zf.read(name)
    raise ValueError("No XML file found in CAISO zip payload")


def _text(element: ET.Element, tag: str) -> str | None:
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return None


def _parse_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _to_timestamp_micros(text: str | None) -> int | None:
    if not text:
        return None
    # Prefer Zulu or offset-aware strings
    cleaned = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        # fallback: treat as naive UTC
        dt = datetime.strptime(cleaned, "%Y-%m-%dT%H:%M:%S")
    return int(dt.timestamp() * 1_000_000)


def iter_records(xml_bytes: bytes) -> Iterator[dict[str, object]]:
    root = ET.fromstring(xml_bytes)
    for report_data in root.findall(".//REPORT_DATA"):
        start = _text(report_data, "INTERVALSTARTTIME_UTC") or _text(
            report_data, "INTERVALSTARTTIME_GMT"
        )
        end = _text(report_data, "INTERVALENDTIME_UTC") or _text(
            report_data, "INTERVALENDTIME_GMT"
        )
        node = _text(report_data, "NODE") or ""
        lmp = _parse_float(_text(report_data, "LMP_PRC"))
        record = {
            "iso_code": "CAISO",
            "market": _text(report_data, "MARKET_RUN_ID") or "DAY_AHEAD",
            "delivery_date": 0,
            "interval_start": _to_timestamp_micros(start) or 0,
            "interval_end": _to_timestamp_micros(end),
            "interval_minutes": None,
            "location_id": _text(report_data, "NODE_ID") or node,
            "location_name": node or None,
            "location_type": (_text(report_data, "NODE_TYPE") or "NODE").upper(),
            "price_total": lmp or 0.0,
            "price_energy": _parse_float(_text(report_data, "ENERGY_PRC")),
            "price_congestion": _parse_float(_text(report_data, "CONGESTION_PRC")),
            "price_loss": _parse_float(_text(report_data, "LOSS_PRC")),
            "currency": "USD",
            "uom": "MWh",
            "settlement_point": _text(report_data, "NODE_SHORT") or None,
            "source_run_id": _text(report_data, "OASIS_RUN_ID") or None,
            "ingest_ts": 0,
            "record_hash": hashlib.sha256(
                (start or "")
                .encode("utf-8")
                + b"|"
                + (node or "").encode("utf-8")
                + b"|"
                + (str(lmp or "")).encode("utf-8")
            ).hexdigest(),
            "metadata": None,
        }
        if record["interval_start"]:
            record["delivery_date"] = int(
                datetime.utcfromtimestamp(record["interval_start"] / 1_000_000)
                .date()
                .toordinal()
                - datetime.utcfromtimestamp(0).date().toordinal()
            )
        yield record


def load_schema(schema_path: Path) -> tuple[dict[str, object], int]:
    _require_fastavro()
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    parsed = parse_schema(schema)  # type: ignore[arg-type]
    return parsed, schema


def load_dlq_schema() -> tuple[dict[str, object], int]:
    _require_fastavro()
    schema = json.loads(DLQ_SCHEMA_PATH.read_text(encoding="utf-8"))
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
    payload = resp.json()
    return int(payload["id"])


def build_producer(bootstrap: str):
    from kafka import KafkaProducer  # imported lazily to avoid optional dependency during tests

    return KafkaProducer(bootstrap_servers=bootstrap)


def produce_records(
    producer,
    topic: str,
    schema: dict[str, object],
    schema_id: int,
    records: Iterable[dict[str, object]],
) -> None:
    _require_fastavro()
    for record in records:
        record["ingest_ts"] = int(datetime.utcnow().timestamp() * 1_000_000)
        buffer = io.BytesIO()
        buffer.write(b"\x00")
        buffer.write(struct.pack(">I", schema_id))
        schemaless_writer(buffer, schema, record)
        producer.send(topic, value=buffer.getvalue())
    producer.flush()


def publish_error(
    producer,
    topic: str | None,
    schema: dict[str, object] | None,
    schema_id: int | None,
    source: str,
    error_message: str,
    context: dict[str, object],
) -> None:
    if producer is None or not topic or schema is None or schema_id is None:
        return
    _require_fastavro()
    record = {
        "source": source,
        "error_message": error_message,
        "context": {k: str(v) for k, v in context.items()},
        "ingest_ts": int(datetime.utcnow().timestamp() * 1_000_000),
    }
    buffer = io.BytesIO()
    buffer.write(b"\x00")
    buffer.write(struct.pack(">I", schema_id))
    schemaless_writer(buffer, schema, record)
    producer.send(topic, value=buffer.getvalue())
    producer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest CAISO PRC_LMP data into Kafka")
    parser.add_argument("--start", required=True, help="Start datetime (YYYY-MM-DDTHH:MM-0000)")
    parser.add_argument("--end", required=True, help="End datetime (YYYY-MM-DDTHH:MM-0000)")
    parser.add_argument("--market", default="RTPD", help="Market run ID (e.g. RTPD, RTM)")
    parser.add_argument(
        "--topic", default="aurum.iso.caiso.lmp.v1", help="Kafka topic for CAISO LMP"
    )
    parser.add_argument(
        "--schema-registry",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--oasis-url",
        default="https://oasis.caiso.com/oasisapi/SingleZip",
        help="CAISO OASIS SingleZip endpoint",
    )
    parser.add_argument("--node", help="Optional CAISO node filter (e.g., TH_SP15)")
    parser.add_argument("--resultformat", default="6")
    parser.add_argument("--version", default="1")
    parser.add_argument("--dlq-topic", default=os.environ.get("AURUM_DLQ_TOPIC"))
    parser.add_argument("--dlq-subject", default="aurum.ingest.error.v1")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-seconds", type=float, default=2.0)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--output-json",
        help="Optional path to write normalized records as a JSON array for downstream SeaTunnel jobs",
    )
    parser.add_argument(
        "--no-kafka",
        action="store_true",
        help="Skip producing to Kafka (useful when only staging data for SeaTunnel)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    params = {
        "queryname": DEFAULT_QUERYNAME,
        "resultformat": args.resultformat,
        "version": args.version,
        "startdatetime": args.start,
        "enddatetime": args.end,
        "market_run_id": args.market,
    }
    if args.node:
        params["node"] = args.node

    subject = f"{args.topic}-value" if not args.topic.endswith("-value") else args.topic

    producer = None
    parsed_schema = None
    schema_id: int | None = None
    dlq_schema = None
    dlq_schema_id: int | None = None
    dlq_topic = args.dlq_topic
    dlq_subject = args.dlq_subject or "aurum.ingest.error.v1"

    if not args.no_kafka:
        parsed_schema, _ = load_schema(SCHEMA_PATH)
        schema_id = fetch_schema_id(args.schema_registry, subject)
        producer = build_producer(args.bootstrap_servers)
        if dlq_topic:
            dlq_schema, _ = load_dlq_schema()
            dlq_schema_id = fetch_schema_id(args.schema_registry, dlq_subject)

    try:
        zip_bytes = fetch_zip_content(
            args.oasis_url,
            params,
            max_retries=args.max_retries,
            backoff_seconds=args.backoff_seconds,
            debug=args.debug,
        )
        xml_bytes = extract_xml_bytes(zip_bytes)
    except Exception as exc:
        if dlq_topic and dlq_schema and dlq_schema_id is not None:
            publish_error(
                producer,
                dlq_topic,
                dlq_schema,
                dlq_schema_id,
                source="caiso_prc_lmp_to_kafka",
                error_message=str(exc),
                context={"params": json.dumps(params)},
            )
        print(f"Failed to retrieve CAISO data: {exc}")
        return 1

    if b"INVALID_REQUEST" in xml_bytes.upper():
        publish_error(
            producer,
            dlq_topic,
            dlq_schema,
            dlq_schema_id,
            source="caiso_prc_lmp_to_kafka",
            error_message="CAISO returned INVALID_REQUEST",
            context={"params": json.dumps(params)},
        )
        print("CAISO returned INVALID_REQUEST; exiting")
        return 1

    records = list(iter_records(xml_bytes))
    if args.output_json:
        _write_json_output(records, args.output_json)

    if not records:
        publish_error(
            producer,
            dlq_topic,
            dlq_schema,
            dlq_schema_id,
            source="caiso_prc_lmp_to_kafka",
            error_message="No records parsed",
            context={"params": json.dumps(params)},
        )
        print("No CAISO records produced; exiting")
        return 0

    if args.no_kafka:
        target = args.output_json or ""
        print(f"Staged {len(records)} CAISO LMP records to {target}")
        return 0

    if parsed_schema is None or schema_id is None or producer is None:
        raise RuntimeError("Kafka producer not initialised")

    produce_records(producer, args.topic, parsed_schema, schema_id, records)
    print(f"Published {len(records)} CAISO LMP records to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
