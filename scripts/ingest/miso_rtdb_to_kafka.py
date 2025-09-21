#!/usr/bin/env python
"""Ingest MISO Data Broker real-time LMP data and publish Avro records to Kafka."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import logging
import math
import os
import re
import struct
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from aurum.compat import requests

try:  # pragma: no cover - optional dependency
    from fastavro import parse_schema, schemaless_writer
except ImportError:  # pragma: no cover - handled by _require_fastavro
    parse_schema = schemaless_writer = None  # type: ignore[misc]


SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "iso.lmp.v1.avsc"
DLQ_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "ingest.error.v1.avsc"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DEFAULT_ENDPOINT = "/MISORTDBService/rest/data/getLMPConsolidatedTable"
_LOG = logging.getLogger("miso_rtdb_ingest")


def _require_fastavro() -> None:
    if parse_schema is None or schemaless_writer is None:  # type: ignore[truthy-function]
        raise ModuleNotFoundError(
            "The 'fastavro' package is required for MISO ingestion. Install it via 'pip install fastavro'."
        )


def _parse_ref_id(value: str) -> datetime:
    """Parse the RefId field into a timezone-aware datetime."""
    if not value:
        raise ValueError("RefId is empty")
    cleaned = value.strip()
    cleaned = cleaned.replace("Z", "+00:00")
    # Accept ISO strings with optional milliseconds/timezone
    try:
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y", "%Y%m%d"):
        try:
            dt = datetime.strptime(cleaned, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized RefId format: {value}")


_HOUR_AND_MIN_RE = re.compile(r"^(?P<hour>\d{1,2}):?(?P<minute>\d{2})$", re.ASCII)


def _parse_hour_min(value: str) -> tuple[int, int]:
    if not value:
        raise ValueError("HourAndMin is empty")
    match = _HOUR_AND_MIN_RE.match(value.strip())
    if not match:
        raise ValueError(f"Unrecognized HourAndMin format: {value}")
    hour = int(match.group("hour"))
    minute = int(match.group("minute"))
    if hour > 23 or minute > 59:
        raise ValueError(f"Invalid HourAndMin values: {value}")
    return hour, minute


def combine_interval(ref_id: str, hour_min: str, *, interval_seconds: int) -> tuple[datetime, datetime, int]:
    """Combine RefId and HourAndMin to produce interval start/end timestamps."""
    base = _parse_ref_id(ref_id)
    hour, minute = _parse_hour_min(hour_min)

    start = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if start < base and (base.hour or base.minute):
        # Some payloads encode RefId at the interval boundary already; keep the later timestamp.
        start = base
    end = start + timedelta(seconds=interval_seconds)
    interval_minutes = max(1, interval_seconds // 60)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc), interval_minutes


def _coerce_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not (isinstance(value, float) and math.isnan(value)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _to_upper(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return text.upper() or None
    return str(value).upper()


def _lookup(row: Mapping[str, object], *keys: str) -> object | None:
    lowered = {k.lower(): k for k in row.keys()}
    for key in keys:
        actual = lowered.get(key.lower())
        if actual is not None:
            value = row[actual]
            if isinstance(value, str):
                return value.strip()
            return value
    return None


def normalize_market(market: str) -> str:
    canonical = market.strip().upper()
    if canonical in {"DA", "DAY_AHEAD", "DAM"}:
        return "DAY_AHEAD"
    if canonical in {"RT", "RTM", "REAL_TIME", "RTHR", "RTRT"}:
        return "REAL_TIME"
    if canonical in {"RTPD", "FIVE_MINUTE"}:
        return "FIVE_MINUTE"
    return canonical or "REAL_TIME"


_ALLOWED_LOCATION_TYPES = {
    "NODE",
    "HUB",
    "ZONE",
    "SYSTEM",
    "AGGREGATE",
    "INTERFACE",
    "RESOURCE",
    "OTHER",
}


def _normalise_location_type(value: object | None) -> str:
    candidate = _to_upper(value) or "NODE"
    if candidate not in _ALLOWED_LOCATION_TYPES:
        if "HUB" in candidate:
            return "HUB"
        if "ZONE" in candidate:
            return "ZONE"
        if "SYSTEM" in candidate:
            return "SYSTEM"
        if "INTERFACE" in candidate:
            return "INTERFACE"
        return "OTHER"
    return candidate


def to_record(
    row: Mapping[str, object],
    *,
    market: str,
    region: str,
    interval_seconds: int,
    default_currency: str = "USD",
    default_uom: str = "MWh",
) -> dict[str, object]:
    ref_id = _lookup(row, "RefId", "RefID", "ref_id", "EventDate", "OperatingDay")
    hour_min = _lookup(row, "HourAndMin", "HourMinute", "Interval")
    if not isinstance(ref_id, str) or not isinstance(hour_min, str):
        raise ValueError("Missing RefId or HourAndMin in payload row")

    start_dt, end_dt, interval_minutes = combine_interval(ref_id, hour_min, interval_seconds=interval_seconds)

    market_norm = normalize_market(str(_lookup(row, "Market", "MarketType") or market))
    location_id = _lookup(row, "LocationId", "Location ID", "PnodeId", "CpnodeId", "CPNodeID", "CPNode Id")
    location_name = _lookup(row, "Location", "SettlementLocation", "CPNode", "NodeName", "PnodeName")

    if location_id is None:
        location_id = location_name or ""
    location_id_str = str(location_id)
    location_name_str = str(location_name) if location_name is not None else None

    price_total = _coerce_float(_lookup(row, "LMP", "Lmp", "SystemMarginalPrice", "Price"))
    if price_total is None:
        raise ValueError("Missing LMP price in payload row")
    price_congestion = _coerce_float(_lookup(row, "MCC", "MarginalCongestionComponent", "Congestion"))
    price_loss = _coerce_float(_lookup(row, "MLC", "MarginalLossComponent", "Loss"))

    price_energy = price_total
    if price_congestion is not None:
        price_energy -= price_congestion
    if price_loss is not None:
        price_energy -= price_loss

    timezone_name = _lookup(row, "TimeZone", "Timezone", "TZ")
    location_type = _normalise_location_type(
        _lookup(row, "LocationType", "Type", "Location Category", "SettlementLocationType")
    )

    record = {
        "iso_code": "MISO",
        "market": market_norm,
        "delivery_date": (start_dt.date() - EPOCH.date()).days,
        "interval_start": int(start_dt.timestamp() * 1_000_000),
        "interval_end": int(end_dt.timestamp() * 1_000_000),
        "interval_minutes": interval_minutes,
        "location_id": location_id_str,
        "location_name": location_name_str,
        "location_type": location_type,
        "zone": None,
        "hub": None,
        "timezone": str(timezone_name) if timezone_name else None,
        "price_total": price_total,
        "price_energy": price_energy,
        "price_congestion": price_congestion,
        "price_loss": price_loss,
        "currency": default_currency,
        "uom": default_uom,
        "settlement_point": location_name_str,
        "source_run_id": str(_lookup(row, "RefId", "RunId", "Version") or ref_id),
        "ingest_ts": int(datetime.utcnow().timestamp() * 1_000_000),
        "record_hash": hashlib.sha256(
            "|".join([
                str(ref_id),
                str(hour_min),
                location_id_str,
                f"{price_total:.6f}",
            ]).encode("utf-8")
        ).hexdigest(),
        "metadata": {
            "region": str(region),
        },
    }
    return record


def _walk_json(node: object) -> Iterator[object]:
    if isinstance(node, Mapping):
        yield node
        for value in node.values():
            yield from _walk_json(value)
    elif isinstance(node, list):
        for item in node:
            yield from _walk_json(item)


def iter_lmp_rows(payload: object) -> Iterator[Mapping[str, object]]:
    for node in _walk_json(payload):
        if isinstance(node, Mapping):
            keys = {k.lower() for k in node.keys()}
            if any(key in keys for key in ("lmp", "lmpvalue", "systemmarginalprice")) and (
                "refid" in keys or "hourandmin" in keys
            ):
                yield node  # type: ignore[return-value]


def fetch_json(
    url: str,
    params: Mapping[str, str],
    headers: Mapping[str, str],
    *,
    max_retries: int = 5,
    backoff_seconds: float = 2.0,
    timeout: int = 60,
) -> object:
    attempt = 0
    while attempt < max_retries:
        attempt += 1
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        if response.status_code == 429:
            wait = backoff_seconds * attempt
            _LOG.warning("MISO RTDB 429 - backing off %.1fs", wait)
            time.sleep(wait)
            continue
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError as exc:
            raise ValueError("Expected JSON response from MISO RTDB") from exc
    raise RuntimeError("Exceeded retries fetching MISO RTDB payload")


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


def publish_error(
    producer,
    topic: str | None,
    dlq_schema: dict[str, object] | None,
    schema_id: int | None,
    source: str,
    message: str,
    context: Mapping[str, object],
) -> None:
    if not producer or not topic or not dlq_schema or schema_id is None:
        return
    _require_fastavro()
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


def produce(
    producer,
    topic: str,
    parsed_schema: dict[str, object],
    schema_id: int,
    rows: Iterable[Mapping[str, object]],
    *,
    market: str,
    region: str,
    interval_seconds: int,
    dlq_topic: str | None = None,
    dlq_schema: dict[str, object] | None = None,
    dlq_schema_id: int | None = None,
) -> int:
    _require_fastavro()
    count = 0
    for row in rows:
        try:
            record = to_record(row, market=market, region=region, interval_seconds=interval_seconds)
            record["ingest_ts"] = int(datetime.utcnow().timestamp() * 1_000_000)
        except Exception as exc:  # pragma: no cover - defensive
            publish_error(
                producer,
                dlq_topic,
                dlq_schema,
                dlq_schema_id,
                source="miso_rtdb_to_kafka",
                message=str(exc),
                context={
                    "ref_id": _lookup(row, "RefId", "RefID") or "",
                    "hour_min": _lookup(row, "HourAndMin", "HourMinute") or "",
                },
            )
            continue
        buf = io.BytesIO()
        buf.write(b"\x00")
        buf.write(struct.pack(">I", schema_id))
        schemaless_writer(buf, parsed_schema, record)
        producer.send(topic, value=buf.getvalue())
        count += 1
    producer.flush()
    return count


def _parse_headers(entries: Iterable[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for entry in entries:
        if not entry:
            continue
        if ":" in entry:
            name, value = entry.split(":", 1)
        elif "=" in entry:
            name, value = entry.split("=", 1)
        else:
            raise ValueError(f"Invalid header format: {entry}")
        headers[name.strip()] = value.strip()
    return headers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest MISO RTDB LMP data into Kafka")
    parser.add_argument("--base-url", default=os.getenv("MISO_RTDB_BASE", "https://api.misoenergy.org"))
    parser.add_argument(
        "--endpoint",
        default=os.getenv("MISO_RTDB_ENDPOINT", DEFAULT_ENDPOINT),
        help="Endpoint path for getLMPConsolidatedTable",
    )
    parser.add_argument("--start", required=True, help="Interval start timestamp (ISO)")
    parser.add_argument("--end", required=True, help="Interval end timestamp (ISO)")
    parser.add_argument("--market", default=os.getenv("MISO_RTDB_MARKET", "RTM"))
    parser.add_argument("--region", default=os.getenv("MISO_RTDB_REGION", "ALL"))
    parser.add_argument("--topic", default=os.getenv("MISO_RTDB_TOPIC", "aurum.iso.miso.lmp.v1"))
    parser.add_argument("--subject", default=None, help="Schema Registry subject (default <topic>-value)")
    parser.add_argument(
        "--schema-registry",
        default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("MISO_INTERVAL_SECONDS", "300")),
        help="Interval duration in seconds (default 300)",
    )
    headers_env = os.getenv("MISO_RTDB_HEADERS")
    default_headers: list[str] = []
    if headers_env:
        default_headers = [h.strip() for h in headers_env.split("||") if h.strip()]

    parser.add_argument(
        "--header",
        action="append",
        default=default_headers,
        help="Additional request header in the form Name:Value",
    )
    parser.add_argument("--dlq-topic", default=os.getenv("AURUM_DLQ_TOPIC"))
    parser.add_argument("--dlq-subject", default="aurum.ingest.error.v1")
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--backoff-seconds", type=float, default=2.0)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--dry-run", action="store_true", help="Render records to stdout instead of producing")
    parser.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO))

    base_url = args.base_url.rstrip("/")
    endpoint = args.endpoint if args.endpoint.startswith("/") else f"/{args.endpoint}"
    url = f"{base_url}{endpoint}"

    params = {
        "start": args.start,
        "end": args.end,
        "market": args.market,
        "region": args.region,
    }
    headers = {"Accept": "application/json"}
    headers.update(_parse_headers(args.header))

    payload = fetch_json(
        url,
        params=params,
        headers=headers,
        max_retries=args.max_retries,
        backoff_seconds=args.backoff_seconds,
        timeout=args.timeout,
    )

    rows = list(iter_lmp_rows(payload))
    if args.dry_run:
        sample = [
            to_record(row, market=args.market, region=args.region, interval_seconds=args.interval_seconds)
            for row in rows
        ]
        print(json.dumps(sample, indent=2, default=str))
        return 0

    parsed_schema, _ = load_schema()
    subject = args.subject or f"{args.topic}-value"
    schema_id = fetch_schema_id(args.schema_registry, subject)

    dlq_schema = dlq_schema_id = None
    if args.dlq_topic:
        dlq_schema, _ = load_dlq_schema()
        dlq_schema_id = fetch_schema_id(args.schema_registry, args.dlq_subject)

    producer = build_producer(args.bootstrap_servers)
    try:
        count = produce(
            producer,
            args.topic,
            parsed_schema,
            schema_id,
            rows,
            market=args.market,
            region=args.region,
            interval_seconds=args.interval_seconds,
            dlq_topic=args.dlq_topic,
            dlq_schema=dlq_schema,
            dlq_schema_id=dlq_schema_id,
        )
        _LOG.info("Produced %s MISO RTDB records to %s", count, args.topic)
    finally:
        producer.flush()
        producer.close()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
