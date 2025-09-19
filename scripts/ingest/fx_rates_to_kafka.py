#!/usr/bin/env python
"""Fetch ECB FX reference rates and publish to Kafka with Avro encoding."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable

import requests
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "fx.rate.v1.avsc"


def _load_schema() -> dict:
    schema_dict = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return parse_schema(schema_dict)


def _fetch_rates(base: str, symbols: Iterable[str]) -> tuple[datetime, Dict[str, float]]:
    symbol_list = ",".join(symbols)
    url = "https://api.exchangerate.host/latest"
    params = {"base": base.upper(), "symbols": symbol_list.upper(), "source": "ecb"}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    date_str = payload.get("date")
    as_of = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now()
    return as_of, payload.get("rates", {})


def _micros(dt: datetime) -> int:
    return int(dt.astimezone(timezone.utc).timestamp() * 1_000_000)


def _days(dt: datetime) -> int:
    epoch = datetime(1970, 1, 1)
    return (dt.date() - epoch.date()).days


def publish_rates(
    producer: KafkaProducer,
    topic: str,
    schema: dict,
    base: str,
    symbols: Iterable[str],
    metadata_source: str,
) -> int:
    as_of, rates = _fetch_rates(base, symbols)
    ingest_ts = _micros(datetime.now(timezone.utc))
    from io import BytesIO

    count = 0
    for quote, rate in sorted(rates.items()):
        record = {
            "base_currency": base.upper(),
            "quote_currency": quote.upper(),
            "rate": float(rate),
            "source": "ECB",
            "as_of_date": _days(as_of),
            "ingest_ts": ingest_ts,
            "metadata": {"api": metadata_source},
        }
        buffer = BytesIO()
        schemaless_writer(buffer, schema, record)
        producer.send(topic, value=buffer.getvalue())
        count += 1
    producer.flush()
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish ECB FX rates to Kafka")
    parser.add_argument("--base", default=os.getenv("FX_BASE_CURRENCY", "EUR"))
    parser.add_argument(
        "--symbols",
        default=os.getenv("FX_SYMBOLS", "USD,GBP,JPY,CAD,AUD"),
        help="Comma separated quote currencies",
    )
    parser.add_argument("--topic", default=os.getenv("FX_TOPIC", "aurum.ref.fx.rate.v1"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--schema-path", default=str(SCHEMA_PATH))
    parser.add_argument("--metadata-source", default="exchangerate.host")

    args = parser.parse_args()
    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if not symbols:
        raise SystemExit("No quote symbols specified")

    schema = _load_schema()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap)
    count = publish_rates(producer, args.topic, schema, args.base, symbols, args.metadata_source)
    print(f"Published {count} FX rates for base {args.base.upper()} to {args.topic}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
