#!/usr/bin/env python
"""Unified ISO adapter runner for LMP -> Kafka (Avro).

Supports CAISO, MISO, ISO-NE, ERCOT, SPP via the shared IsoAdapter base.

Example:
  ./scripts/ingest/iso_adapter_lmp_to_kafka.py \
      --provider isone \
      --series-id demo.isone.lmp \
      --start 2025-01-01T00:00:00Z \
      --end 2025-01-01T04:00:00Z \
      --topic aurum.iso.isone.lmp.v1 \
      --schema-registry $SCHEMA_REGISTRY_URL \
      --bootstrap-servers $KAFKA_BOOTSTRAP_SERVERS
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

from aurum.external.adapters.caiso import CaisoAdapter
from aurum.external.adapters.miso import MisoAdapter
from aurum.external.adapters.isone import IsoneAdapter
from aurum.external.adapters.ercot import ErcotAdapter
from aurum.external.adapters.spp import SppAdapter
from aurum.external.adapters.base import IsoRequestChunk


ADAPTERS = {
    "caiso": CaisoAdapter,
    "miso": MisoAdapter,
    "isone": IsoneAdapter,
    "ercot": ErcotAdapter,
    "spp": SppAdapter,
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run ISO adapter to emit LMP records to Kafka")
    p.add_argument("--provider", required=True, choices=sorted(ADAPTERS.keys()))
    p.add_argument("--series-id", required=True, help="Series identifier stamped on records")
    p.add_argument("--start", required=True, help="Start time (ISO 8601)")
    p.add_argument("--end", required=True, help="End time (ISO 8601)")
    p.add_argument("--topic", required=True, help="Kafka topic (e.g., aurum.iso.isone.lmp.v1)")
    p.add_argument("--schema-registry", dest="schema_registry_url", help="Schema Registry URL")
    p.add_argument("--bootstrap-servers", dest="bootstrap_servers", help="Kafka bootstrap servers")
    return p.parse_args()


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def main() -> int:
    args = parse_args()
    adapter_cls = ADAPTERS[args.provider]
    adapter = adapter_cls(series_id=args.series_id, kafka_topic=args.topic, schema_registry_url=args.schema_registry_url)

    # Apply optional Kafka bootstrap from CLI env
    if args.bootstrap_servers:
        adapter.collector.config.kafka_bootstrap_servers = args.bootstrap_servers

    start = _parse_dt(args.start)
    end = _parse_dt(args.end)

    chunk = IsoRequestChunk(start=start, end=end)
    emitted = adapter.run_window(chunk, flush=True)
    print(f"Emitted {emitted} records to {args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

