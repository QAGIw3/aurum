#!/usr/bin/env python
"""Compare Kafka topic offsets against Iceberg/Trino row counts to spot late data."""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

from kafka import KafkaConsumer, TopicPartition
from trino import dbapi


@dataclass
class ReconciliationTarget:
    name: str
    topic: str
    count_sql: str
    catalog: str
    schema: str | None = None


def load_plan(path: Path, default_catalog: str, default_schema: str | None) -> list[ReconciliationTarget]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    targets: list[ReconciliationTarget] = []
    for entry in payload.get("targets", []):
        targets.append(
            ReconciliationTarget(
                name=entry.get("name", entry["topic"]),
                topic=entry["topic"],
                count_sql=entry.get("count_sql", f"select count(*) from {entry['iceberg_table']}"),
                catalog=entry.get("catalog", entry.get("trino_catalog", default_catalog)),
                schema=entry.get("schema", entry.get("trino_schema", default_schema)),
            )
        )
    if not targets:
        raise ValueError(f"No reconciliation targets found in {path}")
    return targets


def fetch_offsets(bootstrap: str, topic: str) -> dict[int, int]:
    consumer = KafkaConsumer(bootstrap_servers=bootstrap, enable_auto_commit=False)
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        raise RuntimeError(f"Topic {topic} not found or has no partitions")
    assignments = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(assignments)
    consumer.seek_to_end(*assignments)
    offsets = {tp.partition: consumer.position(tp) for tp in assignments}
    consumer.close()
    return offsets


def run_trino_query(
    host: str,
    port: int,
    user: str,
    catalog: str,
    schema: str | None,
    sql: str,
) -> int:
    with dbapi.connect(host=host, port=port, user=user, catalog=catalog, schema=schema) as conn:
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(f"Query returned no rows: {sql}")
        value = row[0]
        if not isinstance(value, (int, float)):
            raise RuntimeError(f"Unexpected value type {type(value)} for query {sql}")
        return int(value)


def reconcile(
    *,
    bootstrap: str,
    trino_host: str,
    trino_port: int,
    trino_user: str,
    targets: Iterable[ReconciliationTarget],
    max_delta: int,
) -> int:
    exit_code = 0
    for target in targets:
        offsets = fetch_offsets(bootstrap, target.topic)
        total_offset = sum(offsets.values())
        lake_count = run_trino_query(trino_host, trino_port, trino_user, target.catalog, target.schema, target.count_sql)
        delta = total_offset - lake_count
        print(
            json.dumps(
                {
                    "target": target.name,
                    "topic": target.topic,
                    "total_offset": total_offset,
                    "lake_count": lake_count,
                    "delta": delta,
                    "partitions": offsets,
                },
                sort_keys=True,
            )
        )
        if abs(delta) > max_delta:
            exit_code = 2
    return exit_code


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Kafka offsets with Iceberg row counts")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--plan", type=Path, default=Path("config/kafka_lake_reconciliation.json"), help="Plan file mapping topics to queries")
    parser.add_argument("--trino-server", help="Full Trino server URL (overrides host/port)")
    parser.add_argument("--trino-host", default="localhost", help="Trino coordinator host")
    parser.add_argument("--trino-port", type=int, default=8080, help="Trino coordinator port")
    parser.add_argument("--trino-user", default="aurum", help="Trino user")
    parser.add_argument("--trino-catalog", default="iceberg", help="Default Trino catalog")
    parser.add_argument("--trino-schema", default=None, help="Default Trino schema")
    parser.add_argument("--max-delta", type=int, default=1000, help="Maximum acceptable difference before exit code 2")
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Iterable[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        trino_host = args.trino_host
        trino_port = args.trino_port
        if args.trino_server:
            parsed = urlparse(args.trino_server)
            if parsed.hostname:
                trino_host = parsed.hostname
            if parsed.port:
                trino_port = parsed.port
        targets = load_plan(args.plan, args.trino_catalog, args.trino_schema)
        return reconcile(
            bootstrap=args.bootstrap,
            trino_host=trino_host,
            trino_port=trino_port,
            trino_user=args.trino_user,
            targets=targets,
            max_delta=args.max_delta,
        )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Reconciliation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
