#!/usr/bin/env python
"""Analyze the ingest DLQ topic and emit a health summary.

The script samples recent messages from the ingest error topic, aggregates
counts by source, and evaluates warn/critical thresholds. It supports both the
CLI entry point and programmatic invocation from Airflow DAGs.
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Dict, Iterable, Mapping, MutableMapping, Optional

try:
    from kafka import KafkaConsumer, TopicPartition  # type: ignore
except Exception as exc:  # pragma: no cover - dependency optional at import time
    KafkaConsumer = None  # type: ignore
    TopicPartition = None  # type: ignore

try:
    import requests
except Exception:  # pragma: no cover - requests is part of runtime deps
    requests = None  # type: ignore

try:
    from fastavro import parse_schema, schemaless_reader  # type: ignore
except Exception:
    parse_schema = schemaless_reader = None  # type: ignore


class DLQThresholdExceeded(RuntimeError):
    """Raised when DLQ counts breach configured thresholds."""

    def __init__(self, level: str, summary: str) -> None:
        super().__init__(summary)
        self.level = level
        self.summary = summary


@dataclass
class DLQSummary:
    window_minutes: int
    total_records: int
    per_source: Mapping[str, int]
    severities: Mapping[str, Mapping[str, int]]
    max_source: Optional[str]
    max_count: int


def _require_dependencies() -> None:
    if KafkaConsumer is None or TopicPartition is None:
        raise RuntimeError(
            "kafka-python is required for ingest DLQ processing. Install with 'pip install kafka-python'."
        )
    if requests is None:
        raise RuntimeError("The 'requests' package is required to fetch schemas from Schema Registry.")
    if parse_schema is None or schemaless_reader is None:
        raise RuntimeError("fastavro is required to decode ingest DLQ events. Install with 'pip install fastavro'.")


def _fetch_schema(schema_registry: str, schema_id: int) -> Dict[str, object]:
    url = f"{schema_registry.rstrip('/')}/schemas/ids/{schema_id}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    payload = resp.json()
    schema = payload.get("schema")
    if not schema:
        raise RuntimeError(f"Schema registry response for id {schema_id} missing 'schema' field")
    return json.loads(schema)


def _decode_avro(
    payload: bytes,
    *,
    schema_registry: str,
    cache: MutableMapping[int, tuple[dict, dict]],
) -> Mapping[str, object]:
    if len(payload) < 5:
        raise RuntimeError("Payload too short to be Avro encoded")
    if payload[0] != 0:
        raise RuntimeError("Unexpected magic byte in Avro payload")
    schema_id = struct.unpack(">I", payload[1:5])[0]
    if schema_id not in cache:
        raw_schema = _fetch_schema(schema_registry, schema_id)
        cache[schema_id] = (raw_schema, parse_schema(raw_schema))  # type: ignore[arg-type]
    raw_schema, parsed_schema = cache[schema_id]
    buf = BytesIO(payload[5:])
    record = schemaless_reader(buf, parsed_schema)  # type: ignore[arg-type]
    # Populate defaults when older producers omit newer fields
    if "severity" not in record:
        record["severity"] = "ERROR"
    if "recommended_action" not in record:
        record["recommended_action"] = None
    return record


def _iter_recent_records(
    consumer: KafkaConsumer,
    topic: str,
    *,
    max_messages: int,
    schema_registry: str,
) -> Iterable[Mapping[str, object]]:
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return []
    topic_partitions = [TopicPartition(topic, p) for p in partitions]
    consumer.assign(topic_partitions)
    end_offsets = consumer.end_offsets(topic_partitions)
    per_partition = max(max_messages // max(len(topic_partitions), 1), 1)
    for tp in topic_partitions:
        offset = max(end_offsets.get(tp, 0) - per_partition, 0)
        consumer.seek(tp, offset)

    schema_cache: Dict[int, tuple[dict, dict]] = {}
    consumed = 0
    while consumed < max_messages:
        msgs = consumer.poll(timeout_ms=500)
        if not msgs:
            break
        for records in msgs.values():
            for record in records:
                consumed += 1
                try:
                    decoded = _decode_avro(record.value, schema_registry=schema_registry, cache=schema_cache)
                except Exception as exc:
                    print(f"Failed to decode DLQ record at offset {record.offset}: {exc}")
                    continue
                yield decoded
                if consumed >= max_messages:
                    break
            if consumed >= max_messages:
                break


def analyse_dlq(
    *,
    bootstrap: str,
    schema_registry: str,
    topic: str,
    lookback: timedelta,
    max_messages: int,
    warn_threshold: int,
    crit_threshold: int,
) -> DLQSummary:
    _require_dependencies()
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="latest",
        consumer_timeout_ms=2000,
        value_deserializer=lambda v: v,
    )
    try:
        cutoff = datetime.now(timezone.utc) - lookback
        per_source: Counter[str] = Counter()
        per_source_severity: Dict[str, Counter[str]] = defaultdict(Counter)
        total = 0
        for record in _iter_recent_records(
            consumer,
            topic,
            max_messages=max_messages,
            schema_registry=schema_registry,
        ):
            ingest_ts_raw = record.get("ingest_ts")
            try:
                ingest_ts = datetime.fromtimestamp(int(ingest_ts_raw) / 1_000_000, tz=timezone.utc)
            except Exception:
                ingest_ts = None
            if ingest_ts is None or ingest_ts < cutoff:
                continue
            source = str(record.get("source") or "<unknown>")
            severity = str(record.get("severity") or "ERROR").upper()
            per_source[source] += 1
            per_source_severity[source][severity] += 1
            total += 1

        max_source, max_count = (None, 0)
        if per_source:
            max_source, max_count = per_source.most_common(1)[0]

        if max_count >= crit_threshold:
            summary = (
                f"DLQ critical: {max_source} emitted {max_count} events in the past "
                f"{int(lookback.total_seconds() // 60)} minutes (threshold {crit_threshold})."
            )
            raise DLQThresholdExceeded("critical", summary)
        if max_count >= warn_threshold:
            summary = (
                f"DLQ warning: {max_source} emitted {max_count} events in the past "
                f"{int(lookback.total_seconds() // 60)} minutes (threshold {warn_threshold})."
            )
            raise DLQThresholdExceeded("warning", summary)

        return DLQSummary(
            window_minutes=int(lookback.total_seconds() // 60),
            total_records=total,
            per_source=per_source,
            severities={k: dict(v) for k, v in per_source_severity.items()},
            max_source=max_source,
            max_count=max_count,
        )
    finally:
        consumer.close()


def _print_summary(summary: DLQSummary) -> None:
    print(
        f"Processed {summary.total_records} DLQ records over the past {summary.window_minutes} minutes."
    )
    if not summary.per_source:
        print("No recent DLQ traffic detected.")
        return
    for source, count in summary.per_source.most_common():
        severity_breakdown = ", ".join(
            f"{sev}:{summary.severities.get(source, {}).get(sev, 0)}"
            for sev in sorted(summary.severities.get(source, {}))
        )
        print(f"  - {source}: {count} (by severity: {severity_breakdown})")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyse ingest DLQ volumes and emit alerts")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--schema-registry", default=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"))
    parser.add_argument("--topic", default=os.getenv("AURUM_INGEST_ERROR_TOPIC", "aurum.ingest.error.v1"))
    parser.add_argument("--lookback-minutes", type=int, default=int(os.getenv("AURUM_DLQ_LOOKBACK_MINUTES", "15")))
    parser.add_argument("--max-messages", type=int, default=int(os.getenv("AURUM_DLQ_MAX_MESSAGES", "1500")))
    parser.add_argument("--warn-threshold", type=int, default=int(os.getenv("AURUM_DLQ_WARN_THRESHOLD", "20")))
    parser.add_argument("--crit-threshold", type=int, default=int(os.getenv("AURUM_DLQ_CRIT_THRESHOLD", "75")))
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        summary = analyse_dlq(
            bootstrap=args.bootstrap,
            schema_registry=args.schema_registry,
            topic=args.topic,
            lookback=timedelta(minutes=args.lookback_minutes),
            max_messages=args.max_messages,
            warn_threshold=args.warn_threshold,
            crit_threshold=args.crit_threshold,
        )
    except DLQThresholdExceeded as exc:
        print(exc.summary)
        return 2 if exc.level == "critical" else 1
    except Exception as exc:
        print(f"Failed to analyse DLQ topic: {exc}")
        return 3

    _print_summary(summary)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
