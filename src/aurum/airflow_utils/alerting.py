"""Alerting utilities shared across Airflow DAGs."""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional

ALERT_TOPIC_DEFAULT = os.getenv("AURUM_ALERT_TOPIC", "aurum.alert.v1")
ALERT_SCHEMA_PATH = Path(__file__).resolve().parents[2] / "kafka" / "schemas" / "alert.v1.avsc"


def _load_avro_producer():  # pragma: no cover - requires confluent stack
    try:
        from confluent_kafka import avro  # type: ignore
        from confluent_kafka.avro import AvroProducer  # type: ignore
    except Exception:
        return None, None
    return avro, AvroProducer


def emit_alert(
    message: str,
    *,
    severity: str = "ERROR",
    source: str,
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """Emit an alert to the configured Kafka topic if the runtime supports it."""

    bootstrap = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    if not bootstrap:
        print(f"[alert] bootstrap servers not configured; skipping alert: {message}")
        return
    schema_registry = schema_registry_url or os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
    alert_topic = topic or ALERT_TOPIC_DEFAULT

    avro, producer_cls = _load_avro_producer()
    if not avro or not producer_cls:
        print(f"[alert] confluent_kafka.avro not available; skipping alert: {message}")
        return

    try:
        value_schema = avro.load(str(ALERT_SCHEMA_PATH))  # type: ignore[attr-defined]
    except Exception as exc:  # pragma: no cover - schema missing
        print(f"[alert] failed to load alert schema: {exc}")
        return

    try:
        producer = producer_cls(
            {"bootstrap.servers": bootstrap, "schema.registry.url": schema_registry},
            default_value_schema=value_schema,
        )
    except Exception as exc:  # pragma: no cover - producer misconfiguration
        print(f"[alert] failed to initialise AvroProducer: {exc}")
        return

    payload = {
        "alert_id": str(uuid.uuid4()),
        "tenant_id": None,
        "category": "OPERATIONS",
        "severity": severity,
        "source": source,
        "message": message,
        "payload": json.dumps(context or {}),
        "created_ts": int(datetime.now(timezone.utc).timestamp() * 1_000_000),
    }
    try:
        producer.produce(topic=alert_topic, value=payload)
        producer.flush(2)
    except Exception as exc:  # pragma: no cover - transient failure
        print(f"[alert] unable to emit alert to {alert_topic}: {exc}")


def build_failure_callback(
    *,
    source: str,
    severity: str = "CRITICAL",
    topic: Optional[str] = None,
    bootstrap_servers: Optional[str] = None,
    schema_registry_url: Optional[str] = None,
) -> Callable[[Dict[str, Any]], None]:
    """Return an Airflow ``on_failure_callback`` that emits a Kafka alert."""

    def _callback(context: Dict[str, Any]) -> None:
        dag_id = context.get("dag_id")
        dag_run = context.get("dag_run")
        run_id = getattr(dag_run, "run_id", None)
        execution_date = getattr(dag_run, "execution_date", None)
        summary = {
            "dag_id": dag_id,
            "run_id": run_id,
            "execution_date": str(execution_date) if execution_date is not None else None,
        }
        emit_alert(
            message=f"Airflow DAG {dag_id} failed",
            severity=severity,
            source=source,
            topic=topic,
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            context=summary,
        )

    return _callback


__all__ = ["emit_alert", "build_failure_callback"]
