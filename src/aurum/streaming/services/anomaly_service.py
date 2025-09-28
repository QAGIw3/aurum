"""Anomaly Detection Streaming Service using KafkaProcessor.

Runs the price anomaly handler against a real Kafka cluster using env config.

Environment variables (examples in parentheses):
- AURUM_KAFKA_BOOTSTRAP (kafka:9092)
- AURUM_KAFKA_GROUP (aurum-anomaly-service)
- AURUM_KAFKA_INPUT_TOPIC (prices)
- AURUM_KAFKA_OUTPUT_TOPIC (price_anomalies)
- AURUM_KAFKA_COMMIT (auto|sync|batch, default auto)
- AURUM_KAFKA_CLIENT (aiokafka|confluent, default aiokafka)
- AURUM_KAFKA_SECURITY_PROTOCOL (SASL_SSL|SSL|PLAINTEXT)
- AURUM_KAFKA_SASL_MECHANISM (PLAIN|SCRAM-SHA-256|SCRAM-SHA-512)
- AURUM_KAFKA_SASL_USERNAME
- AURUM_KAFKA_SASL_PASSWORD
- AURUM_KAFKA_SSL_CAFILE
- AURUM_KAFKA_SSL_CERTFILE
- AURUM_KAFKA_SSL_KEYFILE
- AURUM_SCHEMA_REGISTRY_URL (for confluent client)

Model/logic parameters:
- AURUM_ANOM_WINDOW (24)
- AURUM_ANOM_Z (3.0)

Usage:
  aurum-anomaly-service
"""
from __future__ import annotations

import asyncio
import os
import signal
from typing import Optional

from ..kafka_processor import KafkaProcessor, KafkaProcessorConfig
from ..handlers.ml_anomaly import register_price_anomaly_detector


def _bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.lower() in {"1", "true", "yes", "y", "on"}


async def _run() -> None:
    bootstrap = os.getenv("AURUM_KAFKA_BOOTSTRAP")
    group = os.getenv("AURUM_KAFKA_GROUP", "aurum-anomaly-service")
    input_topic = os.getenv("AURUM_KAFKA_INPUT_TOPIC", "prices")
    output_topic = os.getenv("AURUM_KAFKA_OUTPUT_TOPIC", "price_anomalies")
    commit = os.getenv("AURUM_KAFKA_COMMIT", "auto")
    client = os.getenv("AURUM_KAFKA_CLIENT", "aiokafka")

    window = int(os.getenv("AURUM_ANOM_WINDOW", "24"))
    z = float(os.getenv("AURUM_ANOM_Z", "3.0"))

    in_memory = bootstrap is None or bootstrap == ""

    cfg = KafkaProcessorConfig(
        bootstrap_servers=bootstrap,
        group_id=group,
        input_topics=(input_topic,),
        in_memory=in_memory,
        commit_strategy=commit,
        use_confluent_consumer=(client == "confluent"),
        use_confluent_producer=(client == "confluent"),
        schema_registry_url=os.getenv("AURUM_SCHEMA_REGISTRY_URL"),
        security_protocol=os.getenv("AURUM_KAFKA_SECURITY_PROTOCOL"),
        sasl_mechanism=os.getenv("AURUM_KAFKA_SASL_MECHANISM"),
        sasl_plain_username=os.getenv("AURUM_KAFKA_SASL_USERNAME"),
        sasl_plain_password=os.getenv("AURUM_KAFKA_SASL_PASSWORD"),
        ssl_cafile=os.getenv("AURUM_KAFKA_SSL_CAFILE"),
        ssl_certfile=os.getenv("AURUM_KAFKA_SSL_CERTFILE"),
        ssl_keyfile=os.getenv("AURUM_KAFKA_SSL_KEYFILE"),
    )

    proc = KafkaProcessor(cfg)
    register_price_anomaly_detector(
        proc,
        input_topic=input_topic,
        output_topic=output_topic,
        window=window,
        z_threshold=z,
    )

    # Shutdown handling
    stop_event = asyncio.Event()

    def _signal(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal)
        except NotImplementedError:
            pass

    await proc.start()
    await stop_event.wait()
    await proc.stop()


def main() -> None:
    asyncio.run(_run())


__all__ = ["main"]

