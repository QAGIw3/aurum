"""Kafka sink for structured logging."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient

from .structured_logger import LogEvent


class KafkaLogSink:
    """Kafka sink for real-time structured logging."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        use_avro: bool = True,
        schema_registry_url: Optional[str] = None,
        key_schema: Optional[Dict[str, Any]] = None,
        value_schema: Optional[Dict[str, Any]] = None
    ):
        """Initialize Kafka log sink.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topic: Kafka topic for logs
            use_avro: Whether to use Avro serialization
            schema_registry_url: Schema Registry URL
            key_schema: Avro schema for keys
            value_schema: Avro schema for values
        """
        self.topic = topic
        self.use_avro = use_avro
        avro_enabled = bool(use_avro and schema_registry_url)
        self._avro_enabled = avro_enabled

        # Kafka configuration
        kafka_config = {
            'bootstrap.servers': bootstrap_servers,
            'acks': 'all',  # Ensure durability
            'retries': 10,
            'retry.backoff.ms': 100,
            'delivery.timeout.ms': 30000,
            'request.timeout.ms': 10000,
            'compression.type': 'lz4',  # Good balance of speed and compression
        }

        if avro_enabled:
            # Schema Registry configuration
            schema_registry_config = {
                'url': schema_registry_url,
                'basic.auth.user.info': '',  # Add credentials if needed
            }

            # Create Schema Registry client
            schema_registry_client = SchemaRegistryClient(schema_registry_config)

            # Define Avro schemas
            if not key_schema:
                key_schema = {
                    "type": "record",
                    "name": "LogKey",
                    "fields": [
                        {"name": "source_name", "type": "string"},
                        {"name": "timestamp", "type": "string"}
                    ]
                }

            if not value_schema:
                value_schema = {
                    "type": "record",
                    "name": "LogEvent",
                    "fields": [
                        {"name": "timestamp", "type": "string"},
                        {"name": "level", "type": "string"},
                        {"name": "message", "type": "string"},
                        {"name": "event_type", "type": "string"},
                        {"name": "source_name", "type": ["null", "string"], "default": None},
                        {"name": "dataset", "type": ["null", "string"], "default": None},
                        {"name": "job_id", "type": ["null", "string"], "default": None},
                        {"name": "task_id", "type": ["null", "string"], "default": None},
                        {"name": "duration_ms", "type": ["null", "double"], "default": None},
                        {"name": "records_processed", "type": ["null", "long"], "default": None},
                        {"name": "bytes_processed", "type": ["null", "long"], "default": None},
                        {"name": "error_code", "type": ["null", "string"], "default": None},
                        {"name": "error_message", "type": ["null", "string"], "default": None},
                        {"name": "hostname", "type": ["null", "string"], "default": None},
                        {"name": "container_id", "type": ["null", "string"], "default": None},
                        {"name": "thread_id", "type": ["null", "string"], "default": None},
                        {"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": None}
                    ]
                }

            # Avro producer configuration
            avro_config = kafka_config.copy()
            avro_config.update({
                'schema.registry.url': schema_registry_url,
            })

            self.producer = AvroProducer(
                avro_config,
                default_key_schema=json.dumps(key_schema),
                default_value_schema=json.dumps(value_schema),
            )

        else:
            # JSON producer configuration
            self.producer = Producer(kafka_config)

    def emit(self, event: LogEvent) -> None:
        """Emit log event to Kafka.

        Args:
            event: Log event to emit
        """
        # Create key for partitioning
        key = {
            "source_name": event.source_name or "unknown",
            "timestamp": event.timestamp
        }

        # Convert event to value
        value = event.to_dict()

        # Emit to Kafka
        try:
            self.producer.produce(
                topic=self.topic,
                key=key if self._avro_enabled else json.dumps(key, default=str).encode(),
                value=value if self._avro_enabled else json.dumps(value, default=str).encode(),
                callback=self._delivery_callback
            )

        except Exception as e:
            print(f"Failed to produce log message: {e}")
            raise

    def _delivery_callback(self, err, msg) -> None:
        """Delivery callback for Kafka producer.

        Args:
            err: Error if delivery failed
            msg: Delivered message
        """
        if err:
            print(f"Failed to deliver log message: {err}")
        else:
            # Optional: Add delivery confirmation logging
            pass

    def flush(self, timeout: float = 5.0) -> None:
        """Flush pending messages to Kafka.

        Args:
            timeout: Timeout in seconds
        """
        self.producer.flush(timeout)

    def close(self) -> None:
        """Close the Kafka producer."""
        self.flush()
        self.producer = None
