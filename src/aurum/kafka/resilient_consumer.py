"""Resilient Kafka consumer with cooperative-sticky rebalancing and enhanced error handling."""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union, Callable

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from ..config import settings
from ..observability.metrics import get_metrics_client


@dataclass
class ConsumerConfig:
    """Configuration for resilient Kafka consumer."""
    bootstrap_servers: str
    group_id: str
    client_id: str
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False  # Manual commit for better control
    max_poll_records: int = 1000
    max_poll_interval_ms: int = 300000  # 5 minutes
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    partition_assignment_strategy: str = "cooperative-sticky"  # Cooperative rebalancing

    # Reliability settings
    retry_backoff_ms: int = 100
    retry_backoff_max_ms: int = 5000
    retries: int = 5

    # Dead letter queue settings
    dlq_topic: Optional[str] = None
    dlq_max_retries: int = 3
    dlq_retry_delay_seconds: int = 60

    # Schema registry
    schema_registry_url: Optional[str] = None
    schema_registry_auth: str = ""

    # Additional consumer config
    extra_config: Dict[str, Any] = field(default_factory=dict)


class MessageHandler(ABC):
    """Abstract base class for message handlers."""

    @abstractmethod
    async def handle_message(self, message: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
        """Handle a message. Return True if successful, False to retry."""
        pass

    @abstractmethod
    async def handle_error(self, error: Exception, message: Dict[str, Any], metadata: Dict[str, Any]) -> bool:
        """Handle an error. Return True to retry, False to send to DLQ."""
        pass


@dataclass
class ConsumerStats:
    """Consumer statistics."""
    messages_processed: int = 0
    messages_failed: int = 0
    messages_retried: int = 0
    messages_dlq: int = 0
    last_message_time: float = 0.0
    processing_time_total: float = 0.0
    lag: int = 0


class ResilientKafkaConsumer:
    """Resilient Kafka consumer with cooperative-sticky rebalancing and comprehensive error handling."""

    def __init__(
        self,
        config: ConsumerConfig,
        message_handler: MessageHandler,
        producer_client: Optional[Any] = None  # For DLQ
    ):
        self.config = config
        self.message_handler = message_handler
        self.producer_client = producer_client
        self.consumer = None
        self.schema_registry_client = None
        self.avro_deserializer = None
        self.string_deserializer = StringDeserializer('utf-8')
        self.metrics = get_metrics_client()

        # Consumer state
        self.running = False
        self.stats = ConsumerStats()
        self.retry_budgets: Dict[str, int] = {}  # Per-key retry budget
        self.processing_tasks: Dict[str, asyncio.Task] = {}

        # Backoff state
        self.backoff_until: float = 0.0
        self.consecutive_errors = 0

    async def initialize(self):
        """Initialize the consumer and schema registry."""
        self._init_consumer()
        self._init_schema_registry()

    def _init_consumer(self):
        """Initialize the Kafka consumer with resilient configuration."""
        consumer_config = {
            # Basic configuration
            'bootstrap.servers': self.config.bootstrap_servers,
            'group.id': self.config.group_id,
            'client.id': self.config.client_id,
            'auto.offset.reset': self.config.auto_offset_reset,
            'enable.auto.commit': self.config.enable_auto_commit,

            # Cooperative-sticky rebalancing
            'partition.assignment.strategy': self.config.partition_assignment_strategy,

            # Poll and session configuration
            'max.poll.records': self.config.max_poll_records,
            'max.poll.interval.ms': self.config.max_poll_interval_ms,
            'session.timeout.ms': self.config.session_timeout_ms,
            'heartbeat.interval.ms': self.config.heartbeat_interval_ms,

            # Reliability configuration
            'retry.backoff.ms': self.config.retry_backoff_ms,
            'retry.backoff.max.ms': self.config.retry_backoff_max_ms,
            'reconnect.backoff.ms': self.config.retry_backoff_ms,
            'reconnect.backoff.max.ms': self.config.retry_backoff_max_ms,

            # Buffer and performance
            'receive.message.max.bytes': 10485760,  # 10MB
            'fetch.message.max.bytes': 10485760,    # 10MB
            'socket.receive.buffer.bytes': 1048576,
            'socket.keepalive.enable': True,

            # Monitoring
            'statistics.interval.ms': 30000,
            'log_level': 4,

            # Additional configuration
            **self.config.extra_config
        }

        self.consumer = Consumer(consumer_config)

        # Set up error callback
        def error_callback(err: KafkaError):
            if err.code() == KafkaError._PARTITION_EOF:
                return  # End of partition, normal

            self.metrics.increment_counter("kafka_consumer_errors")
            logging.error(f"Kafka consumer error: {err}")
            self.consecutive_errors += 1

            # Implement backoff on persistent errors
            if self.consecutive_errors > 5:
                backoff_time = min(30.0, 2 ** self.consecutive_errors)
                self.backoff_until = time.time() + backoff_time
                self.consecutive_errors = 0

        self.consumer.subscribe(
            [self.config.group_id.split('_')[0]],  # Subscribe to topic
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost
        )

    def _on_assign(self, consumer, partitions):
        """Called when partitions are assigned (cooperative rebalancing)."""
        self.metrics.increment_counter("kafka_consumer_rebalance_assign")
        logging.info(f"Partitions assigned: {[p.partition for p in partitions]}")

    def _on_revoke(self, consumer, partitions):
        """Called when partitions are revoked (cooperative rebalancing)."""
        self.metrics.increment_counter("kafka_consumer_rebalance_revoke")
        logging.info(f"Partitions revoked: {[p.partition for p in partitions]}")

    def _on_lost(self, consumer, partitions):
        """Called when partitions are lost."""
        self.metrics.increment_counter("kafka_consumer_rebalance_lost")
        logging.warning(f"Partitions lost: {[p.partition for p in partitions]}")

    def _init_schema_registry(self):
        """Initialize schema registry for deserialization."""
        if self.config.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.config.schema_registry_url,
                'basic.auth.user.info': self.config.schema_registry_auth,
            })

            self.avro_deserializer = AvroDeserializer(
                self.schema_registry_client,
                None  # Schema will be determined from message
            )

    def _deserialize_message(self, message: Message) -> Optional[Dict[str, Any]]:
        """Deserialize message based on content type."""
        try:
            if message.value():
                if self.avro_deserializer and message.key():
                    # Try Avro deserialization
                    return self.avro_deserializer(
                        message.value(),
                        None  # SerializationContext not needed for deserialization
                    )
                else:
                    # JSON deserialization
                    return json.loads(message.value().decode('utf-8'))
            return None
        except Exception as e:
            self.metrics.increment_counter("kafka_consumer_deserialization_errors")
            logging.error(f"Failed to deserialize message: {e}")
            return None

    async def _send_to_dlq(self, message: Message, error: Exception, retry_count: int):
        """Send a message to the dead letter queue."""
        if not self.config.dlq_topic or not self.producer_client:
            return

        try:
            # Create DLQ message with metadata
            dlq_message = {
                'original_message': {
                    'topic': message.topic(),
                    'partition': message.partition(),
                    'offset': message.offset(),
                    'key': message.key().decode('utf-8') if message.key() else None,
                    'value': message.value().decode('utf-8') if message.value() else None,
                    'timestamp': message.timestamp()[1],
                    'headers': dict(message.headers()) if message.headers() else {}
                },
                'error': str(error),
                'retry_count': retry_count,
                'dlq_timestamp': int(time.time() * 1000),
                'consumer_group': self.config.group_id,
                'processing_time': time.time()
            }

            # Send to DLQ topic
            await self.producer_client.produce(
                topic=self.config.dlq_topic,
                value=dlq_message,
                key=message.key(),
                headers={
                    'dlq_reason': 'processing_error',
                    'original_topic': message.topic(),
                    'retry_count': str(retry_count),
                    'error_type': type(error).__name__
                }
            )

            self.stats.messages_dlq += 1
            self.metrics.increment_counter("kafka_consumer_dlq_messages")

        except Exception as e:
            self.metrics.increment_counter("kafka_consumer_dlq_errors")
            logging.error(f"Failed to send message to DLQ: {e}")

    async def _process_message(self, message: Message):
        """Process a single message with retry logic and DLQ."""
        start_time = time.time()
        message_key = message.key().decode('utf-8') if message.key() else str(message.offset())
        task_id = f"{message.topic()}-{message.partition()}-{message.offset()}"

        try:
            # Deserialize message
            deserialized = self._deserialize_message(message)
            if not deserialized:
                raise ValueError("Failed to deserialize message")

            # Extract metadata
            metadata = {
                'topic': message.topic(),
                'partition': message.partition(),
                'offset': message.offset(),
                'key': message_key,
                'timestamp': message.timestamp()[1],
                'headers': dict(message.headers()) if message.headers() else {}
            }

            # Check retry budget
            retry_budget = self.retry_budgets.get(message_key, self.config.retries)
            if retry_budget <= 0:
                await self._send_to_dlq(message, Exception("Retry budget exhausted"), 0)
                return

            # Handle the message
            success = await self.message_handler.handle_message(deserialized, metadata)

            if success:
                # Success - reset retry budget and commit
                self.retry_budgets[message_key] = self.config.retries
                self.stats.messages_processed += 1
                self.consecutive_errors = 0
            else:
                # Failed - decrement retry budget and retry
                self.retry_budgets[message_key] = retry_budget - 1
                self.stats.messages_retried += 1
                self.consecutive_errors += 1

                # If still have retries, re-queue the message
                if self.retry_budgets[message_key] > 0:
                    # Implement exponential backoff
                    backoff_time = min(60.0, 2 ** (self.config.retries - self.retry_budgets[message_key]))
                    await asyncio.sleep(backoff_time)
                    # Re-process the message
                    await self._process_message(message)
                else:
                    # Send to DLQ
                    await self._send_to_dlq(message, Exception("Max retries exceeded"), self.config.retries)

        except Exception as e:
            self.stats.messages_failed += 1
            self.consecutive_errors += 1
            self.metrics.increment_counter("kafka_consumer_processing_errors")

            logging.error(f"Error processing message {task_id}: {e}")

            # Send to DLQ
            await self._send_to_dlq(message, e, self.retry_budgets.get(message_key, 0))

        finally:
            processing_time = time.time() - start_time
            self.stats.processing_time_total += processing_time
            self.stats.last_message_time = time.time()

            # Clean up task
            if task_id in self.processing_tasks:
                del self.processing_tasks[task_id]

    async def _poll_messages(self):
        """Poll for messages and process them."""
        while self.running:
            try:
                # Check backoff
                if time.time() < self.backoff_until:
                    await asyncio.sleep(1.0)
                    continue

                # Poll for messages
                message = self.consumer.poll(timeout=1.0)

                if message is None:
                    continue

                # Handle errors
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                # Process message asynchronously
                task_id = f"{message.topic()}-{message.partition()}-{message.offset()}"
                if task_id not in self.processing_tasks:
                    task = asyncio.create_task(self._process_message(message))
                    self.processing_tasks[task_id] = task

                # Clean up completed tasks
                completed_tasks = [
                    task_id for task_id, task in self.processing_tasks.items()
                    if task.done()
                ]
                for task_id in completed_tasks:
                    del self.processing_tasks[task_id]

            except Exception as e:
                self.metrics.increment_counter("kafka_consumer_poll_errors")
                logging.error(f"Error polling messages: {e}")

                # Implement backoff
                if self.consecutive_errors > 5:
                    backoff_time = min(30.0, 2 ** self.consecutive_errors)
                    self.backoff_until = time.time() + backoff_time
                    self.consecutive_errors = 0

                await asyncio.sleep(1.0)

    async def _update_stats(self):
        """Update consumer statistics."""
        while self.running:
            try:
                await asyncio.sleep(30.0)  # Update every 30 seconds

                # Get consumer lag
                try:
                    # This would need actual lag calculation implementation
                    self.stats.lag = 0  # Placeholder
                except Exception:
                    pass

                # Report metrics
                self.metrics.gauge("kafka_consumer_messages_processed", self.stats.messages_processed)
                self.metrics.gauge("kafka_consumer_messages_failed", self.stats.messages_failed)
                self.metrics.gauge("kafka_consumer_messages_retried", self.stats.messages_retried)
                self.metrics.gauge("kafka_consumer_messages_dlq", self.stats.messages_dlq)
                self.metrics.gauge("kafka_consumer_lag", self.stats.lag)
                self.metrics.histogram("kafka_consumer_processing_time", self.stats.processing_time_total)

            except Exception as e:
                logging.error(f"Error updating consumer stats: {e}")

    async def start(self):
        """Start the consumer."""
        self.running = True
        await self.initialize()

        # Start background tasks
        asyncio.create_task(self._poll_messages())
        asyncio.create_task(self._update_stats())

        logging.info(f"Started resilient Kafka consumer for group {self.config.group_id}")

    async def stop(self):
        """Stop the consumer."""
        self.running = False

        # Wait for processing tasks to complete
        if self.processing_tasks:
            await asyncio.gather(*self.processing_tasks.values(), return_exceptions=True)

        # Close consumer
        if self.consumer:
            self.consumer.close()

        logging.info("Stopped resilient Kafka consumer")

    async def commit(self):
        """Manually commit current offsets."""
        if self.consumer:
            self.consumer.commit()

    def get_stats(self) -> ConsumerStats:
        """Get consumer statistics."""
        return self.stats.copy()


class ConsumerPool:
    """Pool of resilient Kafka consumers for load balancing."""

    def __init__(self, pool_size: int = 3, **consumer_config):
        self.pool_size = pool_size
        self.consumers = []
        self.current_index = 0

        # Create consumers
        for i in range(pool_size):
            config = ConsumerConfig(
                client_id=f"aurum-consumer-{i}",
                **consumer_config
            )
            consumer = ResilientKafkaConsumer(
                config,
                message_handler=None  # Will be set per consumer
            )
            self.consumers.append(consumer)

    async def start_all(self, message_handler: MessageHandler):
        """Start all consumers with the same message handler."""
        for consumer in self.consumers:
            consumer.message_handler = message_handler
            await consumer.start()

    async def stop_all(self):
        """Stop all consumers."""
        await asyncio.gather(*[consumer.stop() for consumer in self.consumers])

    def get_consumer(self) -> ResilientKafkaConsumer:
        """Get next consumer in round-robin fashion."""
        consumer = self.consumers[self.current_index]
        self.current_index = (self.current_index + 1) % self.pool_size
        return consumer


# Convenience functions for common use cases
async def create_eia_consumer(
    group_id: str,
    message_handler: MessageHandler,
    producer_client: Optional[Any] = None,
    **consumer_config
) -> ResilientKafkaConsumer:
    """Create a resilient consumer for EIA data."""
    config = ConsumerConfig(
        group_id=group_id,
        dlq_topic="aurum.eia.dlq",
        **consumer_config
    )

    consumer = ResilientKafkaConsumer(config, message_handler, producer_client)

    # EIA-specific optimizations
    consumer_config.update({
        'max.poll.records': 500,  # Smaller batches for EIA
        'session.timeout.ms': 60000,  # Longer session timeout
        'heartbeat.interval.ms': 10000,  # Longer heartbeat
    })

    return consumer


async def create_weather_consumer(
    group_id: str,
    message_handler: MessageHandler,
    producer_client: Optional[Any] = None,
    **consumer_config
) -> ResilientKafkaConsumer:
    """Create a resilient consumer for weather data."""
    config = ConsumerConfig(
        group_id=group_id,
        dlq_topic="aurum.weather.dlq",
        **consumer_config
    )

    consumer = ResilientKafkaConsumer(config, message_handler, producer_client)

    # Weather-specific optimizations
    consumer_config.update({
        'max.poll.records': 1000,  # Larger batches for weather
        'session.timeout.ms': 30000,  # Standard timeout
        'heartbeat.interval.ms': 5000,  # More frequent heartbeat
    })

    return consumer


__all__ = [
    "ConsumerConfig",
    "MessageHandler",
    "ConsumerStats",
    "ResilientKafkaConsumer",
    "ConsumerPool",
    "create_eia_consumer",
    "create_weather_consumer",
]
