"""Optimized Kafka producer with batching, compression, and performance tuning."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Dict, List, Optional, Union

from confluent_kafka import Producer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from ..config import settings
from ..monitoring.metrics import get_metrics_client


class OptimizedKafkaProducer:
    """High-performance Kafka producer with optimized configuration."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        schema_registry_url: Optional[str] = None,
        client_id: Optional[str] = None,
        **producer_config
    ):
        """Initialize optimized Kafka producer."""
        self.bootstrap_servers = bootstrap_servers or settings.KAFKA_BOOTSTRAP_SERVERS
        self.schema_registry_url = schema_registry_url or settings.SCHEMA_REGISTRY_URL
        self.client_id = client_id or "aurum-optimized-producer"

        # Initialize components
        self.producer = None
        self.schema_registry_client = None
        self.avro_serializer = None
        self.metrics = get_metrics_client()

        # Performance tracking
        self.message_count = 0
        self.batch_count = 0
        self.last_flush_time = time.time()
        self.total_bytes = 0

        # Initialize producer with optimized config
        self._init_producer(producer_config)
        self._init_schema_registry()

        # Start performance monitoring
        self._start_monitoring()

    def _init_producer(self, additional_config: Dict[str, Any]):
        """Initialize producer with optimized configuration."""
        # Base configuration optimized for performance
        base_config = {
            # Client configuration
            'bootstrap.servers': self.bootstrap_servers,
            'client.id': self.client_id,

            # Reliability configuration
            'acks': 'all',  # Wait for all replicas
            'enable.idempotence': True,
            'max.in.flight.requests.per.connection': 5,

            # Batching and buffering optimization
            'batch.size': 32768,  # 32KB batches
            'linger.ms': 500,     # Wait 500ms for batching
            'buffer.memory': 67108864,  # 64MB buffer
            'compression.type': 'snappy',  # Fast compression

            # Timeout configuration
            'delivery.timeout.ms': 120000,    # 2 minutes
            'request.timeout.ms': 30000,      # 30 seconds
            'retry.backoff.ms': 100,          # 100ms backoff
            'retry.backoff.max.ms': 10000,    # Max 10 seconds

            # Performance optimization
            'send.buffer.bytes': 1048576,     # 1MB send buffer
            'receive.buffer.bytes': 1048576,  # 1MB receive buffer
            'socket.keepalive.enable': True,

            # Monitoring
            'statistics.interval.ms': 30000,  # Statistics every 30 seconds
            'log_level': 4,  # INFO level for debugging

            # Additional configuration from caller
            **additional_config
        }

        self.producer = Producer(base_config)

        # Set up delivery callback
        def delivery_callback(err, msg):
            if err:
                self.metrics.increment_counter("kafka_producer_delivery_errors")
                logging.error(f"Message delivery failed: {err}")
            else:
                self.message_count += 1
                self.total_bytes += len(msg.value()) if msg.value() else 0

        self.producer.poll = self._enhanced_poll
        self.delivery_callback = delivery_callback

    def _init_schema_registry(self):
        """Initialize Schema Registry client and serializers."""
        if self.schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({
                'url': self.schema_registry_url,
                'basic.auth.user.info': getattr(settings, 'SCHEMA_REGISTRY_AUTH', '')
            })

            # Create serializers
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                None  # Schema will be provided per message
            )

    def _enhanced_poll(self, timeout: float = 0):
        """Enhanced poll with monitoring."""
        start_time = time.time()
        result = self.producer.poll(timeout)
        poll_duration = time.time() - start_time

        # Track poll performance
        self.metrics.histogram("kafka_producer_poll_duration", poll_duration)

        return result

    def _start_monitoring(self):
        """Start background monitoring of producer performance."""
        asyncio.create_task(self._monitor_performance())

    async def _monitor_performance(self):
        """Background task to monitor producer performance."""
        while True:
            try:
                await asyncio.sleep(30)  # Report every 30 seconds

                # Get producer statistics
                stats = self.producer.stats

                if stats:
                    stats_data = json.loads(stats)

                    # Record key metrics
                    self.metrics.gauge("kafka_producer_message_count", self.message_count)
                    self.metrics.gauge("kafka_producer_batch_count", self.batch_count)
                    self.metrics.gauge("kafka_producer_total_bytes", self.total_bytes)

                    # Record queue and buffer status
                    if 'msg_cnt' in stats_data:
                        self.metrics.gauge("kafka_producer_queue_size", stats_data['msg_cnt'])

                    if 'tx' in stats_data:
                        self.metrics.gauge("kafka_producer_transmissions", stats_data['tx'])

                    if 'tx_bytes' in stats_data:
                        self.metrics.gauge("kafka_producer_bytes_transmitted", stats_data['tx_bytes'])

            except Exception as e:
                logging.error(f"Error in producer monitoring: {e}")

    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        schema_subject: Optional[str] = None,
        schema_version: Optional[str] = None
    ):
        """Produce a message with optimized batching and error handling."""

        def _produce_with_callback():
            """Internal produce method with callback."""
            try:
                # Serialize value if Avro serializer is available
                if self.avro_serializer and schema_subject:
                    context = SerializationContext(topic, MessageField.VALUE)
                    serialized_value = self.avro_serializer(
                        value,
                        context,
                        schema_subject=schema_subject,
                        schema_version=schema_version
                    )
                else:
                    serialized_value = json.dumps(value).encode('utf-8') if isinstance(value, dict) else value

                # Serialize key
                serialized_key = key.encode('utf-8') if isinstance(key, str) else key

                # Prepare headers
                kafka_headers = None
                if headers:
                    kafka_headers = [
                        (key, value.encode('utf-8') if isinstance(value, str) else str(value).encode('utf-8'))
                        for key, value in headers.items()
                    ]

                # Produce message
                self.producer.produce(
                    topic=topic,
                    value=serialized_value,
                    key=serialized_key,
                    partition=partition,
                    headers=kafka_headers,
                    callback=self.delivery_callback
                )

                # Track metrics
                self.metrics.increment_counter("kafka_producer_messages_produced")

            except Exception as e:
                self.metrics.increment_counter("kafka_producer_produce_errors")
                logging.error(f"Error producing message: {e}")
                raise

        _produce_with_callback()

    def flush(self, timeout: float = 30.0):
        """Flush pending messages with performance tracking."""
        start_time = time.time()

        try:
            # Poll to ensure delivery callbacks are called
            self.producer.poll(0)

            # Flush with timeout
            remaining = self.producer.flush(timeout)

            flush_duration = time.time() - start_time

            # Record metrics
            self.metrics.histogram("kafka_producer_flush_duration", flush_duration)
            self.metrics.gauge("kafka_producer_pending_messages", remaining)

            if remaining > 0:
                self.metrics.increment_counter("kafka_producer_flush_timeouts")
                logging.warning(f"Flush timeout: {remaining} messages still pending")

            return remaining

        except Exception as e:
            self.metrics.increment_counter("kafka_producer_flush_errors")
            logging.error(f"Error during flush: {e}")
            raise

    async def produce_batch(
        self,
        messages: List[Dict[str, Any]],
        topic: str,
        batch_size: int = 100,
        flush_interval: float = 5.0
    ):
        """Produce messages in optimized batches."""
        start_time = time.time()
        batch_start_time = start_time

        for i, message in enumerate(messages):
            # Extract message components
            value = message.get('value')
            key = message.get('key')
            partition = message.get('partition')
            headers = message.get('headers', {})
            schema_subject = message.get('schema_subject')
            schema_version = message.get('schema_version')

            # Add common headers
            headers.update({
                'producer_client_id': self.client_id,
                'batch_id': str(self.batch_count),
                'message_index': str(i),
                'timestamp': str(int(time.time() * 1000))
            })

            # Produce message
            self.produce(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                headers=headers,
                schema_subject=schema_subject,
                schema_version=schema_version
            )

            # Flush periodically or at batch boundaries
            if (i + 1) % batch_size == 0 or (time.time() - batch_start_time) >= flush_interval:
                remaining = self.flush(5.0)  # 5 second flush timeout
                batch_start_time = time.time()
                self.batch_count += 1

                # Record batch metrics
                batch_duration = time.time() - start_time
                self.metrics.histogram("kafka_producer_batch_duration", batch_duration)
                self.metrics.gauge("kafka_producer_batch_size", batch_size)

        # Final flush
        final_remaining = self.flush(10.0)  # 10 second final flush timeout
        total_duration = time.time() - start_time

        # Record final metrics
        self.metrics.histogram("kafka_producer_total_batch_duration", total_duration)
        self.metrics.gauge("kafka_producer_final_pending", final_remaining)

        return {
            "messages_produced": len(messages),
            "batches_created": self.batch_count,
            "total_duration": total_duration,
            "pending_messages": final_remaining,
            "throughput_msg_per_sec": len(messages) / total_duration if total_duration > 0 else 0
        }

    def create_topic_if_not_exists(self, topic: str, partitions: int = 3, replication_factor: int = 1):
        """Create topic if it doesn't exist."""
        admin_client = AdminClient({'bootstrap.servers': self.bootstrap_servers})

        # Check if topic exists
        existing_topics = admin_client.list_topics(timeout=10).topics

        if topic not in existing_topics:
            # Create topic
            new_topic = admin_client.create_topics([
                {
                    'topic': topic,
                    'num_partitions': partitions,
                    'replication_factor': replication_factor,
                    'config': {
                        'cleanup.policy': 'delete',
                        'retention.ms': '604800000',  # 7 days
                        'segment.ms': '86400000',     # 1 day segments
                        'segment.bytes': '1073741824', # 1GB segments
                        'compression.type': 'snappy',
                        'min.insync.replicas': '1'
                    }
                }
            ])

            for topic_name, future in new_topic.items():
                try:
                    future.result()  # Wait for creation
                    logging.info(f"Created topic: {topic_name}")
                except Exception as e:
                    logging.error(f"Failed to create topic {topic_name}: {e}")
                    raise

    def close(self):
        """Close the producer and clean up resources."""
        if self.producer:
            self.flush(30.0)  # Final flush with longer timeout
            self.producer = None

        logging.info(f"Producer {self.client_id} closed. Total messages: {self.message_count}")


class ProducerPool:
    """Pool of optimized Kafka producers for load balancing."""

    def __init__(self, pool_size: int = 3, **producer_config):
        """Initialize producer pool."""
        self.pool_size = pool_size
        self.producers = []
        self.current_index = 0

        # Create producers
        for i in range(pool_size):
            producer = OptimizedKafkaProducer(
                client_id=f"aurum-producer-{i}",
                **producer_config
            )
            self.producers.append(producer)

    def get_producer(self) -> OptimizedKafkaProducer:
        """Get next producer in round-robin fashion."""
        producer = self.producers[self.current_index]
        self.current_index = (self.current_index + 1) % self.pool_size
        return producer

    async def produce_to_topic(
        self,
        topic: str,
        messages: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Distribute messages across producer pool."""
        # Split messages among producers
        messages_per_producer = len(messages) // self.pool_size
        remainder = len(messages) % self.pool_size

        results = []
        start_index = 0

        for i, producer in enumerate(self.producers):
            # Calculate batch size for this producer
            producer_batch_size = messages_per_producer + (1 if i < remainder else 0)

            if producer_batch_size > 0:
                end_index = start_index + producer_batch_size
                producer_messages = messages[start_index:end_index]

                # Produce with this producer
                result = await producer.produce_batch(
                    producer_messages,
                    topic,
                    batch_size=batch_size
                )
                results.append(result)

                start_index = end_index

        # Aggregate results
        total_results = {
            "total_messages": sum(r["messages_produced"] for r in results),
            "total_batches": sum(r["batches_created"] for r in results),
            "max_duration": max(r["total_duration"] for r in results),
            "total_throughput": sum(r["throughput_msg_per_sec"] for r in results),
            "individual_results": results
        }

        return total_results

    def close_all(self):
        """Close all producers in the pool."""
        for producer in self.producers:
            producer.close()


# Convenience functions for common use cases
async def produce_eia_data(
    topic: str,
    records: List[Dict[str, Any]],
    producer_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Produce EIA data with optimized configuration."""
    if producer_config is None:
        producer_config = {}

    # EIA-specific optimizations
    producer_config.update({
        'batch.size': 16384,      # Smaller batches for EIA data
        'linger.ms': 300,         # Shorter linger for EIA
        'compression.type': 'lz4' # Fast compression for time series
    })

    producer = OptimizedKafkaProducer(**producer_config)

    try:
        # Add EIA-specific headers
        for record in records:
            record.setdefault('headers', {}).update({
                'data_source': 'eia',
                'data_type': 'time_series'
            })

        result = await producer.produce_batch(records, topic)
        return result

    finally:
        producer.close()


async def produce_weather_data(
    topic: str,
    records: List[Dict[str, Any]],
    producer_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Produce weather data with optimized configuration."""
    if producer_config is None:
        producer_config = {}

    # Weather-specific optimizations
    producer_config.update({
        'batch.size': 8192,       # Smaller batches for weather data
        'linger.ms': 1000,        # Longer linger for weather
        'compression.type': 'gzip' # Best compression for weather data
    })

    producer = OptimizedKafkaProducer(**producer_config)

    try:
        # Add weather-specific headers
        for record in records:
            record.setdefault('headers', {}).update({
                'data_source': 'noaa',
                'data_type': 'weather_observation'
            })

        result = await producer.produce_batch(records, topic)
        return result

    finally:
        producer.close()
