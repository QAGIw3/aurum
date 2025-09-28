"""Async Kafka processing primitives with observability and safety features."""
from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Deque, Dict, Mapping, MutableMapping, Optional

# Optional Prometheus instrumentation
try:  # pragma: no cover - optional dependency for metrics export
    from prometheus_client import Counter as _PromCounter, Gauge as _PromGauge, Histogram as _PromHistogram  # type: ignore
except Exception:  # pragma: no cover - library may be absent in some test envs
    _PromCounter = _PromGauge = _PromHistogram = None  # type: ignore[assignment]

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class KafkaProcessorConfig:
    """Configuration values for :class:`KafkaProcessor`."""

    bootstrap_servers: str | None = None
    group_id: str = "aurum-market-stream"
    input_topics: tuple[str, ...] = tuple()
    poll_interval: float = 1.0
    max_batch_size: int = 500
    enable_auto_commit: bool = True
    circuit_breaker_window: int = 50
    circuit_breaker_threshold: float = 0.3
    circuit_breaker_reset_after: float = 15.0
    backpressure_high_watermark: int = 5_000
    backpressure_low_watermark: int = 1_000
    backpressure_cooldown: float = 0.5
    in_memory: bool = True
    in_memory_max_queue: int = 10_000
    # Commit strategy (Kafka only): 'auto' (broker manages), 'sync' (after each message),
    # or 'batch' (commit periodically by size/interval)
    commit_strategy: str = "auto"
    commit_batch_size: int = 100
    commit_interval: float = 5.0  # seconds
    # Confluent/Schema Registry feature flag for Avro emission (producer-only)
    use_confluent_producer: bool = False
    schema_registry_url: str | None = None
    avro_value_subject: str | None = None
    # Confluent consumer (subject-aware Avro deserialization)
    use_confluent_consumer: bool = False
    schema_registry_basic_auth: str | None = None
    # Optional security/auth for real Kafka clusters
    security_protocol: str | None = None
    sasl_mechanism: str | None = None
    sasl_plain_username: str | None = None
    sasl_plain_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    client_id: str | None = None


@dataclass(frozen=True)
class KafkaMessage:
    """Representation of a Kafka message delivered to handlers."""

    topic: str
    value: Any
    key: str | bytes | None
    timestamp: datetime
    headers: Mapping[str, Any] = field(default_factory=dict)
    partition: int | None = None
    offset: int | None = None


class _InMemoryBroker:
    """Simple in-memory Kafka-like broker for testing and local development."""

    def __init__(self, *, max_queue: int) -> None:
        self._queues: MutableMapping[str, asyncio.Queue[KafkaMessage]] = defaultdict(
            lambda: asyncio.Queue(maxsize=max_queue)
        )

    async def publish(self, message: KafkaMessage) -> None:
        queue = self._queues[message.topic]
        await queue.put(message)

    async def consume(self, topic: str, timeout: float | None = None) -> KafkaMessage | None:
        queue = self._queues[topic]
        try:
            if timeout is None:
                return await queue.get()
            return await asyncio.wait_for(queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    def qsize(self, topic: str) -> int:
        return self._queues[topic].qsize()

    async def drain(self) -> None:
        for queue in self._queues.values():
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()


Handler = Callable[[KafkaMessage], Awaitable[None]]


@dataclass
class KafkaProcessorMetrics:
    """Runtime metrics captured during processing."""

    processed: int = 0
    failed: int = 0
    backpressure_events: int = 0
    circuit_open_events: int = 0
    last_error: str | None = None
    topic_counts: Dict[str, int] = field(default_factory=dict)


class KafkaProcessor:
    """High-level asyncio Kafka processor with health and safety tooling."""

    def __init__(
        self,
        config: KafkaProcessorConfig,
        *,
        consumer_factory: Callable[..., Any] | None = None,
        producer_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.config = config
        self._handlers: Dict[str, Handler] = {}
        self._tasks: list[asyncio.Task[None]] = []
        self._running = asyncio.Event()
        self._metrics = KafkaProcessorMetrics()
        self._outcome_window: Deque[bool] = deque(maxlen=config.circuit_breaker_window)
        self._breaker_open_until: datetime | None = None
        self._lock = asyncio.Lock()

        self._consumer = None
        self._producer = None
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._consumer_lock = asyncio.Lock()
        self._confluent_producer = None
        self._confluent_consumer = None
        self._confluent_consumer_lock = asyncio.Lock()
        self._schema_registry_client = None
        self._avro_deserializer = None

        if config.in_memory:
            self._broker: _InMemoryBroker | None = _InMemoryBroker(max_queue=config.in_memory_max_queue)
        else:
            self._broker = None

        self._loop = asyncio.get_event_loop()

        # Commit tracking (Kafka only)
        self._since_commit: int = 0
        self._last_commit_at: datetime | None = None

        # Prometheus metrics (no-op if client unavailable)
        self._m_proc = _PromCounter("aurum_kafka_messages_processed_total", "Processed Kafka messages", ["topic"]) if _PromCounter else None
        self._m_fail = _PromCounter("aurum_kafka_messages_failed_total", "Failed Kafka messages", ["topic"]) if _PromCounter else None
        self._m_bp = _PromCounter("aurum_kafka_backpressure_events_total", "Backpressure events", ["topic"]) if _PromCounter else None
        self._m_commit = _PromCounter("aurum_kafka_commits_total", "Kafka commits", ["strategy"]) if _PromCounter else None
        self._m_dur = _PromHistogram("aurum_kafka_handler_duration_seconds", "Handler duration", ["topic"]) if _PromHistogram else None
        self._m_qsize = _PromGauge("aurum_kafka_in_memory_queue_size", "In-memory broker queue size", ["topic"]) if _PromGauge else None

    @property
    def metrics(self) -> KafkaProcessorMetrics:
        return self._metrics

    def register_handler(self, topic: str, handler: Handler) -> None:
        """Register an async handler for the provided topic."""
        self._handlers[topic] = handler

    async def publish(
        self,
        topic: str,
        value: Any,
        *,
        key: str | bytes | None = None,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Publish a message to the configured broker.

        In in-memory mode, messages are queued locally. When using a real Kafka
        cluster, this delegates to the configured producer.
        """

        timestamp = datetime.now(timezone.utc)
        message = KafkaMessage(
            topic=topic,
            value=value,
            key=key,
            timestamp=timestamp,
            headers=dict(headers or {}),
        )

        if self._broker is not None:
            if self._broker.qsize(topic) >= self.config.backpressure_high_watermark:
                await self._apply_backpressure(topic)
            await self._broker.publish(message)
            if self._m_qsize:
                try:
                    self._m_qsize.labels(topic=topic).set(self._broker.qsize(topic))
                except Exception:
                    pass
            return

        # Confluent producer path (optional)
        if self.config.use_confluent_producer:
            producer = await self._ensure_confluent_producer()
            if producer is None:
                raise RuntimeError("Confluent Kafka producer is not available; disable confluent mode or provide configuration")
            # Derive Avro subject if provided, else default to topic-value
            subject = self.config.avro_value_subject or f"{topic}-value"
            try:
                norm_headers = dict(headers or {})
                producer.produce(
                    topic=topic,
                    value=value,
                    key=key if isinstance(key, (str, bytes, bytearray)) else str(key) if key is not None else None,
                    headers=norm_headers,
                    schema_subject=subject,
                )
            except Exception as exc:  # pragma: no cover - integration only
                self._register_failure(str(exc))
                LOGGER.exception("Confluent producer publish failed for topic '%s'", topic)
            else:
                self._register_success(topic)
            return

        # aiokafka producer path
        if not self._producer:
            producer = await self._ensure_producer()
        else:
            producer = self._producer

        if producer is None:
            raise RuntimeError("Kafka producer is not available; enable in-memory mode or provide a factory")

        payload = self._serialise(value)
        norm_headers = self._normalise_headers(headers or {})
        await producer.send_and_wait(topic, payload, key=key, headers=norm_headers)

    async def start(self) -> None:
        """Start background consumption for all registered topics."""
        if self._running.is_set():
            return
        self._running.set()

        if self._broker is None:
            if self.config.use_confluent_consumer:
                await self._ensure_confluent_consumer()
            else:
                await self._ensure_consumer()

        for topic in self._registered_topics:
            task = asyncio.create_task(self._consume_loop(topic), name=f"kafka-consumer-{topic}")
            self._tasks.append(task)

    async def stop(self) -> None:
        """Stop all background tasks and close resources."""
        if not self._running.is_set():
            return

        self._running.clear()

        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        if self._broker is not None:
            await self._broker.drain()

        if self._consumer:
            await self._consumer.stop()
            self._consumer = None
        if self._producer:
            await self._producer.stop()
            self._producer = None
        if self._confluent_consumer:
            try:
                self._confluent_consumer.close()
            except Exception:
                pass
            self._confluent_consumer = None

    @property
    def _registered_topics(self) -> tuple[str, ...]:
        if self.config.input_topics:
            return self.config.input_topics
        return tuple(self._handlers.keys())

    async def _consume_loop(self, topic: str) -> None:
        LOGGER.debug("Starting consumer loop for topic '%s'", topic)
        last_metrics_log = datetime.now(timezone.utc)
        try:
            while self._running.is_set():
                if self._breaker_open_until and datetime.now(timezone.utc) < self._breaker_open_until:
                    await asyncio.sleep(self.config.poll_interval)
                    continue

                message = await self._fetch_message(topic)
                if message is None:
                    await asyncio.sleep(self.config.poll_interval)
                    continue

                handler = self._handlers.get(topic)
                if handler is None:
                    LOGGER.debug("No handler registered for topic '%s'", topic)
                    continue

                try:
                    start = datetime.now(timezone.utc)
                    await handler(message)
                except Exception as exc:  # pragma: no cover - defensive
                    self._register_failure(str(exc))
                    LOGGER.exception("Handler failure for topic '%s'", topic)
                else:
                    self._register_success(topic)
                    if self._m_dur:
                        try:
                            dur = (datetime.now(timezone.utc) - start).total_seconds()
                            self._m_dur.labels(topic=topic).observe(dur)
                        except Exception:
                            pass
                    await self._maybe_commit_after_success()

                now = datetime.now(timezone.utc)
                if (now - last_metrics_log) >= timedelta(seconds=30):
                    LOGGER.debug(
                        "KafkaProcessor metrics: processed=%s failed=%s backpressure=%s circuit_open=%s",
                        self._metrics.processed,
                        self._metrics.failed,
                        self._metrics.backpressure_events,
                        self._metrics.circuit_open_events,
                    )
                    last_metrics_log = now
        except asyncio.CancelledError:
            LOGGER.debug("Consumer loop for topic '%s' cancelled", topic)
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("Consumer loop for topic '%s' terminated unexpectedly", topic)
        finally:
            LOGGER.debug("Consumer loop for topic '%s' stopped", topic)

    async def _fetch_message(self, topic: str) -> KafkaMessage | None:
        if self._broker is not None:
            return await self._broker.consume(topic, timeout=self.config.poll_interval)

        # Confluent consumer path
        if self.config.use_confluent_consumer:
            consumer = await self._ensure_confluent_consumer()
            if consumer is None:
                return None
            try:
                async with self._confluent_consumer_lock:
                    msg = consumer.poll(self.config.poll_interval)
            except Exception as exc:  # pragma: no cover - defensive
                self._register_failure(str(exc))
                return None
            if msg is None:
                return None
            if msg.error():  # pragma: no cover - integration
                self._register_failure(str(msg.error()))
                return None

            value_raw = msg.value()
            value: Any = None
            if value_raw is not None:
                # Try Avro then JSON
                if self._avro_deserializer is not None:
                    try:
                        value = self._avro_deserializer(value_raw, None)
                    except Exception:
                        value = None
                if value is None:
                    value = self._deserialise(value_raw)

            headers_list = msg.headers() or []
            headers: Dict[str, Any] = {}
            try:
                for k, v in headers_list:
                    headers[str(k)] = v
            except Exception:
                headers = {}

            ts = msg.timestamp()[1] if msg.timestamp() else None
            timestamp = datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc) if ts else datetime.now(timezone.utc)
            km = KafkaMessage(
                topic=msg.topic(),
                value=value,
                key=msg.key(),
                timestamp=timestamp,
                headers=headers,
                partition=msg.partition(),
                offset=msg.offset(),
            )
            if km.topic != topic:
                self._buffer_message(km)
                return None
            return km

        # aiokafka path
        if not self._consumer:
            await self._ensure_consumer()
        consumer = self._consumer
        if consumer is None:
            return None

        try:
            async with self._consumer_lock:
                result = await consumer.getone()
        except Exception as exc:  # pragma: no cover - defensive
            self._register_failure(str(exc))
            return None

        value = self._deserialise(result.value)
        headers = dict(result.headers or {})
        km = KafkaMessage(
            topic=result.topic,
            value=value,
            key=result.key,
            timestamp=datetime.fromtimestamp(result.timestamp / 1000.0, tz=timezone.utc),
            headers=headers,
            partition=result.partition,
            offset=result.offset,
        )
        if km.topic != topic:
            self._buffer_message(km)
            return None
        return km

    async def _ensure_consumer(self) -> Any:
        if self._consumer is not None:
            return self._consumer
        if self.config.in_memory:
            return None

        factory = self._consumer_factory
        if factory is None:
            try:  # pragma: no cover - optional dependency
                from aiokafka import AIOKafkaConsumer
            except ImportError as exc:
                LOGGER.warning("aiokafka not available: falling back to in-memory broker")
                self._broker = _InMemoryBroker(max_queue=self.config.in_memory_max_queue)
                return None
            factory = AIOKafkaConsumer

        kwargs = dict(
            bootstrap_servers=self.config.bootstrap_servers,
            group_id=self.config.group_id,
            # Force auto-commit only when strategy is 'auto'
            enable_auto_commit=(self.config.commit_strategy == "auto"),
        )
        # Attach security params if provided
        self._apply_security_kwargs(kwargs)
        consumer = factory(*self._registered_topics, **kwargs)
        await consumer.start()
        self._consumer = consumer
        return consumer

    async def _ensure_confluent_consumer(self) -> Any:
        if self._confluent_consumer is not None:
            return self._confluent_consumer
        if self.config.in_memory:
            return None
        try:  # pragma: no cover - optional dependency
            from confluent_kafka import Consumer as _CConsumer
            from confluent_kafka.schema_registry import SchemaRegistryClient as _SchemaRegistryClient
            from confluent_kafka.schema_registry.avro import AvroDeserializer as _AvroDeserializer
        except Exception as exc:
            LOGGER.warning("confluent_kafka not available: %s", exc)
            return None

        # Build consumer config
        consumer_conf: Dict[str, Any] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "group.id": self.config.group_id,
            "enable.auto.commit": (self.config.commit_strategy == "auto"),
            "auto.offset.reset": "earliest",
        }
        if self.config.client_id:
            consumer_conf["client.id"] = self.config.client_id

        # Map basic security settings (best effort)
        if self.config.security_protocol:
            consumer_conf["security.protocol"] = self.config.security_protocol
        if self.config.sasl_mechanism:
            consumer_conf["sasl.mechanisms"] = self.config.sasl_mechanism
        if self.config.sasl_plain_username is not None:
            consumer_conf["sasl.username"] = self.config.sasl_plain_username
        if self.config.sasl_plain_password is not None:
            consumer_conf["sasl.password"] = self.config.sasl_plain_password

        consumer = _CConsumer(consumer_conf)
        try:
            consumer.subscribe(list(self._registered_topics))
        except Exception as exc:  # pragma: no cover - integration only
            LOGGER.warning("Confluent consumer subscribe failed: %s", exc)
        self._confluent_consumer = consumer

        # Schema registry client for Avro deserialization (optional)
        if self.config.schema_registry_url:
            try:
                cfg = {"url": self.config.schema_registry_url}
                if self.config.schema_registry_basic_auth:
                    cfg["basic.auth.user.info"] = self.config.schema_registry_basic_auth
                self._schema_registry_client = _SchemaRegistryClient(cfg)
                self._avro_deserializer = _AvroDeserializer(self._schema_registry_client, None)
            except Exception as exc:  # pragma: no cover
                LOGGER.warning("Schema registry init failed: %s", exc)
                self._schema_registry_client = None
                self._avro_deserializer = None

        return self._confluent_consumer

    async def _ensure_producer(self) -> Any:
        if self._producer is not None:
            return self._producer
        if self.config.in_memory:
            return None

        factory = self._producer_factory
        if factory is None:
            try:  # pragma: no cover - optional dependency
                from aiokafka import AIOKafkaProducer
            except ImportError as exc:
                LOGGER.warning("aiokafka not available: falling back to in-memory broker")
                self._broker = _InMemoryBroker(max_queue=self.config.in_memory_max_queue)
                return None
            factory = AIOKafkaProducer

        kwargs = dict(bootstrap_servers=self.config.bootstrap_servers)
        self._apply_security_kwargs(kwargs)
        if self.config.client_id:
            kwargs["client_id"] = self.config.client_id
        producer = factory(**kwargs)
        await producer.start()
        self._producer = producer
        return producer

    async def _ensure_confluent_producer(self) -> Any:
        """Ensure a Confluent Kafka producer is available (producer-only Avro path)."""
        if self._confluent_producer is not None:
            return self._confluent_producer
        if self.config.in_memory:
            return None
        try:  # pragma: no cover - optional dependency
            from aurum.kafka.optimized_producer import OptimizedKafkaProducer
        except Exception as exc:
            LOGGER.warning("Confluent producer unavailable: %s", exc)
            return None

        try:
            self._confluent_producer = OptimizedKafkaProducer(
                bootstrap_servers=self.config.bootstrap_servers,
                schema_registry_url=self.config.schema_registry_url,
                client_id=self.config.client_id or "aurum-streaming",
            )
        except Exception as exc:  # pragma: no cover - integration only
            LOGGER.warning("Failed to construct Confluent producer: %s", exc)
            self._confluent_producer = None
        return self._confluent_producer

    async def _apply_backpressure(self, topic: str) -> None:
        self._metrics.backpressure_events += 1
        LOGGER.warning("Backpressure engaged for topic '%s'", topic)
        await asyncio.sleep(self.config.backpressure_cooldown)
        if self._m_bp:
            try:
                self._m_bp.labels(topic=topic).inc()
            except Exception:
                pass

    def _register_success(self, topic: str) -> None:
        self._metrics.processed += 1
        self._metrics.topic_counts[topic] = self._metrics.topic_counts.get(topic, 0) + 1
        self._outcome_window.append(True)
        if self._breaker_open_until and datetime.now(timezone.utc) >= self._breaker_open_until:
            LOGGER.info("Circuit breaker reset after cooldown")
            self._breaker_open_until = None
        if self._m_proc:
            try:
                self._m_proc.labels(topic=topic).inc()
            except Exception:
                pass

    def _register_failure(self, error: str) -> None:
        self._metrics.failed += 1
        self._metrics.last_error = error
        self._outcome_window.append(False)
        self._maybe_trip_circuit_breaker()
        if self._m_fail:
            try:
                # topic unknown here; aggregate without label
                self._m_fail.labels(topic="unknown").inc()
            except Exception:
                pass

    def _maybe_trip_circuit_breaker(self) -> None:
        if len(self._outcome_window) < self._outcome_window.maxlen:
            return
        failures = sum(1 for outcome in self._outcome_window if not outcome)
        ratio = failures / float(len(self._outcome_window))
        if ratio >= self.config.circuit_breaker_threshold:
            self._metrics.circuit_open_events += 1
            cooldown = timedelta(seconds=self.config.circuit_breaker_reset_after)
            self._breaker_open_until = datetime.now(timezone.utc) + cooldown
            LOGGER.error(
                "Circuit breaker opened after failure ratio %.2f (threshold %.2f)",
                ratio,
                self.config.circuit_breaker_threshold,
            )

    def _apply_security_kwargs(self, kwargs: Dict[str, Any]) -> None:
        if self.config.security_protocol:
            kwargs["security_protocol"] = self.config.security_protocol
        if self.config.sasl_mechanism:
            kwargs["sasl_mechanism"] = self.config.sasl_mechanism
        if self.config.sasl_plain_username is not None:
            kwargs["sasl_plain_username"] = self.config.sasl_plain_username
        if self.config.sasl_plain_password is not None:
            kwargs["sasl_plain_password"] = self.config.sasl_plain_password
        if self.config.ssl_cafile:
            kwargs["ssl_cafile"] = self.config.ssl_cafile
        if self.config.ssl_certfile:
            kwargs["ssl_certfile"] = self.config.ssl_certfile
        if self.config.ssl_keyfile:
            kwargs["ssl_keyfile"] = self.config.ssl_keyfile

    # --- Demux helpers ----------------------------------------------------------
    def _buffer_message(self, message: KafkaMessage) -> None:
        dq = self._demux_buffers[message.topic]
        # Drop oldest when exceeding max size
        if len(dq) >= max(1, self.config.demux_buffer_maxsize):
            try:
                dq.popleft()
            except Exception:
                pass
        dq.append(message)

    def _pop_buffered(self, topic: str) -> KafkaMessage | None:
        dq = self._demux_buffers.get(topic)
        if not dq:
            return None
        try:
            return dq.popleft()
        except Exception:
            return None

    @staticmethod
    def _serialise(value: Any) -> bytes:
        if isinstance(value, (bytes, bytearray)):
            return bytes(value)
        if isinstance(value, str):
            return value.encode("utf-8")
        return json.dumps(value).encode("utf-8")

    @staticmethod
    def _deserialise(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, (bytes, bytearray)):
            try:
                return json.loads(value.decode("utf-8"))
            except json.JSONDecodeError:
                return value.decode("utf-8")
        return value

    @staticmethod
    def _normalise_headers(headers: Mapping[str, Any]) -> list[tuple[str, bytes | None]]:
        norm: list[tuple[str, bytes | None]] = []
        for k, v in (headers or {}).items():
            if v is None:
                norm.append((str(k), None))
                continue
            if isinstance(v, (bytes, bytearray)):
                norm.append((str(k), bytes(v)))
            elif isinstance(v, str):
                norm.append((str(k), v.encode("utf-8")))
            else:
                try:
                    norm.append((str(k), json.dumps(v).encode("utf-8")))
                except Exception:
                    norm.append((str(k), str(v).encode("utf-8")))
        return norm

    async def _maybe_commit_after_success(self) -> None:
        """Apply commit policy after a successfully handled message (Kafka only)."""
        if self._broker is not None:
            return  # in-memory mode has no commits
        if self._consumer is None:
            return

        strategy = (self.config.commit_strategy or "auto").lower()
        if strategy == "auto":
            return  # broker auto-commit is enabled

        self._since_commit += 1
        now = datetime.now(timezone.utc)
        due_by_size = strategy == "sync" or (strategy == "batch" and self._since_commit >= max(1, self.config.commit_batch_size))
        due_by_time = strategy == "batch" and (
            self._last_commit_at is None or (now - self._last_commit_at).total_seconds() >= max(0.1, self.config.commit_interval)
        )

        if due_by_size or due_by_time:
            await self._commit_offsets(strategy)

    async def _commit_offsets(self, strategy_label: str) -> None:
        # aiokafka asynchronous commit
        if self._consumer is not None:
            try:
                async with self._consumer_lock:
                    await self._consumer.commit()
            except Exception as exc:  # pragma: no cover - integration only
                LOGGER.warning("Kafka commit failed: %s", exc)
                return
        # confluent synchronous commit
        elif self._confluent_consumer is not None:
            try:
                # Serialize access to the underlying client
                async with self._confluent_consumer_lock:
                    self._confluent_consumer.commit()
            except Exception as exc:  # pragma: no cover - integration only
                LOGGER.warning("Confluent commit failed: %s", exc)
                return
        else:
            return
        self._since_commit = 0
        self._last_commit_at = datetime.now(timezone.utc)
        if self._m_commit:
            try:
                self._m_commit.labels(strategy=strategy_label).inc()
            except Exception:
                pass


__all__ = [
    "KafkaProcessor",
    "KafkaProcessorConfig",
    "KafkaMessage",
]
