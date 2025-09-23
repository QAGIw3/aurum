"""Async Kafka clients for scenario processing using aiokafka."""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncGenerator, Dict, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord


class AsyncKafkaMessage:
    """Wrapper for aiokafka ConsumerRecord to match expected interface."""

    def __init__(self, record: ConsumerRecord):
        self.topic = record.topic
        self.key = record.key.decode() if record.key else None
        self.value = record.value
        self.timestamp = record.timestamp

    def decode(self) -> str:
        """Decode message value as string."""
        return self.value.decode('utf-8') if self.value else ''


class AsyncKafkaProducer:
    """Async Kafka producer wrapper."""

    def __init__(self, bootstrap_servers: str, **kwargs):
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            **kwargs
        )
        self._started = False

    async def start(self) -> None:
        """Start the producer."""
        await self.producer.start()
        self._started = True

    async def stop(self) -> None:
        """Stop the producer."""
        await self.producer.stop()

    async def send_and_wait(
        self,
        topic: str,
        key: bytes,
        value: bytes,
        **kwargs
    ) -> None:
        """Send message and wait for delivery."""
        await self.producer.send_and_wait(topic, value, key=key, **kwargs)


class AsyncKafkaConsumer:
    """Async Kafka consumer wrapper."""

    def __init__(self, bootstrap_servers: str, group_id: str, **kwargs):
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            **kwargs
        )
        self._started = False

    async def start(self) -> None:
        """Start the consumer."""
        await self.consumer.start()
        self._started = True

    async def stop(self) -> None:
        """Stop the consumer."""
        await self.consumer.stop()

    async def commit(self) -> None:
        """Commit current offsets."""
        await self.consumer.commit()

    async def seek_to_beginning(self, *partitions) -> None:
        """Seek to beginning of partitions."""
        await self.consumer.seek_to_beginning(*partitions)

    async def seek_to_end(self, *partitions) -> None:
        """Seek to end of partitions."""
        await self.consumer.seek_to_end(*partitions)

    async def __aiter__(self) -> AsyncGenerator[AsyncKafkaMessage, None]:
        """Iterate over messages."""
        async for msg in self.consumer:
            yield AsyncKafkaMessage(msg)


def create_kafka_producer(bootstrap_servers: str, **kwargs) -> AsyncKafkaProducer:
    """Create an async Kafka producer."""
    return AsyncKafkaProducer(bootstrap_servers, **kwargs)


def create_kafka_consumer(
    bootstrap_servers: str,
    group_id: str,
    topics: list[str],
    **kwargs
) -> AsyncKafkaConsumer:
    """Create an async Kafka consumer."""
    consumer = AsyncKafkaConsumer(bootstrap_servers, group_id, **kwargs)
    consumer.consumer.subscribe(topics)
    return consumer
