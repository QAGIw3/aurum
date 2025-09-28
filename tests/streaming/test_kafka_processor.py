import asyncio

import pytest

from aurum.streaming import KafkaProcessor, KafkaProcessorConfig


@pytest.mark.asyncio
async def test_kafka_processor_backpressure_and_circuit_breaker():
    config = KafkaProcessorConfig(
        in_memory=True,
        backpressure_high_watermark=0,
        backpressure_low_watermark=0,
        circuit_breaker_window=4,
        circuit_breaker_threshold=0.5,
        poll_interval=0.05,
    )
    processor = KafkaProcessor(config)
    processed: list[dict] = []

    async def handler(message):
        payload = message.value
        if payload.get("fail"):
            raise RuntimeError("synthetic failure")
        processed.append(payload)

    topic = "market.curves"
    processor.register_handler(topic, handler)

    await processor.start()

    try:
        await processor.publish(topic, {"curve_id": "TEST", "price": 10.0})
        await processor.publish(topic, {"curve_id": "TEST", "price": 11.0})
        await processor.publish(topic, {"curve_id": "TEST", "price": 12.0, "fail": True})
        await processor.publish(topic, {"curve_id": "TEST", "price": 13.0, "fail": True})

        # Allow the background consumer to process messages
        await asyncio.sleep(0.3)

        assert processed and all("fail" not in item for item in processed)
        metrics = processor.metrics
        assert metrics.failed >= 2
        assert metrics.circuit_open_events >= 1
        assert metrics.backpressure_events >= 1  # watermark ensures immediate backpressure
    finally:
        await processor.stop()
