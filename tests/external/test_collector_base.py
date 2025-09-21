
from __future__ import annotations

import json
from typing import Any, Dict, Iterable, Optional
from unittest.mock import Mock

import pytest
from requests import Response, Session
from requests.structures import CaseInsensitiveDict

from aurum.external.collect.base import (
    CollectorConfig,
    CollectorMetrics,
    ExternalCollector,
    HttpRequest,
    HttpRequestError,
    RateLimitConfig,
)


class DummyProducer:
    def __init__(self) -> None:
        self.calls: list[Dict[str, Any]] = []
        self.flush_calls = 0

    def produce(
        self,
        *,
        topic: str,
        value: Any,
        key: Optional[Any] = None,
        headers: Optional[Iterable[tuple[str, bytes]]] = None,
    ) -> None:
        self.calls.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "headers": list(headers or []),
            }
        )

    def flush(self) -> None:
        self.flush_calls += 1


class DummyEncoder:
    def encode(self, record: Dict[str, Any]) -> bytes:
        return f"encoded-{record['id']}".encode("ascii")


class FakeClock:
    def __init__(self) -> None:
        self.current = 0.0
        self.sleep_calls: list[float] = []

    def monotonic(self) -> float:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.sleep_calls.append(seconds)
        self.current += seconds


def make_response(status: int = 200, body: Optional[Dict[str, Any]] = None) -> Response:
    response = Response()
    response.status_code = status
    payload: bytes
    if body is None:
        payload = b""
    else:
        payload = json.dumps(body).encode("utf-8")
    response._content = payload  # type: ignore[attr-defined]
    response.headers = CaseInsensitiveDict({"Content-Type": "application/json"})
    response.url = "https://api.test/resource"
    return response


def test_request_success_records_metrics_and_headers() -> None:
    config = CollectorConfig(
        provider="test",
        base_url="https://api.test/",
        kafka_topic="topic",
        default_headers={"X-Default": "true"},
    )
    session = Mock(spec=Session)
    session.headers = CaseInsensitiveDict()
    session.request.return_value = make_response(status=200, body={"ok": True})
    metrics = CollectorMetrics("test")
    collector = ExternalCollector(config, session=session, metrics=metrics)

    request = HttpRequest(
        method="get",
        path="/resource",
        headers={"Authorization": "Bearer abc"},
    )

    response = collector.request(request)

    assert response.status_code == 200
    snapshot = metrics.snapshot()
    assert snapshot["http_requests_total"] == 1
    assert snapshot["http_success"] == 1
    session.request.assert_called_once()
    called_headers = session.request.call_args.kwargs["headers"]
    assert called_headers["Authorization"] == "Bearer abc"
    assert called_headers["X-Default"] == "true"


def test_request_retries_on_retryable_status() -> None:
    config = CollectorConfig(
        provider="test",
        base_url="https://api.test/",
        kafka_topic="topic",
    )
    session = Mock(spec=Session)
    session.headers = CaseInsensitiveDict()
    session.request.side_effect = [
        make_response(status=503),
        make_response(status=200, body={"ok": True}),
    ]
    metrics = CollectorMetrics("test")
    sleep_calls: list[float] = []

    def sleep_stub(seconds: float) -> None:
        sleep_calls.append(seconds)

    collector = ExternalCollector(
        config,
        session=session,
        metrics=metrics,
        sleep=sleep_stub,
    )

    response = collector.request(HttpRequest(method="GET", path="/retry"))

    assert response.status_code == 200
    snapshot = metrics.snapshot()
    assert snapshot["http_requests_total"] == 2
    assert snapshot["retry_backoff_events"] == [(1, config.retry.compute_backoff(1))]
    assert sleep_calls == [config.retry.compute_backoff(1)]


def test_request_raises_after_exhausting_retries() -> None:
    config = CollectorConfig(
        provider="test",
        base_url="https://api.test/",
        kafka_topic="topic",
    )
    session = Mock(spec=Session)
    session.headers = CaseInsensitiveDict()
    session.request.side_effect = [make_response(status=500) for _ in range(config.retry.max_attempts)]
    metrics = CollectorMetrics("test")

    collector = ExternalCollector(config, session=session, metrics=metrics)

    with pytest.raises(HttpRequestError) as exc:
        collector.request(HttpRequest(method="GET", path="/fail"))

    assert "status 500" in str(exc.value)
    snapshot = metrics.snapshot()
    assert snapshot["http_requests_total"] == config.retry.max_attempts
    assert snapshot["http_errors"] == config.retry.max_attempts


def test_emit_records_uses_encoder_and_flushes() -> None:
    config = CollectorConfig(
        provider="test",
        base_url="https://api.test/",
        kafka_topic="topic",
    )
    metrics = CollectorMetrics("test")
    producer = DummyProducer()
    collector = ExternalCollector(
        config,
        metrics=metrics,
        producer=producer,
        encoder=DummyEncoder(),
    )

    records = [{"id": 1}, {"id": 2}]

    emitted = collector.emit_records(
        records,
        key_fn=lambda record: {"id": record["id"]},
        headers_fn=lambda record: [("record-id", str(record["id"]))],
    )

    assert emitted == 2
    assert producer.flush_calls == 1
    assert len(producer.calls) == 2
    assert producer.calls[0]["value"] == b"encoded-1"
    assert producer.calls[0]["headers"] == [("record-id", b"1")]
    snapshot = metrics.snapshot()
    assert snapshot["records_emitted_total"] == 2
    assert snapshot["records_emitted_by_topic"] == {"topic": 2}


def test_rate_limiter_waits_and_records_metric() -> None:
    config = CollectorConfig(
        provider="test",
        base_url="https://api.test/",
        kafka_topic="topic",
        rate_limit=RateLimitConfig(rate=1.0, burst=1.0),
    )
    session = Mock(spec=Session)
    session.headers = CaseInsensitiveDict()
    session.request.side_effect = [make_response(status=200), make_response(status=200)]
    metrics = CollectorMetrics("test")
    clock = FakeClock()

    collector = ExternalCollector(
        config,
        session=session,
        metrics=metrics,
        sleep=clock.sleep,
        monotonic=clock.monotonic,
    )

    collector.request(HttpRequest(method="GET", path="/first"))
    collector.request(HttpRequest(method="GET", path="/second"))

    assert clock.sleep_calls == [pytest.approx(1.0, rel=1e-6)]
    snapshot = metrics.snapshot()
    waits = snapshot["rate_limit_wait_seconds"]
    assert len(waits) == 1
    assert waits[0] == pytest.approx(1.0, rel=1e-6)
