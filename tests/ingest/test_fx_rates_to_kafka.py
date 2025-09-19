from datetime import datetime
from unittest import mock

import pytest


@pytest.fixture
def schema(tmp_path):
    from scripts.ingest import fx_rates_to_kafka as fx

    return fx._load_schema()


def test_publish_rates(monkeypatch, schema):
    from scripts.ingest import fx_rates_to_kafka as fx

    def fake_fetch(base, symbols):
        return datetime(2024, 1, 5), {sym: idx + 1.0 for idx, sym in enumerate(symbols)}

    monkeypatch.setattr(fx, "_fetch_rates", fake_fetch)

    sent_values = []

    class DummyProducer:
        def send(self, topic, value):
            sent_values.append((topic, value))

        def flush(self):
            pass

    count = fx.publish_rates(DummyProducer(), "fx.topic", schema, "EUR", ["USD", "GBP"], "test")
    assert count == 2
    assert len(sent_values) == 2


def test_main_smoke(monkeypatch):
    from scripts.ingest import fx_rates_to_kafka as fx

    monkeypatch.setattr(
        fx, "_fetch_rates", lambda base, symbols: (datetime(2024, 1, 5), {"USD": 1.1})
    )

    class DummyProducer:
        def __init__(self, *args, **kwargs):
            pass

        def send(self, topic, value):
            pass

        def flush(self):
            pass

    monkeypatch.setattr(fx, "KafkaProducer", DummyProducer)
    original_load = fx._load_schema
    monkeypatch.setattr(fx, "_load_schema", lambda: original_load())

    parser_args = [
        "--base",
        "EUR",
        "--symbols",
        "USD",
        "--topic",
        "aurum.ref.fx.rate.v1",
        "--bootstrap",
        "localhost:9092",
    ]

    monkeypatch.setattr("sys.argv", ["fx_rates_to_kafka.py", *parser_args])
    assert fx.main() == 0
