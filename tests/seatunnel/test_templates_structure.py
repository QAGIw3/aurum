from __future__ import annotations

from pathlib import Path


TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "seatunnel" / "jobs"


def test_all_kafka_sinks_have_plugin_input() -> None:
    for tmpl in TEMPLATES_DIR.glob("*.conf.tmpl"):
        text = tmpl.read_text(encoding="utf-8")
        if "sink {" in text:
            sink_slice = text[text.index("sink {") :]
            if "Kafka {" in sink_slice:
                assert "plugin_input" in sink_slice, f"sink Kafka missing plugin_input in {tmpl.name}"


def test_json_sources_have_schema_and_jsonpath() -> None:
    for tmpl in TEMPLATES_DIR.glob("*.conf.tmpl"):
        text = tmpl.read_text(encoding="utf-8")
        # Only apply to Http/LocalFile with JSON format
        if ("Http {" in text or "LocalFile {" in text) and 'format = "json"' in text:
            assert "schema" in text, f"JSON source missing schema in {tmpl.name}"
            assert "jsonpath" in text, f"JSON source missing jsonpath in {tmpl.name}"
