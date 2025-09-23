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


def test_iso_registry_sources_have_correct_schema() -> None:
    """Test that LocalFile sources loading ISO registry have correct schema."""
    expected_fields = [
        "iso",
        "location_id", 
        "location_name",
        "location_type",
        "zone",
        "hub",
        "timezone"
    ]
    
    for tmpl in TEMPLATES_DIR.glob("*.conf.tmpl"):
        text = tmpl.read_text(encoding="utf-8")
        if "ISO_LOCATION_REGISTRY" in text and "LocalFile {" in text:
            # Find the LocalFile block that loads the registry
            registry_source_found = False
            in_registry_source = False
            lines = text.split('\n')
            
            for i, line in enumerate(lines):
                if "path = \"${ISO_LOCATION_REGISTRY}\"" in line:
                    registry_source_found = True
                    in_registry_source = True
                    # Check if schema is defined
                    schema_found = False
                    for j in range(i, min(i + 20, len(lines))):
                        if "schema {" in lines[j]:
                            schema_found = True
                            # Check for expected fields
                            for field in expected_fields:
                                field_found = False
                                for k in range(j, min(j + 30, len(lines))):
                                    if field in lines[k]:
                                        field_found = True
                                        break
                                assert field_found, f"Missing field '{field}' in ISO registry schema in {tmpl.name}"
                            break
                    assert schema_found, f"ISO registry source missing schema in {tmpl.name}"
                    break
            
            assert registry_source_found, f"Template {tmpl.name} references ISO_LOCATION_REGISTRY but doesn't load it"
