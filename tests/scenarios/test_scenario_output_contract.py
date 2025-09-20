from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import json
import fastavro


def _load_schema(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _days_since_epoch(d: date) -> int:
    return (d - date(1970, 1, 1)).days


def _ts_micros(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1_000_000)


def test_scenario_output_avro_contract() -> None:
    schema_path = Path("kafka/schemas/scenario.output.v1.avsc")
    assert schema_path.exists()
    schema = _load_schema(schema_path)

    record = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-a",
        "run_id": "run-123",
        "asof_date": _days_since_epoch(date(2025, 1, 1)),
        "curve_key": "demo-key",
        "tenor_type": "MONTHLY",
        "contract_month": None,
        "tenor_label": "2025-01",
        "metric": "mid",
        "value": 42.0,
        "band_lower": None,
        "band_upper": None,
        "attribution": None,
        "version_hash": "abc123",
        "computed_ts": _ts_micros(datetime(2025, 1, 1, 12, 0)),
    }

    # fastavro.validate returns bool in recent versions; use its validation method
    assert fastavro.validation.validate(record, schema)


def test_scenario_request_avro_contract() -> None:
    schema_path = Path("kafka/schemas/scenario.request.v1.avsc")
    assert schema_path.exists()
    schema = _load_schema(schema_path)

    record = {
        "scenario_id": "scn-1",
        "tenant_id": "tenant-a",
        "requested_by": "api",
        "asof_date": _days_since_epoch(date(2025, 1, 1)),
        "curve_def_ids": ["curve-a", "curve-b"],
        "assumptions": [
            {
                "assumption_id": "asm-1",
                "type": "policy",
                "payload": "{\"policy_name\": \"Clean\"}",
                "version": "v1",
            }
        ],
        "submitted_ts": _ts_micros(datetime(2025, 1, 1, 12, 0)),
    }

    assert fastavro.validation.validate(record, schema)


def test_scenario_output_expectation_suite_exists() -> None:
    suite_path = Path("ge/expectations/scenario_output.json")
    assert suite_path.exists()
    suite = _load_schema(suite_path)
    assert suite.get("expectation_suite_name") == "scenario_output"
    assert any(exp.get("kwargs", {}).get("column") == "scenario_id" for exp in suite.get("expectations", []))
