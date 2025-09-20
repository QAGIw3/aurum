"""Basic validation for Kafka Avro schemas."""
from __future__ import annotations

import json
from pathlib import Path

from datetime import date, datetime, timezone
from io import BytesIO

import pytest

fastavro = pytest.importorskip("fastavro")
fastavro_schema = pytest.importorskip("fastavro.schema")
from fastavro import parse_schema, reader, writer  # type: ignore[import]


SCHEMA_ROOT = Path(__file__).resolve().parents[2] / "kafka" / "schemas"


def _iter_schema_paths() -> list[Path]:
    return sorted(SCHEMA_ROOT.glob("*.avsc"))


def _load_schema_dict(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _find_named_type(node: object, name: str) -> dict:
    if isinstance(node, dict):
        if node.get("name") == name and node.get("type") in {"enum", "record", "fixed"}:
            return node
        for value in node.values():
            try:
                return _find_named_type(value, name)
            except LookupError:
                continue
    elif isinstance(node, list):
        for item in node:
            try:
                return _find_named_type(item, name)
            except LookupError:
                continue
    raise LookupError(name)


def _days_since_epoch(value: date) -> int:
    return (value - date(1970, 1, 1)).days


def _micros_since_epoch(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1_000_000)


def test_all_avro_schemas_parse() -> None:
    for schema_path in _iter_schema_paths():
        fastavro_schema.load_schema(schema_path)


def test_iso_lmp_schema_contains_expected_enums() -> None:
    schema = _load_schema_dict(SCHEMA_ROOT / "iso.lmp.v1.avsc")

    iso_enum = _find_named_type(schema, "IsoCode")
    expected_isos = {"PJM", "CAISO", "ERCOT", "NYISO", "MISO", "ISONE", "SPP"}
    assert expected_isos.issubset(set(iso_enum["symbols"]))

    market_enum = _find_named_type(schema, "IsoMarket")
    assert {"DAY_AHEAD", "REAL_TIME"}.issubset(set(market_enum["symbols"]))

    location_enum = _find_named_type(schema, "IsoLocationType")
    assert "OTHER" in set(location_enum["symbols"])


def test_reference_schema_namespaces() -> None:
    reference_files = {
      "noaa.weather.v1.avsc": "aurum.ref.noaa",
      "eia.series.v1.avsc": "aurum.ref.eia",
      "fred.series.v1.avsc": "aurum.ref.fred",
      "ingest.error.v1.avsc": "aurum.ingest",
      "cpi.series.v1.avsc": "aurum.ref.cpi",
      "fx.rate.v1.avsc": "aurum.ref.fx",
      "fuel.curve.v1.avsc": "aurum.ref.fuel",
    }

    for filename, expected_namespace in reference_files.items():
        schema = _load_schema_dict(SCHEMA_ROOT / filename)
        assert schema.get("namespace") == expected_namespace


def test_iso_schema_serialization_roundtrip() -> None:
    schema_path = SCHEMA_ROOT / "iso.lmp.v1.avsc"
    schema_dict = fastavro_schema.load_schema(schema_path)
    parsed_schema = parse_schema(schema_dict)

    delivery_date = _days_since_epoch(date(2024, 1, 1))
    interval_start_dt = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    record = {
        "iso_code": "PJM",
        "market": "DAY_AHEAD",
        "delivery_date": delivery_date,
        "interval_start": _micros_since_epoch(interval_start_dt),
        "interval_end": _micros_since_epoch(interval_start_dt.replace(hour=1)),
        "interval_minutes": 60,
        "location_id": "PJM123",
        "location_name": "Test Node",
        "location_type": "NODE",
        "zone": "PJM-RTO",
        "hub": "WEST_HUB",
        "timezone": "America/New_York",
        "price_total": 45.67,
        "price_energy": 40.0,
        "price_congestion": 3.5,
        "price_loss": 2.17,
        "currency": "USD",
        "uom": "MWh",
        "settlement_point": "RTO",
        "source_run_id": "run-001",
        "ingest_ts": _micros_since_epoch(datetime.now(timezone.utc)),
        "record_hash": "hash123",
        "metadata": {"source": "test"},
    }

    buffer = BytesIO()
    writer(buffer, parsed_schema, [record])
    buffer.seek(0)
    decoded = next(reader(buffer))
    assert decoded["iso_code"] == "PJM"
    assert decoded["price_total"] == pytest.approx(45.67)
    assert decoded["zone"] == "PJM-RTO"


def test_reference_schemas_serialization_roundtrip() -> None:
    now_ts = _micros_since_epoch(datetime.now(timezone.utc))

    schema_inputs = {
        "noaa.weather.v1.avsc": {
            "record": {
                "station_id": "GHCND:TEST",
                "station_name": "Test Station",
                "latitude": 40.0,
                "longitude": -75.0,
                "elevation_m": 10.0,
                "dataset": "GHCND",
                "date": _days_since_epoch(date(2024, 1, 2)),
                "element": "TMAX",
                "value": 23.4,
                "raw_value": "234",
                "unit": "degC",
                "observation_time": now_ts,
                "measurement_flag": "a",
                "quality_flag": None,
                "source_flag": "1",
                "attributes": {"attr": "value"},
                "ingest_ts": now_ts,
            },
            "assertions": lambda decoded: (
                decoded["station_id"] == "GHCND:TEST"
                and decoded["element"] == "TMAX"
                and isinstance(decoded["date"], date)
                and decoded["date"] == date(2024, 1, 2)
                and isinstance(decoded["ingest_ts"], datetime)
            ),
        },
        "eia.series.v1.avsc": {
            "record": {
                "series_id": "ELEC.GEN.ALL-US-99.A",
                "period": "2024-01",
                "period_start": now_ts,
                "period_end": now_ts,
                "frequency": "MONTHLY",
                "value": 123.45,
                "raw_value": "123.45",
                "unit": "MWh",
                "area": "US",
                "sector": "Electric",
                "seasonal_adjustment": "SA",
                "description": "Electric generation",
                "source": "EIA",
                "dataset": "ELEC",
                "metadata": {"note": "test"},
                "ingest_ts": now_ts,
            },
            "assertions": lambda decoded: (
                decoded["series_id"] == "ELEC.GEN.ALL-US-99.A"
                and decoded["frequency"] == "MONTHLY"
                and isinstance(decoded["value"], float)
                and isinstance(decoded["ingest_ts"], datetime)
            ),
        },
        "fred.series.v1.avsc": {
            "record": {
                "series_id": "DGS10",
                "date": _days_since_epoch(date(2024, 1, 3)),
                "frequency": "DAILY",
                "seasonal_adjustment": "NSA",
                "value": 4.56,
                "raw_value": "4.56",
                "units": "Percent",
                "title": "10-Year Treasury Constant Maturity Rate",
                "notes": "Example",
                "metadata": {"note": "test"},
                "ingest_ts": now_ts,
            },
            "assertions": lambda decoded: (
                decoded["series_id"] == "DGS10"
                and isinstance(decoded["date"], date)
                and decoded["frequency"] == "DAILY"
                and isinstance(decoded["ingest_ts"], datetime)
            ),
        },
        "fx.rate.v1.avsc": {
            "record": {
                "base_currency": "EUR",
                "quote_currency": "USD",
                "rate": 1.085,
                "source": "ECB",
                "as_of_date": _days_since_epoch(date(2024, 1, 4)),
                "ingest_ts": now_ts,
                "metadata": {"provider": "exchangerate.host"},
            },
            "assertions": lambda decoded: (
                decoded["base_currency"] == "EUR"
                and decoded["quote_currency"] == "USD"
                and isinstance(decoded["as_of_date"], date)
                and isinstance(decoded["ingest_ts"], datetime)
            ),
        },
        "cpi.series.v1.avsc": {
            "record": {
                "series_id": "CPIAUCSL",
                "area": "US",
                "frequency": "MONTHLY",
                "seasonal_adjustment": "SA",
                "period": "2024-02",
                "value": 304.5,
                "units": "Index",
                "source": "FRED",
                "ingest_ts": now_ts,
                "metadata": {"base_year": "1982-84"},
            },
            "assertions": lambda decoded: (
                decoded["series_id"] == "CPIAUCSL"
                and decoded["area"] == "US"
                and decoded["frequency"] == "MONTHLY"
                and decoded["seasonal_adjustment"] == "SA"
                and decoded["period"] == "2024-02"
                and isinstance(decoded["ingest_ts"], datetime)
            ),
        },
        "fuel.curve.v1.avsc": {
            "record": {
                "series_id": "NG.HUB.PRICE.D",
                "fuel_type": "NATURAL_GAS",
                "benchmark": "Henry Hub",
                "region": "US_Gulf",
                "frequency": "DAILY",
                "period": "2024-04-15",
                "value": 2.45,
                "units": "USD/MMBtu",
                "currency": "USD",
                "source": "EIA",
                "ingest_ts": now_ts,
                "metadata": {"dataset": "natural-gas/pri/fut"},
            },
            "assertions": lambda decoded: (
                decoded["fuel_type"] == "NATURAL_GAS"
                and decoded["benchmark"] == "Henry Hub"
                and decoded["period"] == "2024-04-15"
                and isinstance(decoded["ingest_ts"], datetime)
                and decoded["metadata"]["dataset"] == "natural-gas/pri/fut"
            ),
        },
    }

    for filename, config in schema_inputs.items():
        schema_dict = fastavro_schema.load_schema(SCHEMA_ROOT / filename)
        parsed_schema = parse_schema(schema_dict)
        buffer = BytesIO()
        writer(buffer, parsed_schema, [config["record"]])
        buffer.seek(0)
        decoded = next(reader(buffer))
        assert config["assertions"](decoded)


def test_pjm_reference_schemas_roundtrip() -> None:
    now = _micros_since_epoch(datetime.now(timezone.utc))
    # iso.load.v1
    load_schema = fastavro_schema.load_schema(SCHEMA_ROOT / "iso.load.v1.avsc")
    load_parsed = parse_schema(load_schema)
    load_buf = BytesIO()
    load_record = {
        "iso_code": "PJM",
        "area": "RTO",
        "interval_start": now,
        "interval_end": now + 3_600_000,
        "interval_minutes": 60,
        "mw": 55000.0,
        "ingest_ts": now,
        "metadata": {"source": "pjm"},
    }
    writer(load_buf, load_parsed, [load_record])
    load_buf.seek(0)
    decoded_load = next(reader(load_buf))
    assert decoded_load["iso_code"] == "PJM"
    assert decoded_load["mw"] == 55000.0

    # iso.genmix.v1
    genmix_schema = fastavro_schema.load_schema(SCHEMA_ROOT / "iso.genmix.v1.avsc")
    genmix_parsed = parse_schema(genmix_schema)
    genmix_buf = BytesIO()
    genmix_record = {
        "iso_code": "PJM",
        "asof_time": now,
        "fuel_type": "GAS",
        "mw": 23000.0,
        "unit": "MW",
        "ingest_ts": now,
        "metadata": {"note": "hourly"},
    }
    writer(genmix_buf, genmix_parsed, [genmix_record])
    genmix_buf.seek(0)
    decoded_genmix = next(reader(genmix_buf))
    assert decoded_genmix["fuel_type"] == "GAS"
    assert decoded_genmix["mw"] == 23000.0

    # iso.pnode.v1
    pnode_schema = fastavro_schema.load_schema(SCHEMA_ROOT / "iso.pnode.v1.avsc")
    pnode_parsed = parse_schema(pnode_schema)
    pnode_buf = BytesIO()
    pnode_record = {
        "iso_code": "PJM",
        "pnode_id": "51234",
        "pnode_name": "TEST_NODE",
        "type": "NODE",
        "zone": "PJM-RTO",
        "hub": "WEST_HUB",
        "effective_start": now,
        "effective_end": None,
        "ingest_ts": now,
    }
    writer(pnode_buf, pnode_parsed, [pnode_record])
    pnode_buf.seek(0)
    decoded_pnode = next(reader(pnode_buf))
    assert decoded_pnode["pnode_id"] == "51234"
    assert decoded_pnode["type"] == "NODE"
