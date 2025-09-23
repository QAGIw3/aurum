from __future__ import annotations

"""Canonical ISO catalog utilities.

Purpose
-------
Provide a single source of truth for canonical ISO contract attributes across
ingestion, storage, and analytics. These helpers:

- Derive and stamp `iso_*` fields (iso_code, iso_market, iso_product, etc.) onto
  both series- and observation-level records using a consistent fallback order:
  explicit field → metadata hint → provider defaults → heuristic → UNKNOWN.
- Maintain an `iso_contract` object inside metadata for downstream consumers.
- Normalize metadata to an Avro-compatible map[str,string] (values stringified),
  preserving rich objects via JSON serialization when needed.

Why string-only metadata?
-------------------------
Kafka contracts model metadata as `map<string,string>` to keep schemas stable
and language-friendly. To remain compliant we stringify non-string values via
`json.dumps` so consumers can parse complex objects as needed while keeping
Avro types unchanged.

Usage
-----
- Adapters call `canonicalize_iso_observation_record` before emitting events.
- Series/catalog upserts go through `canonicalize_iso_series_record`.
- dbt models prefer top-level `iso_*` columns when present and fall back to
  metadata hints or provider defaults for historical rows.
"""

import json
from typing import Any, Dict, Mapping, MutableMapping, Optional

__all__ = [
    "canonicalize_iso_observation_record",
    "canonicalize_iso_series_record",
]

ISO_PROVIDER_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "iso.pjm": {
        "iso_code": "PJM",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "pjm": {
        "iso_code": "PJM",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "iso.isone": {
        "iso_code": "ISONE",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "isone": {
        "iso_code": "ISONE",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "iso.miso": {
        "iso_code": "MISO",
        "timezone": "America/Chicago",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "miso": {
        "iso_code": "MISO",
        "timezone": "America/Chicago",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "iso.ercot": {
        "iso_code": "ERCOT",
        "timezone": "America/Chicago",
        "interval_minutes": 15,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "ercot": {
        "iso_code": "ERCOT",
        "timezone": "America/Chicago",
        "interval_minutes": 15,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "iso.spp": {
        "iso_code": "SPP",
        "timezone": "America/Chicago",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "spp": {
        "iso_code": "SPP",
        "timezone": "America/Chicago",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "iso.nyiso": {
        "iso_code": "NYISO",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "ZONE",
    },
    "nyiso": {
        "iso_code": "NYISO",
        "timezone": "America/New_York",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "ZONE",
    },
    "iso.caiso": {
        "iso_code": "CAISO",
        "timezone": "America/Los_Angeles",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
    "caiso": {
        "iso_code": "CAISO",
        "timezone": "America/Los_Angeles",
        "interval_minutes": 60,
        "unit": "USD/MWh",
        "location_type": "NODE",
    },
}

ISO_SERIES_FIELDS = (
    "iso_code",
    "iso_market",
    "iso_product",
    "iso_location_type",
    "iso_location_id",
    "iso_timezone",
    "iso_interval_minutes",
    "iso_unit",
    "iso_subject",
    "iso_curve_role",
)

ISO_OBSERVATION_FIELDS = ISO_SERIES_FIELDS + (
    "iso_location_name",
)


def canonicalize_iso_series_record(provider: str, record: Mapping[str, Any]) -> Dict[str, Any]:
    """Ensure ISO catalog records expose canonical attributes."""
    normalized_provider = _normalize_provider(provider)
    defaults = ISO_PROVIDER_DEFAULTS.get(normalized_provider, {})

    out: Dict[str, Any] = dict(record)
    metadata_raw = _ensure_mapping(out.get("metadata"))

    iso_code = _coalesce(
        out.get("iso_code"),
        metadata_raw.get("iso_code"),
        defaults.get("iso_code"),
        _derive_iso_code(normalized_provider),
        default="UNKNOWN",
    )

    iso_market = _coalesce(
        out.get("iso_market"),
        metadata_raw.get("iso_market"),
        metadata_raw.get("market"),
        _market_from_series_id(out.get("series_id")),
        default="UNKNOWN",
    )

    iso_product = _coalesce(
        out.get("iso_product"),
        metadata_raw.get("iso_product"),
        metadata_raw.get("product"),
        _product_from_dataset(out.get("dataset_code")),
        default="UNKNOWN",
    )

    iso_location_type = _coalesce(
        out.get("iso_location_type"),
        metadata_raw.get("iso_location_type"),
        metadata_raw.get("location_type"),
        defaults.get("location_type"),
        default="UNKNOWN",
    )

    iso_location_id = _coalesce(
        out.get("iso_location_id"),
        metadata_raw.get("iso_location_id"),
        metadata_raw.get("location_id"),
        out.get("geo_id"),
        default="UNKNOWN",
    )

    iso_timezone = _coalesce(
        out.get("iso_timezone"),
        metadata_raw.get("iso_timezone"),
        defaults.get("timezone"),
        default="UTC",
    )

    iso_interval_minutes = _coalesce_int(
        out.get("iso_interval_minutes"),
        metadata_raw.get("iso_interval_minutes"),
        metadata_raw.get("interval_minutes"),
        defaults.get("interval_minutes"),
        default=60,
    )

    iso_unit = _coalesce(
        out.get("iso_unit"),
        metadata_raw.get("iso_unit"),
        metadata_raw.get("unit"),
        metadata_raw.get("unit_canonical"),
        out.get("unit_code"),
        defaults.get("unit"),
        default="UNKNOWN",
    )

    iso_subject = _coalesce(
        out.get("iso_subject"),
        metadata_raw.get("iso_subject"),
        metadata_raw.get("subject"),
        _subject_from_series_id(out.get("series_id")),
        default="UNKNOWN",
    )

    iso_curve_role = _coalesce(
        out.get("iso_curve_role"),
        metadata_raw.get("iso_curve_role"),
        metadata_raw.get("curve_role"),
        default="pricing",
    )

    iso_contract = _ensure_mapping(metadata_raw.get("iso_contract"))
    iso_contract.update(
        {
            "iso_code": iso_code,
            "market": iso_market,
            "product": iso_product,
            "location_type": iso_location_type,
            "location_id": iso_location_id,
            "timezone": iso_timezone,
            "interval_minutes": iso_interval_minutes,
            "unit": iso_unit,
            "subject": iso_subject,
            "curve_role": iso_curve_role,
        }
    )

    metadata_raw["iso_contract"] = iso_contract
    metadata = _stringify_metadata(metadata_raw)
    _assign_metadata(metadata, "iso_code", iso_code)
    _assign_metadata(metadata, "iso_market", iso_market)
    _assign_metadata(metadata, "iso_product", iso_product)
    _assign_metadata(metadata, "iso_location_type", iso_location_type)
    _assign_metadata(metadata, "iso_location_id", iso_location_id)
    _assign_metadata(metadata, "iso_timezone", iso_timezone)
    _assign_metadata(metadata, "iso_interval_minutes", iso_interval_minutes)
    _assign_metadata(metadata, "iso_unit", iso_unit)
    _assign_metadata(metadata, "iso_subject", iso_subject)
    _assign_metadata(metadata, "iso_curve_role", iso_curve_role)
    _assign_metadata(metadata, "iso_contract", iso_contract)
    out.update(
        {
            "metadata": metadata,
            "iso_contract": iso_contract,
            "iso_code": iso_code,
            "iso_market": iso_market,
            "iso_product": iso_product,
            "iso_location_type": iso_location_type,
            "iso_location_id": iso_location_id,
            "iso_timezone": iso_timezone,
            "iso_interval_minutes": iso_interval_minutes,
            "iso_unit": iso_unit,
            "iso_subject": iso_subject,
            "iso_curve_role": iso_curve_role,
        }
    )

    return out


def canonicalize_iso_observation_record(provider: str, record: Mapping[str, Any]) -> Dict[str, Any]:
    """Ensure ISO observation records expose canonical attributes."""
    series_contract = canonicalize_iso_series_record(provider, record)
    iso_location_name = _coalesce(
        record.get("location_name"),
        record.get("pnode_name"),
        series_contract["iso_contract"].get("location_name"),
        default="UNKNOWN",
    )

    series_contract["iso_location_name"] = iso_location_name
    series_contract["iso_contract"]["location_name"] = iso_location_name
    metadata = _stringify_metadata(series_contract.get("metadata") or {})
    _assign_metadata(metadata, "iso_location_name", iso_location_name)
    series_contract["metadata"] = metadata
    return series_contract


def _normalize_provider(provider: Optional[str]) -> str:
    return (provider or "").strip().lower()


def _coalesce(*values: Any, default: Any = None) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and value.strip() == "":
            continue
        return value
    return default


def _coalesce_int(*values: Any, default: int = 0) -> int:
    for value in values:
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return int(default)


def _ensure_mapping(value: Any) -> MutableMapping[str, Any]:
    if isinstance(value, MutableMapping):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value.items())
    return {}


def _derive_iso_code(provider: str) -> str:
    if provider.startswith("iso."):
        return provider.split(".", 1)[1].upper()
    if provider:
        return provider.upper()
    return "UNKNOWN"


def _market_from_series_id(series_id: Any) -> Optional[str]:
    if not isinstance(series_id, str):
        return None
    lowered = series_id.lower()
    if ".da" in lowered or "_da" in lowered:
        return "DA"
    if ".rt" in lowered or "_rt" in lowered:
        return "RT"
    return None


def _product_from_dataset(dataset_code: Any) -> Optional[str]:
    if not isinstance(dataset_code, str):
        return None
    lowered = dataset_code.lower()
    if "lmp" in lowered:
        return "LMP"
    if "load" in lowered:
        return "LOAD"
    if "genmix" in lowered or "generation" in lowered:
        return "GENERATION"
    if "congestion" in lowered:
        return "CONGESTION"
    return None


def _subject_from_series_id(series_id: Any) -> Optional[str]:
    if not isinstance(series_id, str):
        return None
    lowered = series_id.lower()
    for subject in ("lmp", "load", "genmix", "interchange", "ancillary", "congestion"):
        if subject in lowered:
            return subject.upper()
    return None


def _stringify_metadata(metadata: MutableMapping[str, Any]) -> MutableMapping[str, str]:
    out: Dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        out[key] = _stringify(value)
    return out


def _assign_metadata(metadata: MutableMapping[str, str], key: str, value: Any) -> None:
    if value is None:
        return
    metadata[key] = _stringify(value)


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)
