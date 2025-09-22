#!/usr/bin/env python3
"""Generate ISO ingestion configuration from ISO catalog."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import jsonschema

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.iso import IsoDataType


@dataclass
class IsoSource:
    """ISO source configuration."""

    name: str
    full_name: str
    region: str
    base_url: str
    api_key_required: bool = False
    description: str = ""
    data_types: List[str] = field(default_factory=list)
    markets: List[str] = field(default_factory=list)
    zones: List[str] = field(default_factory=list)
    rate_limits: Dict[str, int] = field(default_factory=dict)
    api_format: str = "json"
    authentication: str = "none"
    price_nodes_available: bool = False
    ancillary_services_available: bool = False

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "IsoSource":
        """Create IsoSource from dictionary."""
        return cls(
            name=data["name"],
            full_name=data["full_name"],
            region=data["region"],
            base_url=data["base_url"],
            api_key_required=data.get("api_key_required", False),
            description=data.get("description", ""),
            data_types=data.get("data_types", []),
            markets=data.get("markets", ["DAM", "RTM"]),
            zones=data.get("zones", []),
            rate_limits=data.get("rate_limits", {}),
            api_format=data.get("api_format", "json"),
            authentication=data.get("authentication", "none"),
            price_nodes_available=data.get("price_nodes_available", False),
            ancillary_services_available=data.get("ancillary_services_available", False)
        )


@dataclass
class GeneratedDataset:
    """Generated dataset configuration."""

    source_name: str
    iso_name: str
    data_type: str
    market: str
    description: str
    schedule: str
    topic_var: str
    default_topic: str
    frequency: str
    units_var: str
    default_units: str
    window_hours: Optional[int] = None
    window_days: Optional[int] = None
    window_months: Optional[int] = None
    window_years: Optional[int] = None
    dlq_topic: str = "aurum.ref.iso.dlq.v1"
    watermark_policy: str = "hour"


def _derive_schedule(data_type: str, market: str) -> str:
    """Derive cron schedule based on data type and market."""
    # Most ISO data is hourly
    if data_type in ["lmp", "load"]:
        if market == "RTM":
            return "0 */6 * * *"  # Every 6 hours for real-time data
        else:
            return "0 6 * * *"  # Daily at 6 AM for day-ahead data
    elif data_type == "generation_mix":
        return "0 8 * * *"  # Daily at 8 AM for generation mix
    else:
        return "0 6 * * *"  # Default daily schedule


def _derive_windowing(data_type: str, market: str) -> Dict[str, Optional[int]]:
    """Derive windowing configuration based on data type and market."""
    window_config = {
        "window_hours": None,
        "window_days": None,
        "window_months": None,
        "window_years": None
    }

    if data_type == "lmp":
        if market == "RTM":
            window_config["window_hours"] = 1  # 1-hour sliding window for RTM
        else:
            window_config["window_days"] = 1   # 1-day window for DAM
    elif data_type == "load":
        window_config["window_hours"] = 1      # 1-hour sliding window for load
    elif data_type == "generation_mix":
        window_config["window_days"] = 1       # 1-day window for generation mix

    return window_config


def _derive_watermark_policy(data_type: str, market: str) -> str:
    """Derive watermark policy based on data type and market."""
    if data_type == "lmp" and market == "RTM":
        return "hour"  # Real-time LMP data updates hourly
    elif data_type == "load":
        return "hour"  # Load data updates hourly
    else:
        return "day"   # Most other data updates daily


def _build_generated_entry(
    iso: IsoSource,
    data_type: str,
    market: str,
    *,
    default_schedule: str
) -> GeneratedDataset:
    """Build generated dataset entry from ISO source and data type."""
    # Create descriptive source name
    source_name = f"iso_{iso.name}_{data_type}_{market.lower()}"

    # Create topic name
    topic_var = f"aurum_iso_{iso.name}_{data_type}_{market.lower()}_topic"
    default_topic = f"aurum.ref.iso.{iso.name}.{data_type}.{market.lower()}.v1"

    # Derive configurations
    schedule = _derive_schedule(data_type, market)
    window_config = _derive_windowing(data_type, market)
    watermark_policy = _derive_watermark_policy(data_type, market)

    return GeneratedDataset(
        source_name=source_name,
        iso_name=iso.name,
        data_type=data_type,
        market=market,
        description=f"{iso.full_name} {data_type.upper()} data for {market} market",
        schedule=schedule,
        topic_var=topic_var,
        default_topic=default_topic,
        frequency="HOURLY" if data_type in ["lmp", "load"] else "DAILY",
        units_var=f"aurum_iso_{iso.name}_{data_type}_units",
        default_units="USD/MWh" if data_type == "lmp" else "MW",
        dlq_topic="aurum.ref.iso.dlq.v1",
        watermark_policy=watermark_policy,
        **window_config
    )


def _load_iso_catalog(catalog_path: Path) -> List[IsoSource]:
    """Load ISO catalog from JSON file."""
    try:
        with open(catalog_path, 'r', encoding='utf-8') as f:
            catalog_data = json.load(f)

        isos = []
        for iso_data in catalog_data.get("isos", []):
            iso = IsoSource.from_dict(iso_data)
            isos.append(iso)

        return isos

    except Exception as e:
        raise RuntimeError(f"Failed to load ISO catalog from {catalog_path}: {e}")


def _write_schema_file() -> None:
    """Write JSON schema file for validation."""
    schema_path = REPO_ROOT / "config" / "iso_ingest_datasets.schema.json"

    schema = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "properties": {
            "datasets": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["source_name", "iso_name", "data_type", "market", "schedule", "topic_var", "default_topic", "frequency"],
                    "properties": {
                        "source_name": {"type": "string", "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$"},
                        "iso_name": {"type": "string"},
                        "data_type": {"type": "string", "enum": ["lmp", "load", "generation_mix", "ancillary_services"]},
                        "market": {"type": "string", "enum": ["DAM", "RTM"]},
                        "description": {"type": "string"},
                        "schedule": {"type": "string", "pattern": r"^([0-9*,\-/ ]+|@yearly|@monthly|@weekly|@daily|@midnight|@noon)$"},
                        "topic_var": {"type": "string"},
                        "default_topic": {"type": "string"},
                        "frequency": {"type": "string", "enum": ["HOURLY", "DAILY"]},
                        "units_var": {"type": "string"},
                        "default_units": {"type": "string"},
                        "window_hours": {"type": ["integer", "null"], "minimum": 0},
                        "window_days": {"type": ["integer", "null"], "minimum": 0},
                        "window_months": {"type": ["integer", "null"], "minimum": 0},
                        "window_years": {"type": ["integer", "null"], "minimum": 0},
                        "dlq_topic": {"type": "string"},
                        "watermark_policy": {"type": "string", "enum": ["exact", "day", "hour", "month", "week"]}
                    }
                }
            }
        },
        "required": ["datasets"]
    }

    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with open(schema_path, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2)
        f.write('\n')


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration against JSON schema."""
    schema_path = REPO_ROOT / "config" / "iso_ingest_datasets.schema.json"

    if not schema_path.exists():
        _write_schema_file()

    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema = json.load(f)

        jsonschema.validate(config, schema)

    except jsonschema.ValidationError as e:
        raise RuntimeError(f"Configuration validation failed: {e.message}")
    except Exception as e:
        raise RuntimeError(f"Schema validation failed: {e}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate ISO ingestion configuration")
    parser.add_argument("--catalog", type=Path, default=REPO_ROOT / "config" / "iso_catalog.json",
                       help="ISO catalog file path")
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "config" / "iso_ingest_datasets.json",
                       help="Output configuration file path")
    parser.add_argument("--default-schedule", default="0 6 * * *",
                       help="Default schedule for datasets")
    args = parser.parse_args(argv)

    try:
        # Load ISO catalog
        isos = _load_iso_catalog(args.catalog)

        # Generate datasets for all ISOs and data types
        entries = []
        for iso in isos:
            for data_type in iso.data_types:
                for market in iso.markets:
                    entry = _build_generated_entry(
                        iso,
                        data_type,
                        market,
                        default_schedule=args.default_schedule
                    )
                    entries.append(entry.__dict__)

        # Create output payload
        payload = {"datasets": entries}

        # Validate configuration
        _validate_config(payload)

        # Write output
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
            f.write('\n')

        print(f"Generated {len(entries)} ISO dataset configurations")
        print(f"Output written to: {args.output}")

        # Print summary
        print("\n📊 Configuration Summary:")
        for iso in isos:
            iso_entries = [e for e in entries if e["iso_name"] == iso.name]
            print(f"  • {iso.name.upper()}: {len(iso_entries)} datasets")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
