#!/usr/bin/env python3
"""Generate FRED ingest configuration from catalog for Airflow DAGs."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

import jsonschema

# Schema for FRED ingest dataset configuration
INGEST_DATASET_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {
        "datasets": {
            "type": "array",
            "items": {
                "type": "object",
                "required": ["source_name", "series_id", "schedule", "topic_var", "default_topic", "frequency"],
                "properties": {
                    "source_name": {"type": "string", "pattern": "^[a-zA-Z][a-zA-Z0-9_]*$"},
                    "series_id": {"type": "string"},
                    "description": {"type": ["string", "null"]},
                    "schedule": {"type": "string", "pattern": "^([0-9*,\\-/ ]+|@yearly|@monthly|@weekly|@daily|@midnight|@noon)$"},
                    "topic_var": {"type": "string"},
                    "default_topic": {"type": "string"},
                    "frequency": {"type": "string", "enum": ["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "ANNUAL"]},
                    "units_var": {"type": "string"},
                    "default_units": {"type": "string"},
                    "seasonal_adjustment": {"type": ["string", "null"]},
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


@dataclass
class FredSeries:
    """FRED series metadata from catalog."""
    id: str
    title: str
    units: str
    frequency: str
    seasonal_adjustment: str
    last_updated: str
    popularity: int
    notes: str
    category: str
    start_date: str
    end_date: str
    api_path: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FredSeries":
        return cls(
            id=data["id"],
            title=data["title"],
            units=data["units"],
            frequency=data["frequency"],
            seasonal_adjustment=data["seasonal_adjustment"],
            last_updated=data["last_updated"],
            popularity=data["popularity"],
            notes=data["notes"],
            category=data["category"],
            start_date=data["start_date"],
            end_date=data["end_date"],
            api_path=data["api_path"]
        )


@dataclass
class GeneratedDataset:
    """Generated FRED ingest dataset configuration."""
    source_name: str
    series_id: str
    description: str
    schedule: str
    topic_var: str
    default_topic: str
    frequency: str
    units_var: str
    default_units: str
    seasonal_adjustment: str
    window_hours: Optional[int]
    window_days: Optional[int]
    window_months: Optional[int]
    window_years: Optional[int]
    dlq_topic: str
    watermark_policy: str


def _load_fred_catalog(catalog_path: Path) -> List[FredSeries]:
    """Load FRED catalog from JSON file."""
    try:
        with open(catalog_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        series = []
        for series_data in data.get("series", []):
            series.append(FredSeries.from_dict(series_data))

        return series
    except Exception as e:
        raise RuntimeError(f"Failed to load FRED catalog from {catalog_path}: {e}")


def _frequency_to_schedule(frequency: str) -> str:
    """Convert FRED frequency to cron schedule."""
    freq_mapping = {
        "Daily": "0 6 * * *",
        "Weekly": "0 6 * * 1",
        "Monthly": "0 6 1 * *",
        "Quarterly": "0 6 1 */3 *",
        "Annual": "0 6 1 1 *"
    }
    return freq_mapping.get(frequency, "0 6 * * *")


def _frequency_to_label(frequency: str) -> str:
    """Convert FRED frequency to label."""
    freq_mapping = {
        "Daily": "DAILY",
        "Weekly": "WEEKLY",
        "Monthly": "MONTHLY",
        "Quarterly": "QUARTERLY",
        "Annual": "ANNUAL"
    }
    return freq_mapping.get(frequency, "OTHER")


def _derive_watermark_policy(frequency: str) -> str:
    """Derive watermark policy based on series frequency."""
    freq = frequency.lower()
    if freq == "daily":
        return "day"
    if freq == "weekly":
        return "week"
    if freq == "monthly":
        return "month"
    if freq == "quarterly":
        return "month"  # Round to month boundary
    if freq == "annual":
        return "month"  # Round to month boundary
    return "exact"


def _build_generated_entry(series: FredSeries, *, default_schedule: str) -> GeneratedDataset:
    """Build generated dataset configuration from FRED series."""
    source_name = f"fred_{series.id.lower()}"
    frequency_label = _frequency_to_label(series.frequency)

    # Derive windowing based on frequency
    window_hours = None
    window_days = None
    window_months = None
    window_years = None

    if frequency_label == "DAILY":
        window_hours = 24
    elif frequency_label == "WEEKLY":
        window_days = 7
    elif frequency_label == "MONTHLY":
        window_months = 1
    elif frequency_label == "QUARTERLY":
        window_months = 3
    elif frequency_label == "ANNUAL":
        window_years = 1

    return GeneratedDataset(
        source_name=source_name,
        series_id=series.id,
        description=series.title,
        schedule=_frequency_to_schedule(series.frequency),
        topic_var=f"aurum_{source_name}_topic",
        default_topic=f"aurum.ref.fred.{series.id.lower()}.v1",
        frequency=frequency_label,
        units_var=f"aurum_{source_name}_units",
        default_units=series.units,
        seasonal_adjustment=series.seasonal_adjustment,
        window_hours=window_hours,
        window_days=window_days,
        window_months=window_months,
        window_years=window_years,
        dlq_topic="aurum.ref.fred.series.dlq.v1",
        watermark_policy=_derive_watermark_policy(series.frequency)
    )


def _validate_config(config: Dict[str, Any]) -> None:
    """Validate configuration against JSON schema."""
    try:
        jsonschema.validate(config, INGEST_DATASET_SCHEMA)
    except jsonschema.ValidationError as e:
        raise ValueError(f"Configuration validation failed: {e.message}")
    except Exception as e:
        # Handle other potential validation errors
        raise ValueError(f"Configuration validation failed: {str(e)}")


def _write_schema_file() -> None:
    """Write JSON schema file if it doesn't exist."""
    config_dir = REPO_ROOT / "config"
    config_dir.mkdir(exist_ok=True)  # Ensure config directory exists
    schema_path = config_dir / "fred_ingest_datasets.schema.json"
    if not schema_path.exists():
        with open(schema_path, 'w', encoding='utf-8') as f:
            json.dump(INGEST_DATASET_SCHEMA, f, indent=2)
        print(f"Created schema file: {schema_path}")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate FRED ingest configuration")
    parser.add_argument("--catalog", type=Path, default=REPO_ROOT / "config" / "fred_catalog.json",
                       help="Path to FRED catalog JSON file")
    parser.add_argument("--output", type=Path, default=REPO_ROOT / "config" / "fred_ingest_datasets.json",
                       help="Output path for generated configuration")
    parser.add_argument("--default-schedule", default="0 6 * * *",
                       help="Default cron schedule for series without frequency info")
    parser.add_argument("--export-vars", action="store_true",
                       help="Export Airflow variables to stdout")

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Main entry point."""
    args = parse_args(argv)

    _write_schema_file()

    try:
        # Load catalog
        series = _load_fred_catalog(args.catalog)
        print(f"Loaded {len(series)} FRED series from catalog")

        # Generate configurations
        entries = []
        for series in series:
            entries.append(_build_generated_entry(series, default_schedule=args.default_schedule))

        # Create output structure
        payload = {"datasets": [entry.__dict__ for entry in entries]}

        # Validate configuration
        _validate_config(payload)

        # Write configuration
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)
        print(f"Generated configuration for {len(entries)} datasets: {args.output}")

        # Export Airflow variables if requested
        if args.export_vars:
            print("\n# Airflow Variables")
            print("# Run this in your Airflow environment:")
            print("airflow variables set aurum_fred_catalog_url 'https://api.stlouisfed.org/fred'")
            print("airflow variables set aurum_fred_api_base 'https://api.stlouisfed.org/fred'")

            for entry in entries:
                topic_var = entry.topic_var
                units_var = entry.units_var
                default_topic = entry.default_topic
                default_units = entry.default_units

                print(f"airflow variables set {topic_var} '{default_topic}'")
                print(f"airflow variables set {units_var} '{default_units}'")

        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
