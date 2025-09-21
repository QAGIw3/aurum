"""Utilities for consistent Avro subject naming and Kafka key generation across SeaTunnel templates."""

from __future__ import annotations

import re
from typing import Dict, Optional, Tuple


class SubjectNamingError(ValueError):
    """Raised when subject naming validation fails."""


def enforce_subject_naming_pattern(
    topic: str,
    subject_override: Optional[str] = None,
    entity_type: str = "value"
) -> str:
    """
    Enforce consistent Avro subject naming pattern.

    Args:
        topic: Kafka topic name
        subject_override: Optional subject override (e.g., from environment)
        entity_type: Schema entity type ("value" for value schema, "key" for key schema)

    Returns:
        Validated and normalized subject name

    Raises:
        SubjectNamingError: If subject naming pattern is invalid
    """
    if subject_override:
        # Validate custom subject
        if not _is_valid_subject_name(subject_override):
            raise SubjectNamingError(
                f"Invalid subject name: {subject_override}. "
                "Subject names must be alphanumeric with dots and hyphens only."
            )
        return subject_override

    # Generate subject from topic
    return f"{topic}-{entity_type}"


def _is_valid_subject_name(subject: str) -> bool:
    """Validate subject name format."""
    # Allow alphanumeric characters, dots, and hyphens
    pattern = r"^[a-zA-Z0-9._-]+$"
    return bool(re.match(pattern, subject))


def generate_key_fields(
    source_type: str,
    template_vars: Dict[str, str]
) -> Tuple[str, str]:
    """
    Generate appropriate key fields and format based on source type.

    Args:
        source_type: Type of data source (eia, fred, cpi, noaa, iso, etc.)
        template_vars: Template variables available for key generation

    Returns:
        Tuple of (key_expression, key_format)
    """
    # Default key strategies by source type
    key_strategies = {
        "eia": {
            "key_expr": "CONCAT(COALESCE(series_id, ''), '_', COALESCE(CAST(period AS STRING), ''))",
            "key_format": "string"
        },
        "fred": {
            "key_expr": "series_id",
            "key_format": "string"
        },
        "cpi": {
            "key_expr": "CONCAT(series_id, '_', COALESCE(area, 'US'))",
            "key_format": "string"
        },
        "noaa": {
            "key_expr": "CONCAT(station, '_', datatype, '_', date)",
            "key_format": "string"
        },
        "iso": {
            "key_expr": "CONCAT(iso_code, '_', location_id, '_', CAST(interval_start AS STRING))",
            "key_format": "string"
        }
    }

    strategy = key_strategies.get(source_type.lower(), {
        "key_expr": "'default_key'",
        "key_format": "string"
    })

    return strategy["key_expr"], strategy["key_format"]


def validate_topic_naming(topic: str) -> bool:
    """
    Validate Kafka topic naming convention.

    Args:
        topic: Topic name to validate

    Returns:
        True if topic follows naming convention
    """
    # Topic naming pattern: aurum.{domain}.{entity}.{version}
    pattern = r"^aurum\.[a-zA-Z0-9._-]+\.v\d+$"
    return bool(re.match(pattern, topic))


def suggest_key_schema_name(topic: str) -> str:
    """
    Suggest a key schema name based on topic.

    Args:
        topic: Kafka topic name

    Returns:
        Suggested key schema name
    """
    if not validate_topic_naming(topic):
        return f"{topic}-key"

    # Extract domain and entity from topic
    parts = topic.split(".")
    if len(parts) >= 3:
        domain = parts[1]
        entity = parts[2]
        return f"aurum.{domain}.{entity}.Key"

    return f"{topic}-key"


def get_default_subject_naming_config() -> Dict[str, str]:
    """Get default configuration for subject naming across all sources."""
    return {
        # EIA series
        "EIA_SUBJECT": "aurum.ref.eia.series.v1-value",
        "EIA_KEY_SUBJECT": "aurum.ref.eia.series.v1-key",

        # FRED series
        "FRED_SUBJECT": "aurum.ref.fred.series.v1-value",
        "FRED_KEY_SUBJECT": "aurum.ref.fred.series.v1-key",

        # CPI series
        "CPI_SUBJECT": "aurum.ref.cpi.series.v1-value",
        "CPI_KEY_SUBJECT": "aurum.ref.cpi.series.v1-key",

        # NOAA weather
        "NOAA_GHCND_SUBJECT": "aurum.ref.noaa.weather.v1-value",
        "NOAA_GHCND_KEY_SUBJECT": "aurum.ref.noaa.weather.v1-key",

        # ISO LMP
        "ISO_LMP_SUBJECT": "aurum.iso.lmp.v1-value",
        "ISO_LMP_KEY_SUBJECT": "aurum.iso.lmp.v1-key",

        # ISO Load
        "ISO_LOAD_SUBJECT": "aurum.iso.load.v1-value",
        "ISO_LOAD_KEY_SUBJECT": "aurum.iso.load.v1-key",

        # ISO Generation Mix
        "ISO_GENMIX_SUBJECT": "aurum.iso.genmix.v1-value",
        "ISO_GENMIX_KEY_SUBJECT": "aurum.iso.genmix.v1-key",
    }
