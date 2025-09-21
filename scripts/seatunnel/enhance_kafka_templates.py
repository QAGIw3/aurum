#!/usr/bin/env python3
"""Enhance SeaTunnel Kafka templates with key format parameterization and subject naming enforcement."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Dict, List

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE_DIR = REPO_ROOT / "seatunnel" / "jobs" / "templates"

# Template-specific key configurations
TEMPLATE_KEY_CONFIGS = {
    "eia_series_to_kafka.conf.tmpl": {
        "key_serializer_var": "EIA_KEY_SERIALIZER",
        "key_format_var": "EIA_KEY_FORMAT",
        "subject_var": "EIA_SUBJECT"
    },
    "noaa_ghcnd_to_kafka.conf.tmpl": {
        "key_serializer_var": "NOAA_GHCND_KEY_SERIALIZER",
        "key_format_var": "NOAA_GHCND_KEY_FORMAT",
        "subject_var": "NOAA_GHCND_SUBJECT"
    },
    "fred_series_to_kafka.conf.tmpl": {
        "key_serializer_var": "FRED_KEY_SERIALIZER",
        "key_format_var": "FRED_KEY_FORMAT",
        "subject_var": "FRED_SUBJECT"
    },
    "cpi_series_to_kafka.conf.tmpl": {
        "key_serializer_var": "CPI_KEY_SERIALIZER",
        "key_format_var": "CPI_KEY_FORMAT",
        "subject_var": "CPI_SUBJECT"
    },
}

# All other ISO and bulk templates
ISO_TEMPLATES = [
    "pjm_lmp_to_kafka.conf.tmpl",
    "pjm_load_to_kafka.conf.tmpl",
    "pjm_genmix_to_kafka.conf.tmpl",
    "pjm_pnodes_to_kafka.conf.tmpl",
    "miso_lmp_to_kafka.conf.tmpl",
    "miso_load_to_kafka.conf.tmpl",
    "miso_genmix_to_kafka.conf.tmpl",
    "miso_rtd_lmp_to_kafka.conf.tmpl",
    "miso_asm_to_kafka.conf.tmpl",
    "caiso_lmp_to_kafka.conf.tmpl",
    "caiso_load_to_kafka.conf.tmpl",
    "caiso_genmix_to_kafka.conf.tmpl",
    "aeso_lmp_to_kafka.conf.tmpl",
    "aeso_load_to_kafka.conf.tmpl",
    "aeso_genmix_to_kafka.conf.tmpl",
    "nyiso_lmp_to_kafka.conf.tmpl",
    "isone_lmp_to_kafka.conf.tmpl",
    "spp_lmp_to_kafka.conf.tmpl",
    "spp_load_to_kafka.conf.tmpl",
    "spp_genmix_to_kafka.conf.tmpl",
    "ercot_lmp_to_kafka.conf.tmpl",
    "eia_fuel_curve_to_kafka.conf.tmpl",
    "eia_bulk_to_kafka.conf.tmpl"
]

def enhance_kafka_template(template_path: Path, config: Dict[str, str]) -> bool:
    """Enhance a single Kafka template with key configuration."""
    content = template_path.read_text()

    # Check if already enhanced
    if "key.serializer =" in content:
        print(f"Skipping {template_path} - already enhanced")
        return False

    # Add key configuration to avro block
    def enhance_avro_block(match):
        block = match.group(0)

        # Add key configuration after value.schema
        enhanced_block = re.sub(
            r'(value\.schema = """[^"]*""")',
            r'\1\n      # Key configuration for message routing and partitioning\n      key.serializer = "${' + config["key_serializer_var"] + r':-string}"\n      key.format = "${' + config["key_format_var"] + r':-json}"',
            block
        )

        return enhanced_block

    # Enhance avro blocks
    content = re.sub(r'(avro \{(?:[^{}]|{(?:[^{}]|{[^{}]*})*})*\})', enhance_avro_block, content, flags=re.MULTILINE | re.DOTALL)

    # Add documentation for new variables
    def add_key_docs(match):
        docs = match.group(0)
        key_serializer_var = config["key_serializer_var"]
        key_format_var = config["key_format_var"]

        # Add documentation before the env block
        docs_with_key = re.sub(
            r'(# Optional environment variables:\n)',
            rf'\1#   {key_serializer_var} - Kafka key serializer (string/json/avro, default: string)\n#   {key_format_var} - Kafka key format (json/avro, default: json)\n#\n',
            docs
        )

        return docs_with_key

    # Add key documentation
    content = re.sub(r'(# Optional environment variables:\n.*?)(\nenv \{)', add_key_docs, content, flags=re.DOTALL)

    # Write back enhanced content
    template_path.write_text(content)
    print(f"Enhanced {template_path}")
    return True

def enhance_iso_template(template_path: Path, iso_name: str) -> bool:
    """Enhance ISO template with key configuration."""
    content = template_path.read_text()

    # Check if already enhanced
    if "key.serializer =" in content:
        print(f"Skipping {template_path} - already enhanced")
        return False

    # Add key configuration to avro block
    def enhance_avro_block(match):
        block = match.group(0)

        # Add key configuration after value.schema
        enhanced_block = re.sub(
            r'(value\.schema = """[^"]*""")',
            r'\1\n      # Key configuration for message routing and partitioning\n      key.serializer = "${' + iso_name.upper() + r'_KEY_SERIALIZER:-string}"\n      key.format = "${' + iso_name.upper() + r'_KEY_FORMAT:-json}"',
            block
        )

        return enhanced_block

    # Enhance avro blocks
    content = re.sub(r'(avro \{(?:[^{}]|{(?:[^{}]|{[^{}]*})*})*\})', enhance_avro_block, content, flags=re.MULTILINE | re.DOTALL)

    # Add documentation for new variables
    def add_key_docs(match):
        docs = match.group(0)
        key_serializer_var = f"{iso_name.upper()}_KEY_SERIALIZER"
        key_format_var = f"{iso_name.upper()}_KEY_FORMAT"

        # Add documentation before the env block
        docs_with_key = re.sub(
            r'(# Optional environment variables:\n)',
            rf'\1#   {key_serializer_var} - Kafka key serializer (string/json/avro, default: string)\n#   {key_format_var} - Kafka key format (json/avro, default: json)\n#\n',
            docs
        )

        return docs_with_key

    # Add key documentation
    content = re.sub(r'(# Optional environment variables:\n.*?)(\nenv \{)', add_key_docs, content, flags=re.DOTALL)

    # Write back enhanced content
    template_path.write_text(content)
    print(f"Enhanced {template_path}")
    return True

def main() -> int:
    if not TEMPLATE_DIR.exists():
        print(f"Template directory not found: {TEMPLATE_DIR}", file=sys.stderr)
        return 1

    enhanced_count = 0

    # Enhance specific templates
    for template_name, config in TEMPLATE_KEY_CONFIGS.items():
        template_path = TEMPLATE_DIR / template_name
        if template_path.exists():
            if enhance_kafka_template(template_path, config):
                enhanced_count += 1

    # Enhance ISO templates
    for iso_template in ISO_TEMPLATES:
        template_path = TEMPLATE_DIR / iso_template
        if template_path.exists():
            iso_name = template_path.stem.replace("_to_kafka", "").replace("_lmp", "").replace("_load", "").replace("_genmix", "").replace("_pnodes", "").replace("_rtd", "").replace("_asm", "").replace("_fuel_curve", "").replace("_bulk", "")
            if enhance_iso_template(template_path, iso_name):
                enhanced_count += 1

    print(f"Enhanced {enhanced_count} Kafka templates")
    return 0

if __name__ == "__main__":
    sys.exit(main())
