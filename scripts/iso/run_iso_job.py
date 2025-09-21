#!/usr/bin/env python3
"""Run ISO data extraction jobs with SeaTunnel integration."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.iso import (
    IsoConfig,
    IsoDataType,
    NyisoExtractor,
    PjmExtractor,
    CaisoExtractor,
    MisoExtractor,
    SppExtractor,
    AesoExtractor
)


# ISO configurations
ISO_CONFIGS = {
    "nyiso": {
        "base_url": "http://mis.nyiso.com/public",
        "description": "New York Independent System Operator",
        "api_format": "csv"
    },
    "pjm": {
        "base_url": "https://api.pjm.com/api/v1",
        "api_key_required": True,
        "description": "PJM Interconnection",
        "api_format": "json"
    },
    "caiso": {
        "base_url": "http://oasis.caiso.com/oasisapi",
        "description": "California Independent System Operator",
        "api_format": "json"
    },
    "miso": {
        "base_url": "https://api.misoenergy.org",
        "description": "Midcontinent Independent System Operator",
        "api_format": "json"
    },
    "spp": {
        "base_url": "https://api.spp.org/api/v1",
        "description": "Southwest Power Pool",
        "api_format": "json"
    },
    "aeso": {
        "base_url": "https://api.aeso.ca/api/v1",
        "api_key_required": True,
        "description": "Alberta Electric System Operator",
        "api_format": "json"
    }
}


def create_iso_extractor(iso_name: str, api_key: Optional[str] = None) -> object:
    """Create ISO extractor instance."""
    if iso_name not in ISO_CONFIGS:
        raise ValueError(f"Unsupported ISO: {iso_name}")

    config_data = ISO_CONFIGS[iso_name]
    config = IsoConfig(
        base_url=config_data["base_url"],
        api_key=api_key,
        api_format=config_data.get("api_format", "json")
    )

    extractors = {
        "nyiso": NyisoExtractor,
        "pjm": PjmExtractor,
        "caiso": CaisoExtractor,
        "miso": MisoExtractor,
        "spp": SppExtractor,
        "aeso": AesoExtractor
    }

    return extractors[iso_name](config)


def run_iso_extraction(
    iso_name: str,
    data_types: List[str],
    start_date: str,
    end_date: str,
    markets: Optional[List[str]] = None,
    output_dir: Optional[Path] = None,
    api_key: Optional[str] = None
) -> bool:
    """Run ISO data extraction.

    Args:
        iso_name: ISO identifier
        data_types: List of data types to extract
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        markets: List of markets to extract (default: all supported)
        output_dir: Output directory for results
        api_key: API key for authentication

    Returns:
        True if extraction succeeded
    """
    try:
        print(f"🚀 Starting {iso_name.upper()} data extraction...")
        print(f"   Data types: {', '.join(data_types)}")
        print(f"   Date range: {start_date} to {end_date}")

        # Create extractor
        extractor = create_iso_extractor(iso_name, api_key)

        # Default markets if not specified
        if markets is None:
            markets = ["DAM", "RTM"]

        # Extract data
        all_results = {}

        for data_type in data_types:
            if data_type == "lmp":
                for market in markets:
                    print(f"   📊 Extracting LMP data for {market} market...")
                    data = extractor.get_lmp_data(start_date, end_date, market)
                    key = f"{data_type}_{market.lower()}"
                    all_results[key] = data
                    print(f"      ✅ Extracted {len(data)} LMP records")

            elif data_type == "load":
                print("   📊 Extracting load data...")
                data = extractor.get_load_data(start_date, end_date)
                all_results[data_type] = data
                print(f"      ✅ Extracted {len(data)} load records")

            elif data_type == "generation_mix":
                print("   📊 Extracting generation mix data...")
                data = extractor.get_generation_mix(start_date, end_date)
                all_results[data_type] = data
                print(f"      ✅ Extracted {len(data)} generation mix records")

        # Save results
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)

            for data_key, data in all_results.items():
                output_file = output_dir / f"{iso_name}_{data_key}_{start_date}_{end_date}.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2, default=str)
                print(f"   💾 Saved {len(data)} records to {output_file}")

        # Print summary
        total_records = sum(len(data) for data in all_results.values())
        print(f"✅ Extraction completed: {total_records} total records extracted")

        return True

    except Exception as e:
        print(f"❌ Extraction failed: {e}", file=sys.stderr)
        return False


def main() -> int:
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run ISO data extraction")
    parser.add_argument("--iso", required=True, choices=list(ISO_CONFIGS.keys()),
                       help="ISO to extract data from")
    parser.add_argument("--data-types", nargs="+",
                       default=["lmp", "load", "generation_mix"],
                       help="Data types to extract")
    parser.add_argument("--start-date", required=True,
                       help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end-date", required=True,
                       help="End date in YYYY-MM-DD format")
    parser.add_argument("--markets", nargs="+", default=None,
                       help="Markets to extract (default: all supported)")
    parser.add_argument("--output-dir", type=Path, default=None,
                       help="Output directory for results")
    parser.add_argument("--api-key", default=None,
                       help="API key for authentication")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be extracted without actually doing it")

    args = parser.parse_args()

    # Validate arguments
    if args.iso in ["pjm", "aeso"] and not args.api_key:
        print(f"❌ Error: {args.iso.upper()} requires an API key", file=sys.stderr)
        print("   Use --api-key to provide it", file=sys.stderr)
        return 1

    # Show configuration
    config = ISO_CONFIGS[args.iso]
    print(f"📋 ISO Configuration:")
    print(f"   ISO: {args.iso.upper()}")
    print(f"   Description: {config['description']}")
    print(f"   Base URL: {config['base_url']}")
    print(f"   API Format: {config.get('api_format', 'json')}")
    if config.get('api_key_required'):
        print(f"   API Key: {'✅ Required' if args.api_key else '❌ Missing'}")

    if args.dry_run:
        print("
🔍 Dry Run - Would extract:"        print(f"   Data types: {', '.join(args.data_types)}")
        print(f"   Markets: {args.markets or 'All supported'}")
        print(f"   Date range: {args.start_date} to {args.end_date}")
        return 0

    # Run extraction
    success = run_iso_extraction(
        iso_name=args.iso,
        data_types=args.data_types,
        start_date=args.start_date,
        end_date=args.end_date,
        markets=args.markets,
        output_dir=args.output_dir,
        api_key=args.api_key
    )

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
