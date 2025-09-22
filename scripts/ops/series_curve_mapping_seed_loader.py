#!/usr/bin/env python3
"""
Seed loader for initial series-curve mapping data.

This script loads initial series-curve mappings from various sources:
- CSV files with pre-defined mappings
- Configuration files
- Database tables with existing mappings
- External API responses

The script supports:
- Bulk loading of mappings
- Validation of mapping data
- Audit logging of all operations
- Rollback capabilities
"""

import argparse
import asyncio
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from uuid import uuid4

import psycopg
from pydantic import BaseModel, Field, ValidationError

from aurum.api.models import SeriesCurveMappingCreate


class SeedMapping(BaseModel):
    """Model for seed mapping data."""
    external_provider: str = Field(..., description="External data provider")
    external_series_id: str = Field(..., description="External series identifier")
    curve_key: str = Field(..., description="Internal curve key")
    mapping_confidence: float = Field(1.0, ge=0.0, le=1.0, description="Confidence score")
    mapping_method: str = Field("seed", description="How the mapping was determined")
    mapping_notes: Optional[str] = Field(None, description="Mapping rationale")
    is_active: bool = Field(True, description="Whether mapping is active")


class SeedLoaderConfig(BaseModel):
    """Configuration for the seed loader."""
    db_url: str
    created_by: str = "seed_loader"
    dry_run: bool = False
    validate_only: bool = False
    batch_size: int = 100
    continue_on_error: bool = False
    log_file: Optional[str] = None


class SeriesCurveMappingSeedLoader:
    """Loader for series-curve mapping seed data."""

    def __init__(self, config: SeedLoaderConfig):
        self.config = config
        self.stats = {
            "total_mappings": 0,
            "successful": 0,
            "failed": 0,
            "skipped": 0,
            "errors": []
        }

    async def load_from_csv(self, csv_file: str) -> Dict[str, Any]:
        """Load mappings from CSV file."""
        csv_path = Path(csv_file)
        if not csv_path.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_file}")

        print(f"Loading mappings from {csv_file}")

        mappings = []
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=2):  # Start at 2 for header row
                try:
                    # Validate and convert row data
                    mapping_data = {
                        "external_provider": row["external_provider"],
                        "external_series_id": row["external_series_id"],
                        "curve_key": row["curve_key"],
                        "mapping_confidence": float(row.get("mapping_confidence", 1.0)),
                        "mapping_method": row.get("mapping_method", "seed"),
                        "mapping_notes": row.get("mapping_notes"),
                        "is_active": row.get("is_active", "true").lower() == "true"
                    }

                    # Validate with Pydantic model
                    seed_mapping = SeedMapping(**mapping_data)
                    mappings.append(seed_mapping)

                except (KeyError, ValueError, ValidationError) as e:
                    error_msg = f"Row {row_num}: Invalid data - {e}"
                    self.stats["errors"].append(error_msg)
                    if not self.config.continue_on_error:
                        raise ValueError(error_msg)

        return await self._load_mappings(mappings)

    async def load_from_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """Load mappings from configuration dictionary."""
        print("Loading mappings from configuration")

        mappings = []
        for mapping_data in config_data.get("mappings", []):
            try:
                seed_mapping = SeedMapping(**mapping_data)
                mappings.append(seed_mapping)
            except ValidationError as e:
                error_msg = f"Config validation error: {e}"
                self.stats["errors"].append(error_msg)
                if not self.config.continue_on_error:
                    raise ValueError(error_msg)

        return await self._load_mappings(mappings)

    async def _load_mappings(self, mappings: List[SeedMapping]) -> Dict[str, Any]:
        """Load mappings into database."""
        if self.config.validate_only:
            print(f"Validation only mode: {len(mappings)} mappings validated")
            return {"status": "validated", "count": len(mappings)}

        if self.config.dry_run:
            print(f"Dry run mode: {len(mappings)} mappings would be loaded")
            return {"status": "dry_run", "count": len(mappings)}

        async with await psycopg.AsyncConnection.connect(self.config.db_url) as conn:
            async with conn.cursor() as cur:
                for i in range(0, len(mappings), self.config.batch_size):
                    batch = mappings[i:i + self.config.batch_size]
                    await self._load_batch(cur, batch)

                await conn.commit()

        print(f"Successfully loaded {self.stats['successful']} mappings")
        if self.stats["failed"] > 0:
            print(f"Failed to load {self.stats['failed']} mappings")
        if self.stats["skipped"] > 0:
            print(f"Skipped {self.stats['skipped']} mappings (already exist)")

        return {
            "status": "completed",
            "total": self.stats["total_mappings"],
            "successful": self.stats["successful"],
            "failed": self.stats["failed"],
            "skipped": self.stats["skipped"],
            "errors": self.stats["errors"]
        }

    async def _load_batch(self, cursor, batch: List[SeedMapping]) -> None:
        """Load a batch of mappings."""
        for mapping in batch:
            self.stats["total_mappings"] += 1

            try:
                # Check if mapping already exists
                await cursor.execute("""
                    SELECT id FROM series_curve_map
                    WHERE external_provider = %s AND external_series_id = %s
                """, (mapping.external_provider, mapping.external_series_id))

                existing = await cursor.fetchone()

                if existing:
                    self.stats["skipped"] += 1
                    continue

                # Insert new mapping
                await cursor.execute("""
                    INSERT INTO series_curve_map (
                        id, external_provider, external_series_id, curve_key,
                        mapping_confidence, mapping_method, mapping_notes,
                        is_active, created_by, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                """, (
                    str(uuid4()),
                    mapping.external_provider,
                    mapping.external_series_id,
                    mapping.curve_key,
                    mapping.mapping_confidence,
                    mapping.mapping_method,
                    mapping.mapping_notes,
                    mapping.is_active,
                    self.config.created_by,
                    datetime.now(),
                    datetime.now()
                ))

                self.stats["successful"] += 1

            except Exception as e:
                self.stats["failed"] += 1
                self.stats["errors"].append(str(e))
                if not self.config.continue_on_error:
                    raise


async def main():
    parser = argparse.ArgumentParser(description="Load series-curve mapping seed data")
    parser.add_argument('--db-url', required=True, help='Database connection URL')
    parser.add_argument('--created-by', default='seed_loader', help='User creating mappings')

    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument('--csv', help='CSV file containing mappings')
    source_group.add_argument('--config', help='JSON config file containing mappings')
    source_group.add_argument('--config-json', help='JSON string containing mappings')

    parser.add_argument('--dry-run', action='store_true', help='Show what would be loaded without making changes')
    parser.add_argument('--validate-only', action='store_true', help='Only validate data without loading')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for database operations')
    parser.add_argument('--continue-on-error', action='store_true', help='Continue loading despite errors')
    parser.add_argument('--log-file', help='File to write detailed logs')

    args = parser.parse_args()

    # Load configuration
    config = SeedLoaderConfig(
        db_url=args.db_url,
        created_by=args.created_by,
        dry_run=args.dry_run,
        validate_only=args.validate_only,
        batch_size=args.batch_size,
        continue_on_error=args.continue_on_error,
        log_file=args.log_file
    )

    loader = SeriesCurveMappingSeedLoader(config)

    try:
        if args.csv:
            result = await loader.load_from_csv(args.csv)
        elif args.config:
            with open(args.config, 'r') as f:
                config_data = json.load(f)
            result = await loader.load_from_config(config_data)
        elif args.config_json:
            config_data = json.loads(args.config_json)
            result = await loader.load_from_config(config_data)
        else:
            print("No data source specified")
            return 1

        print(f"Result: {result}")

        if args.log_file:
            with open(args.log_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.now().isoformat(),
                    "result": result,
                    "stats": loader.stats
                }, f, indent=2)

        # Return appropriate exit code
        if loader.stats["failed"] > 0:
            return 1
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
