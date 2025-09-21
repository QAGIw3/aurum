#!/usr/bin/env python3
"""
Admin script for managing series-to-curve mappings for external data integration.

This script provides commands to:
- Add new mappings
- Update existing mappings
- List mappings by provider
- Find potential mappings for unmapped series
- Bulk import mappings from CSV
"""

import argparse
import asyncio
import csv
import sys
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import psycopg
from pydantic import BaseModel, Field


class SeriesCurveMapping(BaseModel):
    """Model for series-curve mapping records."""
    external_provider: str = Field(..., description="External data provider (FRED, EIA, etc.)")
    external_series_id: str = Field(..., description="Provider-specific series identifier")
    curve_key: str = Field(..., description="Internal curve key")
    mapping_confidence: float = Field(1.0, ge=0.0, le=1.0, description="Mapping confidence score")
    mapping_method: str = Field("manual", description="How the mapping was determined")
    mapping_notes: Optional[str] = Field(None, description="Human-readable mapping rationale")
    is_active: bool = Field(True, description="Whether this mapping is currently active")
    created_by: str = Field(..., description="User who created the mapping")


class MappingAdmin:
    """Admin class for managing series-curve mappings."""

    def __init__(self, db_url: str):
        self.db_url = db_url

    async def add_mapping(self, mapping: SeriesCurveMapping) -> str:
        """Add a new series-curve mapping."""
        async with await psycopg.AsyncConnection.connect(self.db_url) as conn:
            async with conn.cursor() as cur:
                try:
                    await cur.execute("""
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
                        mapping.created_by,
                        datetime.now(),
                        datetime.now()
                    ))
                    await conn.commit()
                    return f"Successfully added mapping: {mapping.external_provider}/{mapping.external_series_id} -> {mapping.curve_key}"
                except psycopg.IntegrityError as e:
                    await conn.rollback()
                    return f"Failed to add mapping: {e}"

    async def update_mapping(self, provider: str, series_id: str, updates: Dict) -> str:
        """Update an existing mapping."""
        async with await psycopg.AsyncConnection.connect(self.db_url) as conn:
            async with conn.cursor() as cur:
                try:
                    # Build dynamic update query
                    set_clauses = []
                    values = []
                    for key, value in updates.items():
                        if key == 'updated_by':
                            continue
                        set_clauses.append(f"{key} = %s")
                        values.append(value)

                    values.append(provider)
                    values.append(series_id)
                    values.append(datetime.now())

                    await cur.execute(f"""
                        UPDATE series_curve_map
                        SET {', '.join(set_clauses)}, updated_at = %s
                        WHERE external_provider = %s AND external_series_id = %s
                    """, values)

                    await conn.commit()
                    return f"Successfully updated mapping: {provider}/{series_id}"
                except Exception as e:
                    await conn.rollback()
                    return f"Failed to update mapping: {e}"

    async def list_mappings(self, provider: Optional[str] = None, active_only: bool = True) -> List[Dict]:
        """List mappings for a provider."""
        async with await psycopg.AsyncConnection.connect(self.db_url) as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT external_provider, external_series_id, curve_key,
                           mapping_confidence, mapping_method, mapping_notes,
                           is_active, created_by, created_at, updated_at
                    FROM series_curve_map
                    WHERE (%s IS NULL OR external_provider = %s)
                      AND (%s = FALSE OR is_active = TRUE)
                    ORDER BY external_provider, external_series_id
                """
                await cur.execute(query, (provider, provider, active_only))

                mappings = []
                async for row in cur:
                    mappings.append({
                        'provider': row[0],
                        'series_id': row[1],
                        'curve_key': row[2],
                        'confidence': float(row[3]),
                        'method': row[4],
                        'notes': row[5],
                        'active': row[6],
                        'created_by': row[7],
                        'created_at': row[8],
                        'updated_at': row[9]
                    })
                return mappings

    async def find_potential_mappings(self, provider: str, series_id: str, limit: int = 10) -> List[Dict]:
        """Find potential curve mappings for unmapped series."""
        async with await psycopg.AsyncConnection.connect(self.db_url) as conn:
            async with conn.cursor() as cur:
                await cur.execute("""
                    SELECT * FROM find_potential_series_mappings(%s, %s, %s)
                """, (provider, series_id, limit))

                potentials = []
                async for row in cur:
                    potentials.append({
                        'curve_key': row[0],
                        'similarity_score': float(row[1]),
                        'matching_criteria': row[2]
                    })
                return potentials

    async def bulk_import(self, csv_file: str, created_by: str) -> List[str]:
        """Bulk import mappings from CSV file."""
        results = []

        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    mapping = SeriesCurveMapping(
                        external_provider=row['external_provider'],
                        external_series_id=row['external_series_id'],
                        curve_key=row['curve_key'],
                        mapping_confidence=float(row.get('mapping_confidence', 1.0)),
                        mapping_method=row.get('mapping_method', 'manual'),
                        mapping_notes=row.get('mapping_notes'),
                        is_active=row.get('is_active', 'true').lower() == 'true',
                        created_by=created_by
                    )
                    result = await self.add_mapping(mapping)
                    results.append(result)
                except Exception as e:
                    results.append(f"Failed to import row {row}: {e}")

        return results


async def main():
    parser = argparse.ArgumentParser(description="Manage series-curve mappings")
    parser.add_argument('--db-url', required=True, help='Database connection URL')
    parser.add_argument('--created-by', default='admin', help='User creating mappings')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Add mapping command
    add_parser = subparsers.add_parser('add', help='Add a new mapping')
    add_parser.add_argument('provider', help='External provider')
    add_parser.add_argument('series_id', help='External series ID')
    add_parser.add_argument('curve_key', help='Internal curve key')
    add_parser.add_argument('--confidence', type=float, default=1.0, help='Mapping confidence')
    add_parser.add_argument('--method', default='manual', help='Mapping method')
    add_parser.add_argument('--notes', help='Mapping notes')

    # List mappings command
    list_parser = subparsers.add_parser('list', help='List mappings')
    list_parser.add_argument('--provider', help='Filter by provider')
    list_parser.add_argument('--all', action='store_true', help='Show inactive mappings too')

    # Find potential command
    find_parser = subparsers.add_parser('find', help='Find potential mappings')
    find_parser.add_argument('provider', help='External provider')
    find_parser.add_argument('series_id', help='External series ID')
    find_parser.add_argument('--limit', type=int, default=10, help='Maximum results')

    # Bulk import command
    bulk_parser = subparsers.add_parser('import', help='Bulk import from CSV')
    bulk_parser.add_argument('csv_file', help='CSV file to import')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    admin = MappingAdmin(args.db_url)

    if args.command == 'add':
        mapping = SeriesCurveMapping(
            external_provider=args.provider,
            external_series_id=args.series_id,
            curve_key=args.curve_key,
            mapping_confidence=args.confidence,
            mapping_method=args.method,
            mapping_notes=args.notes,
            created_by=args.created_by
        )
        result = await admin.add_mapping(mapping)
        print(result)

    elif args.command == 'list':
        mappings = await admin.list_mappings(args.provider, not args.all)
        for mapping in mappings:
            print(f"{mapping['provider']}/{mapping['series_id']} -> {mapping['curve_key']} "
                  f"(confidence: {mapping['confidence']}, active: {mapping['active']})")

    elif args.command == 'find':
        potentials = await admin.find_potential_mappings(args.provider, args.series_id, args.limit)
        for potential in potentials:
            print(f"Potential match: {potential['curve_key']} (score: {potential['similarity_score']})")

    elif args.command == 'import':
        results = await admin.bulk_import(args.csv_file, args.created_by)
        for result in results:
            print(result)


if __name__ == '__main__':
    asyncio.run(main())
