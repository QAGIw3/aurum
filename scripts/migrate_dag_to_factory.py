#!/usr/bin/env python3
"""Helper script to migrate old DAG files to factory pattern.

Usage:
    python scripts/migrate_dag_to_factory.py airflow/dags/ingest_eia_series_timescale.py

This will:
1. Analyze the DAG structure
2. Generate factory-based configuration
3. Show the migrated code
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, List, Optional


def analyze_dag_file(dag_path: Path) -> Dict[str, any]:
    """Analyze a DAG file and extract key information.
    
    Args:
        dag_path: Path to DAG file
        
    Returns:
        Dictionary with DAG info (dag_id, schedule, tasks, etc.)
    """
    content = dag_path.read_text()
    
    # Extract DAG ID
    dag_id_match = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', content)
    dag_id = dag_id_match.group(1) if dag_id_match else dag_path.stem
    
    # Extract schedule
    schedule_match = re.search(r'schedule(?:_interval)?\s*=\s*["\']([^"\']+)["\']', content)
    schedule = schedule_match.group(1) if schedule_match else "@daily"
    
    # Extract description
    desc_match = re.search(r'description\s*=\s*["\']([^"\']+)["\']', content)
    description = desc_match.group(1) if desc_match else f"Migrated DAG: {dag_id}"
    
    # Extract tags
    tags_match = re.search(r'tags\s*=\s*\[([^\]]+)\]', content)
    tags = []
    if tags_match:
        tags_str = tags_match.group(1)
        tags = [t.strip().strip('"\'') for t in tags_str.split(',')]
    
    # Identify source type
    source = "unknown"
    if "ingest_eia" in dag_path.name:
        source = "eia"
    elif "ingest_fred" in dag_path.name:
        source = "fred"
    elif "ingest_noaa" in dag_path.name:
        source = "noaa"
    elif "ingest_iso" in dag_path.name:
        source = "iso"
    elif "ingest_drought" in dag_path.name:
        source = "drought"
    
    # Identify dataset
    dataset = dag_path.stem.replace("ingest_", "").replace(f"{source}_", "")
    
    return {
        "dag_id": dag_id,
        "schedule": schedule,
        "description": description,
        "tags": tags,
        "source": source,
        "dataset": dataset,
        "original_file": str(dag_path)
    }


def generate_factory_config(dag_info: Dict) -> str:
    """Generate factory-based DAG configuration code.
    
    Args:
        dag_info: DAG information from analyze_dag_file
        
    Returns:
        Python code for factory-based configuration
    """
    source = dag_info["source"]
    dataset = dag_info["dataset"]
    dag_id = dag_info["dag_id"]
    schedule = dag_info["schedule"]
    description = dag_info["description"]
    tags = dag_info["tags"]
    
    code = f'''"""
Migrated DAG: {dag_id}
Original file: {dag_info["original_file"]}
"""

from datetime import datetime, timedelta
from aurum.airflow_factory.dag_templates import DataIngestionDagFactory, DagConfig, IngestionConfig

# Extraction function (implement based on original DAG logic)
def extract_{source}_{dataset}(**context):
    """Extract {source.upper()} {dataset} data."""
    from aurum.external.{source} import extract_data
    return extract_data(dataset="{dataset}")

# Configuration
dag_config = DagConfig(
    dag_id="{dag_id}",
    description="{description}",
    schedule="{schedule}",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={{
        "owner": "aurum-data",
        "depends_on_past": False,
        "email_on_failure": True,
        "retries": 3,
        "retry_delay": timedelta(minutes=10),
    }},
    tags={tags or ['ingestion', source, dataset]}
)

ingestion_config = IngestionConfig(
    source="{source}",
    dataset="{dataset}",
    extract_callable=extract_{source}_{dataset},
    transform_callable=transform_{source}_data,  # Implement if needed
    load_callable=load_to_iceberg,
    quality_check_callable=validate_quality
)

# Generate DAG
{dag_id} = DataIngestionDagFactory.create_ingestion_dag(
    dag_config,
    ingestion_config
)
'''
    
    return code


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate an Airflow DAG to factory pattern"
    )
    parser.add_argument(
        "dag_file",
        help="Path to DAG file to migrate"
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path (default: print to stdout)"
    )
    
    args = parser.parse_args()
    
    dag_path = Path(args.dag_file)
    
    if not dag_path.exists():
        print(f"❌ File not found: {dag_path}")
        return 1
    
    print(f"\nAnalyzing: {dag_path}")
    print("=" * 60)
    
    # Analyze DAG
    dag_info = analyze_dag_file(dag_path)
    
    print("\nExtracted information:")
    print(f"  DAG ID: {dag_info['dag_id']}")
    print(f"  Schedule: {dag_info['schedule']}")
    print(f"  Source: {dag_info['source']}")
    print(f"  Dataset: {dag_info['dataset']}")
    print(f"  Tags: {dag_info['tags']}")
    
    # Generate factory code
    print("\n" + "=" * 60)
    print("Generated Factory Configuration:")
    print("=" * 60)
    
    factory_code = generate_factory_config(dag_info)
    
    if args.output:
        output_path = Path(args.output)
        output_path.write_text(factory_code)
        print(f"\n✅ Saved to: {output_path}")
    else:
        print(factory_code)
    
    print("\n" + "=" * 60)
    print("Next steps:")
    print("  1. Review generated configuration")
    print("  2. Implement extract/transform/load functions")
    print("  3. Add to consolidated DAG file")
    print("  4. Test the new DAG")
    print("  5. Remove old DAG file")
    print("=" * 60)
    print()
    
    return 0


if __name__ == "__main__":
    exit(main())

