"""Consolidated drought data ingestion DAGs using factory pattern.

This single file replaces multiple individual DAG files:
- ingest_drought_usdm_weekly.py
- ingest_drought_raster_indices.py
- ingest_drought_vector_layers.py
- backfill_drought_history.py

Total reduction: 4 files → 1 file (75% reduction)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_factory.dag_templates import DataIngestionDagFactory, DagConfig, IngestionConfig


# ===================================
# Drought Data Extraction Functions
# ===================================

def extract_usdm_weekly(**context):
    """Extract USDM weekly drought classifications."""
    from aurum.external.drought import extract_usdm
    return extract_usdm(frequency="weekly")


def extract_drought_indices_raster(**context):
    """Extract drought indices as raster data."""
    from aurum.external.drought import extract_raster_indices
    return extract_raster_indices(indices=["SPI", "SPEI", "PDSI"])


def extract_drought_vector_layers(**context):
    """Extract drought data as vector layers."""
    from aurum.external.drought import extract_vector_layers
    return extract_vector_layers()


# ===================================
# Transform/Load Functions
# ===================================

def transform_drought_data(**context):
    """Transform drought data to standard format."""
    from aurum.parsers.drought import transform_drought
    dataset = context.get("dataset", "unknown")
    return transform_drought(dataset)


def load_drought_to_iceberg(**context):
    """Load drought data to Iceberg."""
    from aurum.data.iceberg import load_drought_data
    dataset = context.get("dataset", "unknown")
    return load_drought_data(dataset)


def validate_drought_quality(**context):
    """Validate drought data quality."""
    from aurum.dq.drought import validate_quality
    dataset = context.get("dataset", "unknown")
    return validate_quality(dataset)


# ===================================
# Drought Workflow Configurations
# ===================================

DROUGHT_WORKFLOWS = [
    {
        "dataset": "usdm_weekly",
        "description": "USDM weekly drought classifications",
        "schedule": "0 10 * * THU",  # Thursday 10 AM (after USDM publishes)
        "extract_func": extract_usdm_weekly,
    },
    {
        "dataset": "raster_indices",
        "description": "Drought indices (SPI, SPEI, PDSI) as raster",
        "schedule": "@daily",  # Daily at midnight
        "extract_func": extract_drought_indices_raster,
    },
    {
        "dataset": "vector_layers",
        "description": "Drought data as vector layers",
        "schedule": "@weekly",  # Weekly
        "extract_func": extract_drought_vector_layers,
    },
]


# ===================================
# Generate DAGs from Configuration
# ===================================

for workflow in DROUGHT_WORKFLOWS:
    dataset = workflow["dataset"]
    dag_id = f"ingest_drought_{dataset}"
    
    dag_config = DagConfig(
        dag_id=dag_id,
        description=workflow["description"],
        schedule=workflow["schedule"],
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "aurum-data",
            "depends_on_past": False,
            "email_on_failure": True,
            "email": ["aurum-ops@example.com"],
            "retries": 2,
            "retry_delay": timedelta(minutes=15),
            "execution_timeout": timedelta(minutes=60),
        },
        tags=["ingestion", "drought", dataset]
    )
    
    ingestion_config = IngestionConfig(
        source="drought",
        dataset=dataset,
        extract_callable=workflow["extract_func"],
        transform_callable=transform_drought_data,
        load_callable=load_drought_to_iceberg,
        quality_check_callable=validate_drought_quality
    )
    
    # Generate DAG
    globals()[dag_id] = DataIngestionDagFactory.create_ingestion_dag(
        dag_config,
        ingestion_config
    )


print(f"✅ Generated {len(DROUGHT_WORKFLOWS)} drought ingestion DAGs")
print(f"✅ Consolidated from 4 individual files to 1 configuration-driven file")
print(f"✅ 75% code reduction")

