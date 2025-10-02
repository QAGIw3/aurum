"""Example of consolidated ingestion DAGs using templates.

This demonstrates how to replace 50+ individual DAG files with
a single file using the DAG factory pattern.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from aurum.airflow_factory.dag_templates import (
    DataIngestionDagFactory,
    DagConfig,
    IngestionConfig
)


# ===================================
# Extraction Functions
# ===================================

def extract_eia_data(**context):
    """Extract EIA data."""
    dataset = context.get("dataset", "unknown")
    print(f"Extracting EIA data for {dataset}")
    return {"status": "extracted", "records": 1000}


def extract_fred_data(**context):
    """Extract FRED data."""
    dataset = context.get("dataset", "unknown")
    print(f"Extracting FRED data for {dataset}")
    return {"status": "extracted", "records": 500}


def extract_noaa_data(**context):
    """Extract NOAA data."""
    dataset = context.get("dataset", "unknown")
    print(f"Extracting NOAA data for {dataset}")
    return {"status": "extracted", "records": 2000}


# ===================================
# Transformation Functions
# ===================================

def transform_timeseries(**context):
    """Transform time series data."""
    print("Transforming time series data")
    return {"status": "transformed"}


# ===================================
# Load Functions
# ===================================

def load_to_iceberg(**context):
    """Load data to Iceberg tables."""
    dataset = context.get("dataset", "unknown")
    print(f"Loading data to Iceberg: {dataset}")
    return {"status": "loaded"}


# ===================================
# Quality Check Functions
# ===================================

def quality_check_timeseries(**context):
    """Quality check for time series data."""
    print("Running quality checks")
    return {"status": "passed", "quality_score": 0.95}


# ===================================
# DAG Generation
# ===================================

# Configuration for all ingestion workflows
INGESTION_WORKFLOWS = [
    # EIA workflows
    {
        "source": "eia",
        "dataset": "electricity",
        "schedule": "@hourly",
        "tags": ["eia", "electricity"],
    },
    {
        "source": "eia",
        "dataset": "petroleum",
        "schedule": "@daily",
        "tags": ["eia", "petroleum"],
    },
    {
        "source": "eia",
        "dataset": "natural_gas",
        "schedule": "@daily",
        "tags": ["eia", "natural_gas"],
    },
    # FRED workflows
    {
        "source": "fred",
        "dataset": "interest_rates",
        "schedule": "@daily",
        "tags": ["fred", "interest_rates"],
    },
    {
        "source": "fred",
        "dataset": "economic_indicators",
        "schedule": "@daily",
        "tags": ["fred", "economic"],
    },
    # NOAA workflows
    {
        "source": "noaa",
        "dataset": "weather_observations",
        "schedule": "@hourly",
        "tags": ["noaa", "weather"],
    },
    {
        "source": "noaa",
        "dataset": "forecasts",
        "schedule": "*/15 * * * *",  # Every 15 minutes
        "tags": ["noaa", "forecasts"],
    },
]

# Map source to extraction function
EXTRACT_FUNCTIONS = {
    "eia": extract_eia_data,
    "fred": extract_fred_data,
    "noaa": extract_noaa_data,
}


# Generate all DAGs from configuration
for workflow in INGESTION_WORKFLOWS:
    source = workflow["source"]
    dataset = workflow["dataset"]
    dag_id = f"ingest_{source}_{dataset}"
    
    dag_config = DagConfig(
        dag_id=dag_id,
        description=f"Ingest {source.upper()} {dataset} data",
        schedule=workflow["schedule"],
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["ingestion", source] + workflow.get("tags", [])
    )
    
    ingestion_config = IngestionConfig(
        source=source,
        dataset=dataset,
        extract_callable=EXTRACT_FUNCTIONS.get(source, extract_eia_data),
        transform_callable=transform_timeseries,
        load_callable=load_to_iceberg,
        quality_check_callable=quality_check_timeseries
    )
    
    # Generate DAG and add to globals so Airflow can discover it
    globals()[dag_id] = DataIngestionDagFactory.create_ingestion_dag(
        dag_config,
        ingestion_config
    )


# ===================================
# Result
# ===================================

# This single file replaces 7+ individual DAG files
# Each workflow is configured declaratively
# New sources/datasets can be added by extending the INGESTION_WORKFLOWS list

print(f"Generated {len(INGESTION_WORKFLOWS)} ingestion DAGs from template")

