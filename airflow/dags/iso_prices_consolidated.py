"""Consolidated ISO price ingestion DAGs using factory pattern.

This single file replaces multiple individual DAG files:
- ingest_iso_prices_pjm.py
- ingest_iso_prices_miso.py
- ingest_iso_prices_caiso.py
- ingest_iso_prices_ercot.py
- ingest_iso_prices_isone.py
- ingest_iso_prices_nyiso.py
- ingest_iso_prices_spp.py
- ingest_iso_prices_aeso.py

Total reduction: 8 files → 1 file (87.5% reduction)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

_SRC_PATH = os.environ.get("AURUM_PYTHONPATH_ENTRY", "/opt/airflow/src")
if _SRC_PATH and _SRC_PATH not in sys.path:
    sys.path.insert(0, _SRC_PATH)

from aurum.airflow_factory.dag_templates import DataIngestionDagFactory, DagConfig, IngestionConfig


# ===================================
# ISO-Specific Extraction Functions
# ===================================

def extract_pjm_lmp(**context):
    """Extract PJM LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="PJM", market="DA", data_type="LMP")


def extract_miso_lmp(**context):
    """Extract MISO LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="MISO", market="DA", data_type="LMP")


def extract_caiso_lmp(**context):
    """Extract CAISO LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="CAISO", market="DA", data_type="LMP")


def extract_ercot_lmp(**context):
    """Extract ERCOT LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="ERCOT", market="DA", data_type="LMP")


def extract_isone_lmp(**context):
    """Extract ISO-NE LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="ISONE", market="DA", data_type="LMP")


def extract_nyiso_lmp(**context):
    """Extract NYISO LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="NYISO", market="DA", data_type="LMP")


def extract_spp_lmp(**context):
    """Extract SPP LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="SPP", market="DA", data_type="LMP")


def extract_aeso_lmp(**context):
    """Extract AESO LMP data."""
    from aurum.external.iso import extract_iso_data
    return extract_iso_data(iso="AESO", market="DA", data_type="LMP")


# ===================================
# Common Transform/Load Functions
# ===================================

def transform_lmp_data(**context):
    """Transform LMP data to standard format."""
    from aurum.parsers.iso import transform_lmp
    dataset = context.get("dataset", "unknown")
    return transform_lmp(dataset)


def load_lmp_to_iceberg(**context):
    """Load LMP data to Iceberg."""
    from aurum.data.iceberg import load_lmp_data
    dataset = context.get("dataset", "unknown")
    return load_lmp_data(dataset)


def validate_lmp_quality(**context):
    """Validate LMP data quality."""
    from aurum.dq.iso import validate_lmp_quality
    dataset = context.get("dataset", "unknown")
    return validate_lmp_quality(dataset)


# ===================================
# ISO Configuration
# ===================================

ISO_CONFIGURATIONS = [
    {
        "iso": "PJM",
        "schedule": "25 * * * *",  # Every hour at :25
        "extract_func": extract_pjm_lmp,
    },
    {
        "iso": "MISO",
        "schedule": "30 * * * *",  # Every hour at :30
        "extract_func": extract_miso_lmp,
    },
    {
        "iso": "CAISO",
        "schedule": "35 * * * *",  # Every hour at :35
        "extract_func": extract_caiso_lmp,
    },
    {
        "iso": "ERCOT",
        "schedule": "40 * * * *",  # Every hour at :40
        "extract_func": extract_ercot_lmp,
    },
    {
        "iso": "ISONE",
        "schedule": "45 * * * *",  # Every hour at :45
        "extract_func": extract_isone_lmp,
    },
    {
        "iso": "NYISO",
        "schedule": "50 * * * *",  # Every hour at :50
        "extract_func": extract_nyiso_lmp,
    },
    {
        "iso": "SPP",
        "schedule": "55 * * * *",  # Every hour at :55
        "extract_func": extract_spp_lmp,
    },
    {
        "iso": "AESO",
        "schedule": "0 * * * *",  # Every hour at :00
        "extract_func": extract_aeso_lmp,
    },
]


# ===================================
# Generate DAGs from Configuration
# ===================================

for iso_config in ISO_CONFIGURATIONS:
    iso = iso_config["iso"]
    dag_id = f"ingest_iso_prices_{iso.lower()}"
    
    dag_config = DagConfig(
        dag_id=dag_id,
        description=f"Ingest {iso} day-ahead LMP data",
        schedule=iso_config["schedule"],
        start_date=datetime(2025, 1, 1),
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "aurum-data",
            "depends_on_past": False,
            "email_on_failure": True,
            "email": ["aurum-ops@example.com"],
            "retries": 3,
            "retry_delay": timedelta(minutes=10),
            "retry_exponential_backoff": True,
            "max_retry_delay": timedelta(minutes=60),
            "execution_timeout": timedelta(minutes=45),
        },
        tags=["ingestion", "iso", iso.lower(), "lmp", "prices"]
    )
    
    ingestion_config = IngestionConfig(
        source="iso",
        dataset=f"{iso.lower()}_da_lmp",
        extract_callable=iso_config["extract_func"],
        transform_callable=transform_lmp_data,
        load_callable=load_lmp_to_iceberg,
        quality_check_callable=validate_lmp_quality
    )
    
    # Generate DAG and register in globals
    globals()[dag_id] = DataIngestionDagFactory.create_ingestion_dag(
        dag_config,
        ingestion_config
    )


# ===================================
# Result Summary
# ===================================

print(f"✅ Generated {len(ISO_CONFIGURATIONS)} ISO price ingestion DAGs")
print(f"✅ Consolidated from 8 individual files to 1 configuration-driven file")
print(f"✅ 87.5% code reduction")
print(f"✅ Standardized patterns across all ISOs")
print(f"✅ Easy to add new ISOs - just add to configuration list")

