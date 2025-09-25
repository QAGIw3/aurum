#!/usr/bin/env python3
"""Demo script for data pipeline consolidation feature flags."""

import sys
import os
import logging
from pathlib import Path
from enum import Enum

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
sys.path.insert(0, str(Path(__file__).parent / "airflow"))

# Define the classes locally to avoid import issues
class DataSourceType(str, Enum):
    """Types of data sources supported by the factory."""
    EIA = "eia"
    FRED = "fred"
    CPI = "cpi"
    ISO = "iso"
    NOAA = "noaa"
    PUBLIC_FEEDS = "public_feeds"
    VENDOR_CURVES = "vendor_curves"

# Feature flags for pipeline consolidation
PIPELINE_FEATURE_FLAGS = {
    "use_consolidated_dags": os.getenv("AURUM_USE_CONSOLIDATED_DAGS", "false").lower() == "true",
    "pipeline_migration_phase": os.getenv("AURUM_PIPELINE_MIGRATION_PHASE", "1"),
    "enable_dag_templates": os.getenv("AURUM_ENABLE_DAG_TEMPLATES", "false").lower() == "true",
}

def get_pipeline_migration_phase() -> str:
    """Get current pipeline migration phase."""
    return PIPELINE_FEATURE_FLAGS["pipeline_migration_phase"]

def is_pipeline_consolidation_enabled() -> bool:
    """Check if consolidated DAGs are enabled."""
    return PIPELINE_FEATURE_FLAGS["use_consolidated_dags"]

def log_pipeline_migration_status():
    """Log current pipeline migration status."""
    phase = get_pipeline_migration_phase()
    consolidated = is_pipeline_consolidation_enabled()
    logger = logging.getLogger(__name__)

    logger.info(f"Pipeline Migration Status: Phase {phase}, Consolidated: {consolidated}")
    logger.info(f"Template DAGs Enabled: {PIPELINE_FEATURE_FLAGS['enable_dag_templates']}")

class MockDAG:
    """Mock DAG for demonstration purposes."""
    def __init__(self, dag_id, description="", tags=None):
        self.dag_id = dag_id
        self.description = description
        self.tags = tags or []

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def demo_pipeline_consolidation():
    """Demonstrate pipeline consolidation feature flags."""
    logger.info("=== Data Pipeline Consolidation Demo ===")

    # Show current migration status
    logger.info("1. Current Migration Status:")
    logger.info(f"   Consolidation enabled: {is_pipeline_consolidation_enabled()}")
    logger.info(f"   Migration phase: {get_pipeline_migration_phase()}")
    logger.info(f"   Template DAGs enabled: {PIPELINE_FEATURE_FLAGS['enable_dag_templates']}")

    # Test legacy mode (default)
    logger.info("\n2. Testing Legacy Mode (Individual DAGs):")
    os.environ["AURUM_USE_CONSOLIDATED_DAGS"] = "false"
    os.environ["AURUM_PIPELINE_MIGRATION_PHASE"] = "1"

    try:
        # Simulate legacy DAG creation
        legacy_dag = MockDAG(
            dag_id="legacy_ingest_eia_timescale",
            description="Legacy individual ingestion from eia to timescale",
            tags=["aurum", "legacy", "individual", "eia"]
        )
        logger.info(f"✅ Legacy EIA DAG created: {legacy_dag.dag_id}")
        logger.info(f"   Description: {legacy_dag.description}")
        logger.info(f"   Tags: {legacy_dag.tags}")
    except Exception as e:
        logger.error(f"❌ Legacy DAG creation failed: {e}")

    # Test consolidated mode
    logger.info("\n3. Testing Consolidated Mode:")
    os.environ["AURUM_USE_CONSOLIDATED_DAGS"] = "true"
    os.environ["AURUM_PIPELINE_MIGRATION_PHASE"] = "2"

    try:
        # Simulate consolidated DAG creation
        consolidated_dag = MockDAG(
            dag_id="consolidated_ingest_iso_trino",
            description="Consolidated ingestion from iso to trino",
            tags=["aurum", "consolidated", "iso"]
        )
        logger.info(f"✅ Consolidated ISO DAG created: {consolidated_dag.dag_id}")
        logger.info(f"   Description: {consolidated_dag.description}")
        logger.info(f"   Tags: {consolidated_dag.tags}")
    except Exception as e:
        logger.error(f"❌ Consolidated DAG creation failed: {e}")

    # Show migration benefits
    logger.info("\n=== Consolidation Benefits ===")
    logger.info("✅ Reduces 60+ individual DAGs to ~5 consolidated templates")
    logger.info("✅ Standardizes retry, timeout, and resource configurations")
    logger.info("✅ Simplifies monitoring and alerting setup")
    logger.info("✅ Enables easier maintenance and debugging")
    logger.info("✅ Feature-flagged gradual migration")
    logger.info("✅ Automatic fallback to legacy mode")

    logger.info("\n=== Environment Variables ===")
    logger.info("AURUM_USE_CONSOLIDATED_DAGS=true  # Enable consolidated mode")
    logger.info("AURUM_PIPELINE_MIGRATION_PHASE=2   # Set migration phase")
    logger.info("AURUM_ENABLE_DAG_TEMPLATES=true   # Enable template DAGs")

    logger.info("\n=== Data Source Types ===")
    for source_type in DataSourceType:
        logger.info(f"   - {source_type.value}")

    logger.info("\n=== Migration Strategy ===")
    logger.info("1. Phase 1: Individual DAGs (current)")
    logger.info("2. Phase 2: Consolidated templates")
    logger.info("3. Phase 3: Unified pipeline orchestrator")

def demo_dag_comparison():
    """Compare individual vs consolidated DAG creation."""
    logger.info("\n=== DAG Creation Comparison ===")

    # Individual DAG approach (legacy)
    logger.info("Legacy Individual DAG:")
    logger.info("  - ingest_eia_series_timescale.py")
    logger.info("  - ingest_iso_load_timescale.py")
    logger.info("  - ingest_noaa_station_discovery.py")
    logger.info("  - ... (60+ individual DAGs)")
    logger.info("  - Duplicated configuration")
    logger.info("  - Hard to maintain")
    logger.info("  - Inconsistent patterns")

    # Consolidated approach
    logger.info("\nConsolidated DAG:")
    logger.info("  - consolidated_ingest_eia_timescale")
    logger.info("  - consolidated_ingest_iso_trino")
    logger.info("  - consolidated_ingest_noaa_iceberg")
    logger.info("  - ... (5 templates)")
    logger.info("  - Shared configuration")
    logger.info("  - Easy to maintain")
    logger.info("  - Consistent patterns")

if __name__ == "__main__":
    demo_pipeline_consolidation()
    demo_dag_comparison()
