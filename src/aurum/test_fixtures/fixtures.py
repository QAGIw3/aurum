"""Core test fixtures and utilities for external data ingestion system."""

import json
import os
from pathlib import Path
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timedelta

from ..external.pipeline.orchestrator import PipelineConfig
from ..external.quota_manager import DatasetQuota


@dataclass
class TestConfig:
    """Configuration for test environment."""
    kafka_bootstrap_servers: str = "localhost:9092"
    timescale_connection: str = "postgresql://test:test@localhost:5432/test"
    iceberg_catalog: str = "test_nessie"
    schema_registry_url: str = "http://localhost:8081"
    enable_great_expectations: bool = False
    enable_seatunnel_validation: bool = False


def get_golden_data_path() -> Path:
    """Get the path to golden test data directory."""
    return Path(__file__).parent / "golden_data"


def load_golden_data(iso_code: str, data_type: str) -> Dict[str, Any]:
    """Load golden test data for specified ISO and data type."""
    file_path = get_golden_data_path() / f"{iso_code.lower()}_{data_type.lower()}.json"

    if not file_path.exists():
        raise FileNotFoundError(f"Golden data not found: {file_path}")

    with open(file_path, 'r') as f:
        return json.load(f)


def get_test_config() -> TestConfig:
    """Get test configuration."""
    return TestConfig()


def get_pipeline_config() -> PipelineConfig:
    """Get pipeline configuration for testing."""
    return PipelineConfig(
        kafka_bootstrap_servers="localhost:9092",
        kafka_topic_prefix="test.aurum.iso",
        timescale_connection="postgresql://test:test@localhost:5432/test",
        enable_great_expectations=False,  # Disabled for unit tests
        enable_seatunnel_validation=False,  # Disabled for unit tests
        circuit_breaker_enabled=False  # Disabled for unit tests
    )


def get_test_quotas() -> List[DatasetQuota]:
    """Get test quota configurations."""
    return [
        DatasetQuota(
            dataset_id="test_caiso_lmp",
            iso_code="CAISO",
            max_records_per_hour=10_000,
            max_bytes_per_hour=1_000_000,
            max_concurrent_requests=2,
            max_requests_per_minute=10,
            priority=1
        ),
        DatasetQuota(
            dataset_id="test_miso_load",
            iso_code="MISO",
            max_records_per_hour=5_000,
            max_bytes_per_hour=500_000,
            max_concurrent_requests=3,
            max_requests_per_minute=15,
            priority=2
        ),
        DatasetQuota(
            dataset_id="test_pjm_generation",
            iso_code="PJM",
            max_records_per_hour=8_000,
            max_bytes_per_hour=800_000,
            max_concurrent_requests=2,
            max_requests_per_minute=12,
            priority=1
        )
    ]


def get_test_time_range() -> tuple[datetime, datetime]:
    """Get a standard test time range."""
    end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=1)
    return start_time, end_time


def create_test_metadata(iso_code: str, data_type: str) -> Dict[str, Any]:
    """Create test metadata for validation."""
    return {
        "iso_code": iso_code,
        "data_type": data_type,
        "test_run": True,
        "validation_enabled": True,
        "required_fields": get_required_fields_for_data_type(data_type),
        "topic": f"test.aurum.iso.{iso_code.lower()}.{data_type}.v1",
        "table": f"test.{iso_code.lower()}.{data_type}"
    }


def get_required_fields_for_data_type(data_type: str) -> List[str]:
    """Get required fields for a data type."""
    field_mappings = {
        "lmp": ["location_id", "price_total", "interval_start", "currency", "uom"],
        "load": ["location_id", "load_mw", "interval_start"],
        "generation_mix": ["location_id", "fuel_type", "generation_mw", "interval_start"],
        "ancillary_services": ["location_id", "as_type", "as_price", "interval_start"],
        "spp": ["settlement_point", "spp", "interval_start"],
        "interchange": ["interface", "interchange_mw", "interval_start"]
    }
    return field_mappings.get(data_type, ["location_id", "value", "interval_start"])


def get_test_credentials() -> Dict[str, str]:
    """Get test credentials for API access."""
    return {
        "CAISO": {"api_key": "test_caiso_key", "base_url": "https://test.oasis.caiso.com"},
        "MISO": {"api_key": "test_miso_key", "base_url": "https://test.api.misoenergy.org"},
        "PJM": {"api_key": "test_pjm_key", "base_url": "https://test.api.pjm.com"},
        "ERCOT": {"api_key": "test_ercot_key", "base_url": "https://test.api.ercot.com"},
        "SPP": {"api_key": "test_spp_key", "base_url": "https://test.api.spp.org"}
    }


def get_expected_record_counts() -> Dict[str, Dict[str, int]]:
    """Get expected record counts for test scenarios."""
    return {
        "caiso": {
            "lmp": 100,
            "load": 50,
            "asm": 25
        },
        "miso": {
            "lmp": 75,
            "load": 40,
            "generation_mix": 30
        },
        "pjm": {
            "lmp": 60,
            "load": 35,
            "generation": 25
        },
        "ercot": {
            "spp": 80,
            "load": 45,
            "generation": 30
        },
        "spp": {
            "lmp": 55,
            "load": 30,
            "generation": 20
        }
    }
