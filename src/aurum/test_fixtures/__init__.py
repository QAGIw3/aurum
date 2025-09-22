"""Test fixtures and utilities for external data ingestion system.

This module provides comprehensive test fixtures, mock objects, and utilities
for testing all components of the external data ingestion system.
"""

from .fixtures import *
from .mock_objects import *
from .test_data_generators import *
from .validation_helpers import *

__all__ = [
    # Fixtures
    "get_golden_data_path",
    "load_golden_data",
    "get_test_config",

    # Mock Objects
    "MockHttpResponse",
    "MockKafkaProducer",
    "MockDatabaseConnection",
    "MockVaultClient",
    "MockCircuitBreaker",

    # Test Data Generators
    "generate_caiso_lmp_data",
    "generate_miso_load_data",
    "generate_pjm_generation_data",
    "generate_ercot_spp_data",
    "generate_spp_interchange_data",

    # Validation Helpers
    "validate_iso_record_schema",
    "assert_data_quality_metrics",
    "compare_with_golden_data",
]