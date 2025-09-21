"""Test fixture generator for data ingestion validation."""

from __future__ import annotations

import json
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from datetime import datetime

from .synthetic_data import (
    SyntheticDataGenerator,
    SyntheticDataConfig,
    create_data_generator
)


@dataclass
class TestCase:
    """A test case for data ingestion validation."""

    name: str
    description: str
    data_source: str
    config: SyntheticDataConfig
    expected_output_schema: Dict[str, Any]
    validation_rules: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.tags:
            self.tags = ["test", self.data_source]


@dataclass
class DataSourceFixture:
    """Test fixture for a specific data source."""

    data_source: str
    test_cases: List[TestCase] = field(default_factory=list)
    base_config: SyntheticDataConfig = field(default_factory=SyntheticDataConfig)

    def add_test_case(self, test_case: TestCase) -> None:
        """Add a test case to this fixture.

        Args:
            test_case: Test case to add
        """
        self.test_cases.append(test_case)

    def generate_test_data(self, test_case: TestCase) -> List[Dict[str, Any]]:
        """Generate test data for a test case.

        Args:
            test_case: Test case to generate data for

        Returns:
            List of test data records
        """
        generator = create_data_generator(test_case.data_source, test_case.config)
        return generator.generate_records()

    def get_test_case(self, name: str) -> Optional[TestCase]:
        """Get a test case by name.

        Args:
            name: Name of the test case

        Returns:
            Test case or None if not found
        """
        for test_case in self.test_cases:
            if test_case.name == name:
                return test_case
        return None


@dataclass
class FixtureConfig:
    """Configuration for fixture generation."""

    output_dir: Path
    generate_golden_files: bool = True
    include_null_values: bool = True
    include_invalid_values: bool = True
    record_counts: List[int] = field(default_factory=lambda: [10, 100, 1000])
    data_sources: List[str] = field(default_factory=lambda: ["eia", "fred", "cpi", "noaa", "iso"])


class FixtureGenerator:
    """Generate test fixtures and golden files for data ingestion."""

    def __init__(self, config: FixtureConfig):
        """Initialize fixture generator.

        Args:
            config: Configuration for fixture generation
        """
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

        # Create subdirectories
        self.test_data_dir = config.output_dir / "test_data"
        self.golden_files_dir = config.output_dir / "golden_files"
        self.schemas_dir = config.output_dir / "schemas"

        for dir_path in [self.test_data_dir, self.golden_files_dir, self.schemas_dir]:
            dir_path.mkdir(exist_ok=True)

        self.fixtures: Dict[str, DataSourceFixture] = {}
        self._setup_default_fixtures()

    def _setup_default_fixtures(self) -> None:
        """Set up default test fixtures for all data sources."""
        for data_source in self.config.data_sources:
            fixture = DataSourceFixture(data_source=data_source)

            # Basic validation test case
            basic_config = SyntheticDataConfig(
                record_count=100,
                include_null_values=self.config.include_null_values,
                include_invalid_values=False,
                seed=42
            )

            basic_test_case = TestCase(
                name="basic_validation",
                description="Basic data validation test",
                data_source=data_source,
                config=basic_config,
                expected_output_schema=self._get_default_schema(data_source),
                validation_rules={
                    "min_records": 100,
                    "max_null_rate": 0.1,
                    "required_fields": ["id", "source", "ingested_at"]
                }
            )

            fixture.add_test_case(basic_test_case)

            # Data quality test case
            quality_config = SyntheticDataConfig(
                record_count=1000,
                include_null_values=self.config.include_null_values,
                include_invalid_values=self.config.include_invalid_values,
                seed=123
            )

            quality_test_case = TestCase(
                name="data_quality",
                description="Comprehensive data quality validation",
                data_source=data_source,
                config=quality_config,
                expected_output_schema=self._get_default_schema(data_source),
                validation_rules={
                    "min_records": 1000,
                    "max_null_rate": 0.15,
                    "max_invalid_rate": 0.05,
                    "quality_threshold": 0.95
                },
                tags=["quality", "comprehensive"]
            )

            fixture.add_test_case(quality_test_case)

            # Performance test case
            perf_config = SyntheticDataConfig(
                record_count=10000,
                include_null_values=False,
                include_invalid_values=False,
                seed=456
            )

            perf_test_case = TestCase(
                name="performance_test",
                description="Large dataset performance test",
                data_source=data_source,
                config=perf_config,
                expected_output_schema=self._get_default_schema(data_source),
                validation_rules={
                    "min_records": 10000,
                    "max_processing_time_seconds": 30
                },
                tags=["performance", "large_dataset"]
            )

            fixture.add_test_case(perf_test_case)

            self.fixtures[data_source] = fixture

    def _get_default_schema(self, data_source: str) -> Dict[str, Any]:
        """Get default schema for a data source.

        Args:
            data_source: Name of the data source

        Returns:
            Default Avro schema
        """
        schemas = {
            "eia": {
                "type": "record",
                "name": "EiaSeriesRecord",
                "namespace": "aurum.eia",
                "fields": [
                    {"name": "series_id", "type": "string"},
                    {"name": "period", "type": "string"},
                    {"name": "value", "type": ["null", "double"], "default": None},
                    {"name": "units", "type": ["null", "string"], "default": None},
                    {"name": "area", "type": ["null", "string"], "default": None},
                    {"name": "sector", "type": ["null", "string"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            },
            "fred": {
                "type": "record",
                "name": "FredSeriesRecord",
                "namespace": "aurum.fred",
                "fields": [
                    {"name": "series_id", "type": "string"},
                    {"name": "date", "type": "string"},
                    {"name": "value", "type": ["null", "double"], "default": None},
                    {"name": "units", "type": ["null", "string"], "default": None},
                    {"name": "seasonal_adjustment", "type": ["null", "string"], "default": None},
                    {"name": "frequency", "type": ["null", "string"], "default": None},
                    {"name": "title", "type": ["null", "string"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            },
            "cpi": {
                "type": "record",
                "name": "CpiSeriesRecord",
                "namespace": "aurum.cpi",
                "fields": [
                    {"name": "series_id", "type": "string"},
                    {"name": "date", "type": "string"},
                    {"name": "value", "type": ["null", "double"], "default": None},
                    {"name": "units", "type": ["null", "string"], "default": None},
                    {"name": "seasonal_adjustment", "type": ["null", "string"], "default": None},
                    {"name": "area", "type": ["null", "string"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            },
            "noaa": {
                "type": "record",
                "name": "NoaaWeatherRecord",
                "namespace": "aurum.noaa",
                "fields": [
                    {"name": "station_id", "type": "string"},
                    {"name": "date", "type": "string"},
                    {"name": "temperature_max", "type": ["null", "double"], "default": None},
                    {"name": "temperature_min", "type": ["null", "double"], "default": None},
                    {"name": "precipitation", "type": ["null", "double"], "default": None},
                    {"name": "snowfall", "type": ["null", "double"], "default": None},
                    {"name": "wind_speed", "type": ["null", "double"], "default": None},
                    {"name": "data_quality", "type": ["null", "string"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            },
            "iso": {
                "type": "record",
                "name": "IsoLmpRecord",
                "namespace": "aurum.iso",
                "fields": [
                    {"name": "iso", "type": "string"},
                    {"name": "datetime", "type": "string"},
                    {"name": "lmp", "type": ["null", "double"], "default": None},
                    {"name": "load", "type": ["null", "int"], "default": None},
                    {"name": "generation", "type": ["null", "int"], "default": None},
                    {"name": "node", "type": ["null", "string"], "default": None},
                    {"name": "zone", "type": ["null", "string"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            }
        }

        return schemas.get(data_source, schemas["eia"])

    def generate_all_fixtures(self) -> Dict[str, Any]:
        """Generate all test fixtures.

        Returns:
            Summary of generated fixtures
        """
        summary = {
            "total_fixtures": 0,
            "total_test_cases": 0,
            "total_records": 0,
            "fixtures_by_source": {},
            "generated_at": datetime.now().isoformat()
        }

        for data_source, fixture in self.fixtures.items():
            source_summary = self.generate_fixture(fixture)
            summary["fixtures_by_source"][data_source] = source_summary
            summary["total_fixtures"] += 1
            summary["total_test_cases"] += source_summary["test_cases"]
            summary["total_records"] += source_summary["total_records"]

        return summary

    def generate_fixture(self, fixture: DataSourceFixture) -> Dict[str, Any]:
        """Generate test fixture for a data source.

        Args:
            fixture: Data source fixture to generate

        Returns:
            Summary of generated fixture
        """
        summary = {
            "data_source": fixture.data_source,
            "test_cases": 0,
            "total_records": 0,
            "test_cases_details": []
        }

        for test_case in fixture.test_cases:
            case_summary = self.generate_test_case(fixture, test_case)
            summary["test_cases_details"].append(case_summary)
            summary["test_cases"] += 1
            summary["total_records"] += case_summary["record_count"]

        return summary

    def generate_test_case(self, fixture: DataSourceFixture, test_case: TestCase) -> Dict[str, Any]:
        """Generate a single test case.

        Args:
            fixture: Parent fixture
            test_case: Test case to generate

        Returns:
            Summary of generated test case
        """
        # Generate test data
        test_data = fixture.generate_test_data(test_case)

        # Create test case directory
        test_case_dir = self.test_data_dir / fixture.data_source / test_case.name
        test_case_dir.mkdir(parents=True, exist_ok=True)

        # Save test data
        test_data_file = test_case_dir / "input.json"
        with open(test_data_file, 'w') as f:
            json.dump(test_data, f, indent=2, default=str)

        # Save schema
        schema_file = test_case_dir / "schema.avsc"
        with open(schema_file, 'w') as f:
            json.dump(test_case.expected_output_schema, f, indent=2)

        # Generate golden file (expected output after processing)
        if self.config.generate_golden_files:
            golden_file = self._generate_golden_file(test_case, test_data)
            golden_file_path = test_case_dir / "expected_output.json"
            with open(golden_file_path, 'w') as f:
                json.dump(golden_file, f, indent=2, default=str)

        # Save test case metadata
        metadata = {
            "name": test_case.name,
            "description": test_case.description,
            "data_source": test_case.data_source,
            "record_count": len(test_data),
            "config": {
                "record_count": test_case.config.record_count,
                "include_null_values": test_case.config.include_null_values,
                "include_invalid_values": test_case.config.include_invalid_values,
                "seed": test_case.config.seed
            },
            "validation_rules": test_case.validation_rules,
            "tags": test_case.tags,
            "generated_at": datetime.now().isoformat(),
            "checksum": self._calculate_checksum(test_data)
        }

        metadata_file = test_case_dir / "metadata.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)

        return {
            "name": test_case.name,
            "record_count": len(test_data),
            "files_generated": [
                str(test_data_file.relative_to(self.config.output_dir)),
                str(schema_file.relative_to(self.config.output_dir)),
                str(metadata_file.relative_to(self.config.output_dir))
            ],
            "checksum": metadata["checksum"]
        }

    def _generate_golden_file(self, test_case: TestCase, test_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Generate golden file (expected output) for a test case.

        Args:
            test_case: Test case configuration
            test_data: Input test data

        Returns:
            Expected output data
        """
        # This simulates what the SeaTunnel job would output
        # In a real implementation, this would run the actual transformation

        golden_data = []

        for record in test_data:
            # Add processing metadata
            processed_record = record.copy()
            processed_record["processed_at"] = datetime.now().isoformat()
            processed_record["processing_pipeline"] = "test_fixture"

            # Add data quality indicators
            processed_record["data_quality_score"] = 0.95
            processed_record["validation_passed"] = True

            # Add Kafka message metadata
            processed_record["kafka_topic"] = f"aurum.{test_case.data_source}.test.v1"
            processed_record["kafka_partition"] = 0
            processed_record["kafka_offset"] = 0

            golden_data.append(processed_record)

        return golden_data

    def _calculate_checksum(self, data: List[Dict[str, Any]]) -> str:
        """Calculate checksum for test data.

        Args:
            data: Test data records

        Returns:
            SHA256 checksum as hex string
        """
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(data_str.encode()).hexdigest()

    def get_fixture(self, data_source: str) -> Optional[DataSourceFixture]:
        """Get fixture for a data source.

        Args:
            data_source: Name of the data source

        Returns:
            Data source fixture or None if not found
        """
        return self.fixtures.get(data_source)

    def list_fixtures(self) -> Dict[str, Any]:
        """List all available fixtures.

        Returns:
            Summary of all fixtures
        """
        return {
            "fixtures": {
                data_source: {
                    "test_cases": len(fixture.test_cases),
                    "data_source": fixture.data_source
                }
                for data_source, fixture in self.fixtures.items()
            },
            "total_fixtures": len(self.fixtures),
            "output_directory": str(self.config.output_dir)
        }
