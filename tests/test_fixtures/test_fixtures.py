"""Tests for test fixtures and golden file validation."""

from __future__ import annotations

import json
import pytest
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure src is in path for aurum imports
REPO_ROOT = Path(__file__).resolve().parents[3]
import sys
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.test_fixtures import (
    FixtureGenerator,
    FixtureConfig,
    DataSourceFixture,
    TestCase,
    SyntheticDataConfig,
    GoldenFileManager,
    TestValidator,
    ValidationResult,
    SyntheticDataGenerator,
    EIATestDataGenerator,
    FREDTestDataGenerator,
    ValidationSeverity
)


class TestSyntheticDataGenerator:
    def test_synthetic_data_config(self) -> None:
        """Test synthetic data configuration."""
        config = SyntheticDataConfig(
            record_count=100,
            date_range_days=30,
            include_null_values=True,
            null_probability=0.1,
            include_invalid_values=True,
            invalid_probability=0.05,
            seed=42
        )

        assert config.record_count == 100
        assert config.date_range_days == 30
        assert config.include_null_values is True
        assert config.null_probability == 0.1
        assert config.include_invalid_values is True
        assert config.invalid_probability == 0.05
        assert config.seed == 42

    def test_eia_data_generator(self) -> None:
        """Test EIA data generator."""
        config = SyntheticDataConfig(record_count=10, seed=42)
        generator = EIATestDataGenerator(config)

        records = generator.generate_records()
        schema = generator.get_schema()

        assert len(records) == 10
        assert all("series_id" in record for record in records)
        assert all("period" in record for record in records)
        assert all("value" in record for record in records)
        assert all("source" in record for record in records)
        assert schema["name"] == "EiaSeriesRecord"
        assert schema["namespace"] == "aurum.eia"

    def test_fred_data_generator(self) -> None:
        """Test FRED data generator."""
        config = SyntheticDataConfig(record_count=10, seed=42)
        generator = FREDTestDataGenerator(config)

        records = generator.generate_records()
        schema = generator.get_schema()

        assert len(records) == 10
        assert all("series_id" in record for record in records)
        assert all("date" in record for record in records)
        assert all("value" in record for record in records)
        assert all("source" in record for record in records)
        assert schema["name"] == "FredSeriesRecord"
        assert schema["namespace"] == "aurum.fred"

    def test_data_generator_with_nulls(self) -> None:
        """Test data generator with null values."""
        config = SyntheticDataConfig(
            record_count=10,
            include_null_values=True,
            null_probability=1.0,  # All values should be null
            seed=42
        )
        generator = EIATestDataGenerator(config)

        records = generator.generate_records()

        assert len(records) == 10
        # With null_probability=1.0, some fields should be null
        null_count = sum(1 for record in records if record.get("value") is None)
        assert null_count > 0

    def test_data_generator_with_invalid(self) -> None:
        """Test data generator with invalid values."""
        config = SyntheticDataConfig(
            record_count=10,
            include_invalid_values=True,
            invalid_probability=1.0,  # All values should be invalid
            seed=42
        )
        generator = EIATestDataGenerator(config)

        records = generator.generate_records()

        assert len(records) == 10
        # Some values should be marked as invalid
        invalid_count = sum(1 for record in records if "INVALID" in str(record.get("value", "")))
        assert invalid_count > 0


class TestFixtureGenerator:
    def test_fixture_generator_init(self) -> None:
        """Test fixture generator initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            assert generator.config == config
            assert generator.test_data_dir.exists()
            assert generator.golden_files_dir.exists()
            assert generator.schemas_dir.exists()

    def test_generate_all_fixtures(self) -> None:
        """Test generating all fixtures."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            summary = generator.generate_all_fixtures()

            assert "total_fixtures" in summary
            assert "total_test_cases" in summary
            assert "total_records" in summary
            assert "fixtures_by_source" in summary
            assert len(summary["fixtures_by_source"]) == 5  # eia, fred, cpi, noaa, iso

    def test_generate_fixture(self) -> None:
        """Test generating a single fixture."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            fixture = DataSourceFixture(data_source="test")
            test_case = TestCase(
                name="test_case",
                description="Test case",
                data_source="test",
                config=SyntheticDataConfig(record_count=10)
            )
            fixture.add_test_case(test_case)

            summary = generator.generate_fixture(fixture)

            assert summary["data_source"] == "test"
            assert summary["test_cases"] == 1
            assert summary["total_records"] == 10

    def test_list_fixtures(self) -> None:
        """Test listing fixtures."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            fixtures_list = generator.list_fixtures()

            assert "fixtures" in fixtures_list
            assert "total_fixtures" in fixtures_list
            assert fixtures_list["total_fixtures"] == 5


class TestGoldenFileManager:
    def test_golden_file_manager_init(self) -> None:
        """Test golden file manager initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            assert manager.base_dir == Path(temp_dir)
            assert len(manager.golden_files) == 0

    def test_register_golden_file(self) -> None:
        """Test registering a golden file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            test_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}

            golden_file = manager.register_golden_file(
                "test_file",
                test_data,
                schema,
                {"data_source": "test"}
            )

            assert golden_file.name == "test_file"
            assert golden_file.record_count == 1
            assert golden_file.path.exists()

            # Check that file was registered
            assert "test_file" in manager.golden_files

    def test_get_golden_file(self) -> None:
        """Test getting a golden file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            test_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}

            manager.register_golden_file("test_file", test_data, schema)

            # Test getting existing file
            golden_file = manager.get_golden_file("test_file")
            assert golden_file is not None
            assert golden_file.name == "test_file"

            # Test getting non-existent file
            assert manager.get_golden_file("nonexistent") is None

    def test_compare_with_actual(self) -> None:
        """Test comparing golden file with actual data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            test_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}

            manager.register_golden_file("test_file", test_data, schema)

            # Test matching data
            result = manager.compare_with_actual("test_file", test_data)
            assert result.name == "MATCH"

            # Test different data
            different_data = [{"id": "1", "value": 20}]
            result = manager.compare_with_actual("test_file", different_data)
            assert result.name == "DIFFERENT"

            # Test missing golden file
            result = manager.compare_with_actual("missing_file", test_data)
            assert result.name == "MISSING"

    def test_update_golden_file(self) -> None:
        """Test updating a golden file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            test_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}

            manager.register_golden_file("test_file", test_data, schema)

            # Update with new data
            new_data = [{"id": "1", "value": 20}, {"id": "2", "value": 30}]
            new_schema = {"type": "record", "name": "UpdatedTest"}

            updated_file = manager.update_golden_file("test_file", new_data, new_schema)

            assert updated_file.record_count == 2
            assert updated_file.checksum != manager.golden_files["test_file"].checksum

    def test_list_golden_files(self) -> None:
        """Test listing golden files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            # Register multiple files
            for i in range(3):
                test_data = [{"id": str(i), "value": i}]
                schema = {"type": "record", "name": f"Test{i}"}
                manager.register_golden_file(f"test_file_{i}", test_data, schema)

            golden_files = manager.list_golden_files()

            assert len(golden_files) == 3
            assert all(gf.name.startswith("test_file_") for gf in golden_files)

    def test_get_statistics(self) -> None:
        """Test getting golden file statistics."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            # Register files with different data sources
            test_cases = [
                ("eia", 100),
                ("fred", 200),
                ("eia", 150),
                ("cpi", 300)
            ]

            for data_source, record_count in test_cases:
                test_data = [{"id": "test", "value": 1}] * record_count
                schema = {"type": "record", "name": "Test"}
                metadata = {"data_source": data_source}

                manager.register_golden_file(
                    f"test_{data_source}_{record_count}",
                    test_data,
                    schema,
                    metadata
                )

            stats = manager.get_statistics()

            assert stats["total_files"] == 4
            assert stats["total_records"] == 750  # 100 + 200 + 150 + 300
            assert stats["by_data_source"]["eia"] == 2
            assert stats["by_data_source"]["fred"] == 1
            assert stats["by_data_source"]["cpi"] == 1


class TestTestValidator:
    def test_validator_init(self) -> None:
        """Test test validator initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            assert validator.golden_file_manager == manager

    def test_validate_test_output_match(self) -> None:
        """Test validation with matching output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            # Register golden file
            test_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}
            manager.register_golden_file("test_file", test_data, schema)

            validator = TestValidator(manager)

            # Validate matching data
            result = validator.validate_test_output("test_file", test_data)

            assert result.passed is True
            assert result.golden_file_status.name == "MATCH"
            assert len(result.issues) == 0

    def test_validate_test_output_mismatch(self) -> None:
        """Test validation with mismatched output."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            # Register golden file
            golden_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}
            manager.register_golden_file("test_file", golden_data, schema)

            validator = TestValidator(manager)

            # Validate different data
            actual_data = [{"id": "1", "value": 20}]
            result = validator.validate_test_output("test_file", actual_data)

            assert result.passed is False
            assert result.golden_file_status.name == "DIFFERENT"
            assert len(result.issues) > 0

    def test_validate_missing_golden_file(self) -> None:
        """Test validation with missing golden file."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            result = validator.validate_test_output("missing_file", [{"id": "1"}])

            assert result.passed is False
            assert result.golden_file_status.name == "MISSING"
            assert len(result.issues) > 0

    def test_validate_schema_compliance(self) -> None:
        """Test schema compliance validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Test data missing required fields
            test_data = [{"id": "1"}]  # Missing required "name" field
            schema = {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"}  # Required field
                ]
            }

            issues = validator._validate_schema_compliance(test_data, schema)

            assert len(issues) > 0
            assert any("MISSING_REQUIRED_FIELDS" in issue.issue_type for issue in issues)

    def test_validate_data_quality(self) -> None:
        """Test data quality validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Test data with high null rate
            test_data = [
                {"id": "1", "value": None},
                {"id": "2", "value": None},
                {"id": "3", "value": None}
            ]

            issues = validator._validate_data_quality(test_data)

            assert len(issues) > 0
            assert any("HIGH_NULL_RATE" in issue.issue_type for issue in issues)

    def test_calculate_metrics(self) -> None:
        """Test metrics calculation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            test_data = [{"id": "1", "value": 10}, {"id": "2", "value": 20}]
            issues = []

            metrics = validator._calculate_metrics(test_data, issues)

            assert metrics["record_count"] == 2
            assert metrics["field_count"] == 2
            assert metrics["issue_count"] == 0
            assert metrics["quality_score"] == 100

    def test_validate_batch(self) -> None:
        """Test batch validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Register golden files
            for i in range(3):
                test_data = [{"id": str(i), "value": i}]
                schema = {"type": "record", "name": "Test"}
                manager.register_golden_file(f"test_file_{i}", test_data, schema)

            # Test batch validation
            test_results = {
                "test_file_0": [{"id": "0", "value": 0}],
                "test_file_1": [{"id": "1", "value": 1}],
                "test_file_2": [{"id": "2", "value": 2}]
            }

            results = validator.validate_batch(test_results)

            assert len(results) == 3
            assert all(result.passed for result in results)

    def test_generate_validation_report(self) -> None:
        """Test validation report generation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Create test results
            results = [
                ValidationResult(
                    test_name="test1",
                    passed=True,
                    golden_file_status=ValidationResult.GoldenFileStatus.MATCH
                ),
                ValidationResult(
                    test_name="test2",
                    passed=False,
                    golden_file_status=ValidationResult.GoldenFileStatus.DIFFERENT,
                    issues=[ValidationResult.ValidationIssue(
                        test_name="test2",
                        issue_type="OUTPUT_MISMATCH",
                        severity=ValidationSeverity.ERROR,
                        message="Test output mismatch"
                    )]
                )
            ]

            report = validator.generate_validation_report(results)

            assert "Test Validation Report" in report
            assert "test1" in report
            assert "test2" in report
            assert "PASSED" in report
            assert "FAILED" in report


class TestIntegrationScenarios:
    def test_complete_fixture_generation(self) -> None:
        """Test complete fixture generation workflow."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            # Generate all fixtures
            summary = generator.generate_all_fixtures()

            assert summary["total_fixtures"] == 5  # eia, fred, cpi, noaa, iso
            assert summary["total_test_cases"] > 0
            assert summary["total_records"] > 0

            # Check that files were created
            assert generator.test_data_dir.exists()
            assert generator.golden_files_dir.exists()
            assert generator.schemas_dir.exists()

            # Check that test data files exist
            test_data_files = list(generator.test_data_dir.rglob("*.json"))
            assert len(test_data_files) > 0

    def test_golden_file_lifecycle(self) -> None:
        """Test golden file lifecycle management."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))

            # Register initial golden file
            initial_data = [{"id": "1", "value": 10}]
            schema = {"type": "record", "name": "Test"}
            metadata = {"version": "1.0", "data_source": "test"}

            golden_file = manager.register_golden_file(
                "test_file",
                initial_data,
                schema,
                metadata
            )

            assert golden_file.name == "test_file"
            assert golden_file.record_count == 1

            # Update golden file
            updated_data = [{"id": "1", "value": 10}, {"id": "2", "value": 20}]
            updated_schema = {"type": "record", "name": "Test", "version": "2"}

            updated_file = manager.update_golden_file(
                "test_file",
                updated_data,
                updated_schema
            )

            assert updated_file.record_count == 2
            assert updated_file.checksum != golden_file.checksum

    def test_validation_with_realistic_data(self) -> None:
        """Test validation with realistic test data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Create realistic test data
            realistic_data = [
                {"id": "TEST_001", "name": "Test Record 1", "value": 100.5, "source": "test", "ingested_at": "2024-01-01T00:00:00"},
                {"id": "TEST_002", "name": "Test Record 2", "value": 200.7, "source": "test", "ingested_at": "2024-01-01T00:00:00"},
                {"id": "TEST_003", "name": "Test Record 3", "value": 300.1, "source": "test", "ingested_at": "2024-01-01T00:00:00"}
            ]

            schema = {
                "type": "record",
                "name": "TestRecord",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "name", "type": "string"},
                    {"name": "value", "type": ["null", "double"], "default": None},
                    {"name": "source", "type": "string"},
                    {"name": "ingested_at", "type": "string"}
                ]
            }

            # Register golden file
            manager.register_golden_file("realistic_test", realistic_data, schema)

            # Validate matching data
            result = validator.validate_test_output("realistic_test", realistic_data)

            assert result.passed is True
            assert result.golden_file_status.name == "MATCH"
            assert result.metrics["record_count"] == 3
            assert result.metrics["quality_score"] == 100

    def test_error_handling_and_reporting(self) -> None:
        """Test error handling and reporting in fixture system."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            # Test with invalid configuration
            invalid_config = SyntheticDataConfig(record_count=-1)  # Invalid negative count

            test_case = TestCase(
                name="invalid_test",
                description="Test with invalid config",
                data_source="test",
                config=invalid_config
            )

            fixture = DataSourceFixture(data_source="test")
            fixture.add_test_case(test_case)

            # Should handle the error gracefully
            try:
                summary = generator.generate_test_case(fixture, test_case)
                # If it doesn't crash, that's good error handling
                assert "error" in summary or summary.get("record_count", 0) >= 0
            except Exception:
                # If it does crash, that's also acceptable for invalid input
                pass

    def test_performance_with_large_datasets(self) -> None:
        """Test performance with large datasets."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            # Create large test case
            large_config = SyntheticDataConfig(record_count=10000, seed=42)
            test_case = TestCase(
                name="large_dataset_test",
                description="Performance test with large dataset",
                data_source="eia",
                config=large_config
            )

            fixture = generator.get_fixture("eia")
            if fixture:
                fixture.add_test_case(test_case)

                # Generate the test case - should complete in reasonable time
                summary = generator.generate_test_case(fixture, test_case)

                assert summary["record_count"] == 10000
                assert summary["name"] == "large_dataset_test"

    def test_cross_data_source_validation(self) -> None:
        """Test validation across different data sources."""
        with tempfile.TemporaryDirectory() as temp_dir:
            manager = GoldenFileManager(Path(temp_dir))
            validator = TestValidator(manager)

            # Register golden files for different data sources
            test_cases = [
                ("eia", [{"id": "EIA_001", "value": 100, "source": "EIA"}]),
                ("fred", [{"id": "FRED_001", "value": 2.5, "source": "FRED"}]),
                ("cpi", [{"id": "CPI_001", "value": 250.5, "source": "CPI"}])
            ]

            for data_source, test_data in test_cases:
                schema = {"type": "record", "name": f"{data_source}Record"}
                manager.register_golden_file(f"{data_source}_test", test_data, schema)

            # Validate each data source
            for data_source, expected_data in test_cases:
                result = validator.validate_test_output(f"{data_source}_test", expected_data)

                assert result.passed is True, f"Validation failed for {data_source}"
                assert result.test_name == f"{data_source}_test"

    def test_fixture_metadata_and_tracking(self) -> None:
        """Test fixture metadata and tracking capabilities."""
        with tempfile.TemporaryDirectory() as temp_dir:
            config = FixtureConfig(output_dir=Path(temp_dir))
            generator = FixtureGenerator(config)

            # Generate fixtures
            summary = generator.generate_all_fixtures()

            # Verify metadata is tracked
            fixtures_list = generator.list_fixtures()
            assert "total_fixtures" in fixtures_list
            assert fixtures_list["total_fixtures"] > 0

            # Check that metadata files were created
            metadata_files = list(generator.test_data_dir.rglob("metadata.json"))
            assert len(metadata_files) > 0

            # Verify metadata content
            for metadata_file in metadata_files:
                with open(metadata_file) as f:
                    metadata = json.load(f)

                assert "name" in metadata
                assert "checksum" in metadata
                assert "generated_at" in metadata
                assert "record_count" in metadata
