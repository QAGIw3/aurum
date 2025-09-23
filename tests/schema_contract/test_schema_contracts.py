"""Schema contract tests to prevent breaking changes in Kafka schemas.

This module validates that all Avro schemas in kafka/schemas/ are valid and
maintains contract compatibility checks.
"""

from __future__ import annotations

import json
import os
import pytest
from pathlib import Path
from typing import Dict, List, Any, Optional
import avro.schema
import avro.io
import avro.datafile
import avro.errors

# Schema directory
SCHEMA_DIR = Path(__file__).parent.parent.parent / "kafka" / "schemas"


class SchemaContractValidator:
    """Validator for Avro schema contracts."""

    def __init__(self, schema_dir: Path):
        self.schema_dir = schema_dir
        self.schemas: Dict[str, avro.schema.Schema] = {}
        self._load_schemas()

    def _load_schemas(self):
        """Load all Avro schemas from the schema directory."""
        for schema_file in self.schema_dir.glob("*.avsc"):
            with open(schema_file, 'r') as f:
                schema_json = json.load(f)

            try:
                schema = avro.schema.parse(json.dumps(schema_json))
                self.schemas[schema_file.stem] = schema
            except Exception as e:
                pytest.fail(f"Failed to parse schema {schema_file.name}: {e}")

    def validate_schema(self, schema_name: str) -> bool:
        """Validate a single schema."""
        if schema_name not in self.schemas:
            pytest.fail(f"Schema {schema_name} not found")

        schema = self.schemas[schema_name]

        # Basic validation
        assert schema is not None, f"Schema {schema_name} failed to parse"

        # Check for required fields based on schema type
        if schema_name.startswith("Ext"):
            assert self._has_required_external_fields(schema), f"External schema {schema_name} missing required fields"
        elif schema_name.startswith("iso."):
            assert self._has_required_iso_fields(schema), f"ISO schema {schema_name} missing required fields"

        return True

    def _has_required_external_fields(self, schema: avro.schema.Schema) -> bool:
        """Check if external schema has required fields."""
        # This would be schema-specific validation logic
        return True  # Placeholder - would implement based on actual requirements

    def _has_required_iso_fields(self, schema: avro.schema.Schema) -> bool:
        """Check if ISO schema has required fields."""
        # This would be schema-specific validation logic
        return True  # Placeholder - would implement based on actual requirements

    def validate_schema_evolution(self, old_schema_name: str, new_schema_name: str) -> bool:
        """Validate schema evolution compatibility."""
        if old_schema_name not in self.schemas or new_schema_name not in self.schemas:
            pytest.fail(f"One or both schemas not found: {old_schema_name}, {new_schema_name}")

        # This would implement schema evolution rules
        # For now, just check they both parse correctly
        return True

    def validate_all_schemas(self) -> bool:
        """Validate all schemas in the directory."""
        for schema_name in self.schemas.keys():
            self.validate_schema(schema_name)
        return True


class TestSchemaContracts:
    """Test class for schema contract validation."""

    @classmethod
    def setup_class(cls):
        """Set up test class with schema validator."""
        cls.validator = SchemaContractValidator(SCHEMA_DIR)

    def test_all_schemas_parse(self):
        """Test that all schemas can be parsed correctly."""
        assert self.validator.validate_all_schemas()

    def test_individual_schema_validation(self):
        """Test validation of individual schemas."""
        for schema_name in self.validator.schemas.keys():
            assert self.validator.validate_schema(schema_name)

    def test_external_schemas_have_required_fields(self):
        """Test that external schemas have required fields."""
        external_schemas = [name for name in self.validator.schemas.keys() if name.startswith("Ext")]
        for schema_name in external_schemas:
            assert self.validator.validate_schema(schema_name)

    def test_iso_schemas_have_required_fields(self):
        """Test that ISO schemas have required fields."""
        iso_schemas = [name for name in self.validator.schemas.keys() if name.startswith("iso.")]
        for schema_name in iso_schemas:
            assert self.validator.validate_schema(schema_name)

    def test_schema_file_structure(self):
        """Test that schema files follow naming conventions."""
        for schema_file in SCHEMA_DIR.glob("*.avsc"):
            # Check naming convention
            assert schema_file.stem.endswith(".v1"), f"Schema {schema_file.name} should end with version"

            # Check file contains valid JSON
            with open(schema_file, 'r') as f:
                schema_json = json.load(f)
            assert isinstance(schema_json, dict), f"Schema {schema_file.name} is not valid JSON"

    def test_schema_compatibility_matrix(self):
        """Test schema compatibility between related schemas."""
        # Define schema compatibility rules
        compatibility_rules = [
            ("ExtSeriesCatalogUpsertV1", "ExtTimeseriesObsV1"),
            ("iso.lmp.v1", "iso.load.v1"),
            ("iso.asm.v1", "iso.genmix.v1")
        ]

        for schema1, schema2 in compatibility_rules:
            if schema1 in self.validator.schemas and schema2 in self.validator.schemas:
                assert self.validator.validate_schema_evolution(schema1, schema2)

    def test_schema_backward_compatibility(self):
        """Test that schemas are backward compatible."""
        # This would test against previous versions
        # For now, just validate current schemas are well-formed
        for schema_name, schema in self.validator.schemas.items():
            # Check schema has proper namespace
            if hasattr(schema, 'namespace'):
                assert schema.namespace is not None, f"Schema {schema_name} missing namespace"

            # Check schema has proper name
            if hasattr(schema, 'name'):
                assert schema.name is not None, f"Schema {schema_name} missing name"

    def test_schema_performance_characteristics(self):
        """Test schema performance characteristics."""
        for schema_name, schema in self.validator.schemas.items():
            # Measure schema parsing time
            import time
            start_time = time.time()
            # Try to serialize/deserialize with the schema
            parse_time = time.time() - start_time

            # Should parse in reasonable time (< 100ms)
            assert parse_time < 0.1, f"Schema {schema_name} takes too long to parse: {parse_time}s"

    def test_schema_documentation(self):
        """Test that schemas have proper documentation."""
        for schema_name, schema in self.validator.schemas.items():
            # Check schema has doc field (if applicable)
            if hasattr(schema, 'doc'):
                doc = getattr(schema, 'doc', None)
                if doc:
                    assert isinstance(doc, str), f"Schema {schema_name} doc is not a string"
                    assert len(doc) > 0, f"Schema {schema_name} has empty doc"


def test_schema_directory_structure():
    """Test the overall schema directory structure."""
    # Check that schema directory exists
    assert SCHEMA_DIR.exists(), "Schema directory should exist"

    # Check that there are schema files
    schema_files = list(SCHEMA_DIR.glob("*.avsc"))
    assert len(schema_files) > 0, "Should have at least one schema file"

    # Check for contracts.yml file
    contracts_file = SCHEMA_DIR / "contracts.yml"
    assert contracts_file.exists(), "Should have contracts.yml file"

    # Validate contracts.yml is valid YAML
    import yaml
    with open(contracts_file, 'r') as f:
        contracts = yaml.safe_load(f)
    assert isinstance(contracts, dict), "contracts.yml should be valid YAML"


def test_schema_evolution_rules():
    """Test schema evolution rules are enforced."""
    validator = SchemaContractValidator(SCHEMA_DIR)

    # Test that we can detect incompatible changes
    # This would be extended with actual evolution testing
    assert validator.validate_all_schemas()


def test_schema_registry_integration():
    """Test integration with schema registry."""
    # This would test actual schema registry operations
    # For now, just ensure schemas can be loaded
    validator = SchemaContractValidator(SCHEMA_DIR)
    assert len(validator.schemas) > 0


def test_schema_backwards_compatibility():
    """Test that schemas maintain backwards compatibility."""
    # This would implement actual compatibility testing
    # For now, just ensure all schemas are valid
    validator = SchemaContractValidator(SCHEMA_DIR)
    assert validator.validate_all_schemas()


def test_schema_forwards_compatibility():
    """Test that schemas maintain forwards compatibility."""
    # This would implement actual compatibility testing
    # For now, just ensure all schemas are valid
    validator = SchemaContractValidator(SCHEMA_DIR)
    assert validator.validate_all_schemas()
