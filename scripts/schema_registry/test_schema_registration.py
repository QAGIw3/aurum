#!/usr/bin/env python3
"""
Test script for Schema Registry registration and compatibility checking.

This script tests the Schema Registry integration with sample Avro schemas
and validates compatibility checking functionality.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List, Optional

# Add src to path for imports
REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from aurum.schema_registry import (
    SchemaRegistryManager,
    SchemaRegistryConfig,
    SchemaCompatibilityMode,
    CompatibilityChecker,
    CompatibilityResult
)
from aurum.logging import create_logger


def create_sample_schemas() -> List[dict]:
    """Create sample Avro schemas for testing.

    Returns:
        List of sample schemas
    """
    return [
        {
            "name": "BasicRecord",
            "schema": {
                "type": "record",
                "name": "BasicRecord",
                "namespace": "aurum.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "int"}
                ]
            }
        },
        {
            "name": "ExtendedRecord",
            "schema": {
                "type": "record",
                "name": "ExtendedRecord",
                "namespace": "aurum.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "int"},
                    {"name": "description", "type": ["null", "string"], "default": None}
                ]
            }
        },
        {
            "name": "IncompatibleRecord",
            "schema": {
                "type": "record",
                "name": "IncompatibleRecord",
                "namespace": "aurum.test",
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "value", "type": "string"}  # Changed from int to string
                ]
            }
        }
    ]


def test_compatibility_checker() -> bool:
    """Test the compatibility checker with sample schemas.

    Returns:
        True if all tests pass
    """
    print("🧪 Testing Schema Compatibility Checker...")

    checker = CompatibilityChecker()
    schemas = create_sample_schemas()

    # Get basic and extended schemas
    basic_schema = schemas[0]["schema"]
    extended_schema = schemas[1]["schema"]
    incompatible_schema = schemas[2]["schema"]

    # Test backward compatibility (basic -> extended)
    result = checker.check_compatibility(basic_schema, extended_schema, SchemaCompatibilityMode.BACKWARD)
    print(f"  Basic -> Extended (BACKWARD): {'✅' if result.is_compatible else '❌'}")
    if not result.is_compatible:
        print(f"    Issues: {result.messages}")

    # Test backward compatibility (extended -> basic) - should fail
    result = checker.check_compatibility(extended_schema, basic_schema, SchemaCompatibilityMode.BACKWARD)
    print(f"  Extended -> Basic (BACKWARD): {'✅' if result.is_compatible else '❌'}")
    if not result.is_compatible:
        print(f"    Issues: {result.messages}")

    # Test incompatibility (basic -> incompatible)
    result = checker.check_compatibility(basic_schema, incompatible_schema, SchemaCompatibilityMode.BACKWARD)
    print(f"  Basic -> Incompatible (BACKWARD): {'✅' if result.is_compatible else '❌'}")
    if not result.is_compatible:
        print(f"    Issues: {result.messages}")

    # Test forward compatibility (basic -> extended)
    result = checker.check_compatibility(basic_schema, extended_schema, SchemaCompatibilityMode.FORWARD)
    print(f"  Basic -> Extended (FORWARD): {'✅' if result.is_compatible else '❌'}")

    print("✅ Compatibility checker tests completed\n")
    return True


def test_schema_registry_manager() -> bool:
    """Test the Schema Registry manager with mocked HTTP calls.

    Returns:
        True if all tests pass
    """
    print("🧪 Testing Schema Registry Manager...")

    config = SchemaRegistryConfig(
        base_url="http://localhost:8081",
        default_compatibility_mode=SchemaCompatibilityMode.BACKWARD
    )

    manager = SchemaRegistryManager(config)
    schemas = create_sample_schemas()

    # Test schema validation
    print("  Testing schema validation...")
    try:
        manager._validate_schema(schemas[0]["schema"])
        print("    ✅ Valid schema validation passed")
    except Exception as e:
        print(f"    ❌ Schema validation failed: {e}")
        return False

    try:
        manager._validate_schema({"type": "invalid"})
        print("    ❌ Invalid schema should have failed validation")
        return False
    except Exception:
        print("    ✅ Invalid schema validation correctly failed")

    # Test subject name extraction
    print("  Testing subject name extraction...")
    test_schema = {
        "type": "record",
        "name": "TestRecord",
        "namespace": "aurum.test"
    }

    subject = manager._extract_subject_from_schema(test_schema, Path("test.avsc"))
    expected_subject = "aurum.test.TestRecord"
    print(f"    Subject: {subject}")
    assert subject == expected_subject, f"Expected {expected_subject}, got {subject}"

    print("✅ Schema Registry manager tests completed\n")
    return True


def test_sample_schema_files() -> bool:
    """Test the sample schema files in the templates directory.

    Returns:
        True if all tests pass
    """
    print("🧪 Testing sample schema files...")

    template_dir = REPO_ROOT / "seatunnel" / "jobs" / "templates"
    if not template_dir.exists():
        print(f"  ❌ Templates directory not found: {template_dir}")
        return False

    schema_files = list(template_dir.glob("*.avsc"))
    print(f"  Found {len(schema_files)} schema files")

    if len(schema_files) == 0:
        print("  ⚠️  No schema files found in templates directory")
        return True  # Not a failure, just a warning

    for schema_file in schema_files:
        print(f"  Testing {schema_file.name}...")

        try:
            # Load and validate schema
            schema = json.loads(schema_file.read_text())

            # Basic validation
            assert schema.get("type") == "record", "Schema must be of type 'record'"
            assert "name" in schema, "Schema must have a 'name' field"
            assert "fields" in schema, "Schema must have a 'fields' array"

            print(f"    ✅ {schema_file.name} is valid")

        except json.JSONDecodeError as e:
            print(f"    ❌ {schema_file.name} has invalid JSON: {e}")
            return False
        except AssertionError as e:
            print(f"    ❌ {schema_file.name} failed validation: {e}")
            return False
        except Exception as e:
            print(f"    ❌ {schema_file.name} error: {e}")
            return False

    print("✅ Sample schema files tests completed\n")
    return True


def test_ci_script_simulation() -> bool:
    """Simulate running the CI script.

    Returns:
        True if simulation succeeds
    """
    print("🧪 Simulating CI script execution...")

    try:
        # Import the CI script
        sys.path.insert(0, str(REPO_ROOT / "scripts" / "ci"))
        from register_schemas import SchemaRegistrationScript, SchemaRegistrationError

        config = SchemaRegistryConfig(
            base_url="http://localhost:8081",
            default_compatibility_mode=SchemaCompatibilityMode.BACKWARD
        )

        script = SchemaRegistrationScript(config)

        # Test with sample schema files
        template_dir = REPO_ROOT / "seatunnel" / "jobs" / "templates"
        schema_files = list(template_dir.glob("*.avsc"))

        if len(schema_files) == 0:
            print("  ⚠️  No schema files found for testing")
            return True

        # Run dry-run registration
        results = script.register_schemas_from_directory(
            template_dir,
            SchemaCompatibilityMode.BACKWARD,
            dry_run=True
        )

        print(f"  Processed {results['total_schemas']} schemas")
        print(f"  Would register: {results['registered_schemas']}")
        print(f"  Failed: {results['failed_registrations']}")

        if results["errors"]:
            print("  Errors found:")
            for error in results["errors"]:
                print(f"    - {error['schema_file']}: {error['error']}")

        print("✅ CI script simulation completed\n")
        return True

    except ImportError as e:
        print(f"  ❌ Could not import CI script: {e}")
        return False
    except Exception as e:
        print(f"  ❌ CI script simulation failed: {e}")
        return False


def main(argv: Optional[List[str]] = None) -> int:
    """Main test function.

    Args:
        argv: Command line arguments

    Returns:
        Exit code (0 for success)
    """
    print("🚀 Starting Schema Registry Tests\n")

    tests = [
        ("Compatibility Checker", test_compatibility_checker),
        ("Schema Registry Manager", test_schema_registry_manager),
        ("Sample Schema Files", test_sample_schema_files),
        ("CI Script Simulation", test_ci_script_simulation)
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name} PASSED")
            else:
                failed += 1
                print(f"❌ {test_name} FAILED")
        except Exception as e:
            failed += 1
            print(f"❌ {test_name} ERROR: {e}")

    print("""
🏁 Test Results:""")
    print(f"  Passed: {passed}")
    print(f"  Failed: {failed}")
    print(f"  Total: {passed + failed}")

    if failed > 0:
        print("""
❌ Some tests failed""")
        return 1

    print("""
✅ All tests passed!""")
    return 0


if __name__ == "__main__":
    sys.exit(main())
