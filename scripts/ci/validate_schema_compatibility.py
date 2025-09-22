#!/usr/bin/env python3
"""Schema compatibility validation script for CI/CD pipeline."""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import requests


class SchemaValidator:
    """Validates schema compatibility and consistency."""

    def __init__(self, schema_registry_url: str = "http://localhost:8081"):
        """Initialize schema validator."""
        self.schema_registry_url = schema_registry_url
        self.schemas_dir = Path("kafka/schemas")

    def validate_schema_syntax(self) -> List[str]:
        """Validate syntax of all schema files."""
        errors = []
        schema_files = list(self.schemas_dir.glob("*.avsc")) + list(self.schemas_dir.glob("*.json"))

        for schema_file in schema_files:
            try:
                with open(schema_file, 'r') as f:
                    schema = json.load(f)

                # Basic validation
                if not isinstance(schema, dict):
                    errors.append(f"{schema_file}: Schema must be a JSON object")
                    continue

                required_fields = ['type', 'name']
                for field in required_fields:
                    if field not in schema:
                        errors.append(f"{schema_file}: Missing required field '{field}'")

                # Validate namespace if present
                if 'namespace' in schema and not schema['namespace']:
                    errors.append(f"{schema_file}: Namespace cannot be empty")

                # Validate field types
                if schema.get('type') == 'record' and 'fields' in schema:
                    for field in schema['fields']:
                        if not isinstance(field, dict) or 'name' not in field or 'type' not in field:
                            errors.append(f"{schema_file}: Invalid field definition: {field}")

                print(f"✓ Validated schema: {schema_file}")

            except json.JSONDecodeError as e:
                errors.append(f"{schema_file}: Invalid JSON - {e}")
            except Exception as e:
                errors.append(f"{schema_file}: Validation error - {e}")

        return errors

    def validate_schema_references(self) -> List[str]:
        """Validate that schema references are consistent."""
        errors = []
        schema_files = list(self.schemas_dir.glob("*.avsc"))

        # Build dependency graph
        dependencies = {}
        for schema_file in schema_files:
            dependencies[schema_file.name] = self._extract_dependencies(schema_file)

        # Check for circular dependencies and missing references
        visited = set()
        for schema_file in schema_files:
            try:
                self._validate_dependencies(schema_file, dependencies, visited, set())
            except RecursionError:
                errors.append(f"{schema_file}: Circular dependency detected")
            except FileNotFoundError as e:
                errors.append(str(e))

        return errors

    def _extract_dependencies(self, schema_file: Path) -> Set[str]:
        """Extract schema dependencies from a schema file."""
        try:
            with open(schema_file, 'r') as f:
                schema = json.load(f)

            dependencies = set()

            def find_refs(obj):
                if isinstance(obj, dict):
                    if obj.get('type') in ['record', 'enum', 'fixed']:
                        if 'name' in obj:
                            dependencies.add(obj['name'])
                    for value in obj.values():
                        find_refs(value)
                elif isinstance(obj, list):
                    for item in obj:
                        find_refs(item)

            find_refs(schema)
            return dependencies

        except Exception:
            return set()

    def _validate_dependencies(self, schema_file: Path, dependencies: Dict[str, Set[str]], visited: Set[str], path: Set[str]):
        """Recursively validate schema dependencies."""
        if schema_file.name in path:
            raise RecursionError(f"Circular dependency involving {schema_file}")

        if schema_file.name in visited:
            return

        visited.add(schema_file.name)
        path.add(schema_file.name)

        for dep in dependencies.get(schema_file.name, set()):
            if not any(f.name == dep for f in self.schemas_dir.glob("*.avsc")):
                raise FileNotFoundError(f"{schema_file}: References undefined schema '{dep}'")

        path.remove(schema_file.name)

    def validate_schema_evolution(self, base_version: str = "main") -> List[str]:
        """Validate schema evolution compatibility."""
        errors = []

        try:
            # Get current schemas from git
            os.system("git fetch origin")
            os.system(f"git checkout {base_version}")

            # Load base schemas
            base_schemas = {}
            for schema_file in self.schemas_dir.glob("*.avsc"):
                with open(schema_file, 'r') as f:
                    base_schemas[schema_file.name] = json.load(f)

            # Switch back to current branch
            os.system("git checkout -")

            # Compare with current schemas
            for schema_file in self.schemas_dir.glob("*.avsc"):
                if schema_file.name in base_schemas:
                    current_schema = json.load(open(schema_file, 'r'))
                    base_schema = base_schemas[schema_file.name]

                    # Check for breaking changes
                    breaking_changes = self._detect_breaking_changes(base_schema, current_schema)
                    if breaking_changes:
                        errors.extend([
                            f"{schema_file}: Breaking change detected - {change}"
                            for change in breaking_changes
                        ])

        except Exception as e:
            errors.append(f"Schema evolution validation failed: {e}")

        return errors

    def _detect_breaking_changes(self, old_schema: Dict, new_schema: Dict) -> List[str]:
        """Detect breaking changes between schema versions."""
        breaking_changes = []

        # Check for removed fields
        old_fields = {f['name'] for f in old_schema.get('fields', [])}
        new_fields = {f['name'] for f in new_schema.get('fields', [])}

        removed_fields = old_fields - new_fields
        if removed_fields:
            breaking_changes.append(f"Removed fields: {removed_fields}")

        # Check for type changes
        old_field_types = {
            f['name']: f['type']
            for f in old_schema.get('fields', [])
        }
        new_field_types = {
            f['name']: f['type']
            for f in new_schema.get('fields', [])
        }

        for field_name in old_field_types:
            if field_name in new_field_types:
                if not self._is_compatible_type(old_field_types[field_name], new_field_types[field_name]):
                    breaking_changes.append(f"Type change for field '{field_name}': {old_field_types[field_name]} -> {new_field_types[field_name]}")

        return breaking_changes

    def _is_compatible_type(self, old_type: str, new_type: str) -> bool:
        """Check if type change is compatible (non-breaking)."""
        # Simple type compatibility rules
        compatible_pairs = [
            ('string', 'string'),
            ('int', 'long'),
            ('long', 'long'),
            ('float', 'double'),
            ('double', 'double'),
            ('boolean', 'boolean'),
        ]

        # Allow null unions
        if isinstance(old_type, list) and isinstance(new_type, list):
            return set(old_type) <= set(new_type)

        return (old_type, new_type) in compatible_pairs

    def validate_schema_registry_compatibility(self) -> List[str]:
        """Validate schemas can be registered with Schema Registry."""
        errors = []

        try:
            # Test connection to Schema Registry
            response = requests.get(f"{self.schema_registry_url}/subjects", timeout=10)
            if response.status_code != 200:
                errors.append(f"Cannot connect to Schema Registry at {self.schema_registry_url}")
                return errors

            subjects = response.json()
            print(f"Found {len(subjects)} existing subjects in Schema Registry")

            # Validate each schema file
            for schema_file in self.schemas_dir.glob("*.avsc"):
                try:
                    with open(schema_file, 'r') as f:
                        schema = json.load(f)

                    # Determine subject name
                    subject = f"{schema_file.stem}-value"

                    # Test schema registration
                    register_url = f"{self.schema_registry_url}/subjects/{subject}/versions"
                    response = requests.post(
                        register_url,
                        json={"schema": json.dumps(schema)},
                        timeout=30
                    )

                    if response.status_code == 409:
                        # Schema already exists - check compatibility
                        print(f"Schema {subject} already exists, checking compatibility...")
                        compatibility_url = f"{self.schema_registry_url}/compatibility/subjects/{subject}/versions/latest"
                        response = requests.post(
                            compatibility_url,
                            json={"schema": json.dumps(schema)},
                            timeout=30
                        )

                        if response.status_code != 200:
                            errors.append(f"Schema {subject} is not compatible with existing version")

                    elif response.status_code != 200:
                        errors.append(f"Failed to register schema {subject}: {response.text}")

                    else:
                        schema_id = response.json().get('id')
                        print(f"✓ Successfully validated schema {subject} (ID: {schema_id})")

                except Exception as e:
                    errors.append(f"Error validating schema {schema_file}: {e}")

        except Exception as e:
            errors.append(f"Schema Registry validation failed: {e}")

        return errors

    def generate_validation_report(self, errors: List[str]) -> Dict:
        """Generate a comprehensive validation report."""
        schema_files = list(self.schemas_dir.glob("*.avsc")) + list(self.schemas_dir.glob("*.json"))

        return {
            "total_schemas": len(schema_files),
            "errors": errors,
            "error_count": len(errors),
            "validation_status": "PASS" if len(errors) == 0 else "FAIL",
            "validated_at": str(datetime.now()),
            "schema_registry_url": self.schema_registry_url
        }


def main():
    """Main validation function."""
    validator = SchemaValidator()

    all_errors = []

    print("🔍 Starting schema validation...")

    # 1. Validate syntax
    print("\n📝 Validating schema syntax...")
    syntax_errors = validator.validate_schema_syntax()
    all_errors.extend(syntax_errors)
    if syntax_errors:
        print(f"❌ Found {len(syntax_errors)} syntax errors")
        for error in syntax_errors[:5]:  # Show first 5 errors
            print(f"  - {error}")
    else:
        print("✅ All schemas have valid syntax")

    # 2. Validate references
    print("\n🔗 Validating schema references...")
    reference_errors = validator.validate_schema_references()
    all_errors.extend(reference_errors)
    if reference_errors:
        print(f"❌ Found {len(reference_errors)} reference errors")
        for error in reference_errors:
            print(f"  - {error}")
    else:
        print("✅ All schema references are valid")

    # 3. Validate Schema Registry compatibility
    print("\n📋 Validating Schema Registry compatibility...")
    registry_errors = validator.validate_schema_registry_compatibility()
    all_errors.extend(registry_errors)
    if registry_errors:
        print(f"❌ Found {len(registry_errors)} Schema Registry errors")
        for error in registry_errors:
            print(f"  - {error}")
    else:
        print("✅ All schemas are compatible with Schema Registry")

    # 4. Generate final report
    report = validator.generate_validation_report(all_errors)

    print("
📊 Validation Report:"    print(f"  Total schemas: {report['total_schemas']}")
    print(f"  Errors: {report['error_count']}")
    print(f"  Status: {report['validation_status']}")

    if report['error_count'] > 0:
        print(f"\n❌ Validation failed with {report['error_count']} errors")
        return 1

    print("\n✅ All validation checks passed!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
