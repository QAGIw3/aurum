#!/usr/bin/env python3
"""Check Avro schema compatibility between versions."""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

import avro.schema


def load_avro_schema(file_path: str) -> avro.schema.Schema:
    """Load an Avro schema from file."""
    with open(file_path) as f:
        schema_dict = json.load(f)
    return avro.schema.SchemaFromJSONData(schema_dict)


def get_schema_fingerprint(schema: avro.schema.Schema) -> str:
    """Get a fingerprint of the schema for comparison."""
    return schema.to_json()


def check_backward_compatibility(old_schema: avro.schema.Schema, new_schema: avro.schema.Schema) -> bool:
    """Check if new schema is backward compatible with old schema."""
    try:
        # Simple compatibility check - this could be enhanced with more sophisticated logic
        old_json = json.loads(old_schema.to_json())
        new_json = json.loads(new_schema.to_json())

        # Check if required fields are still required
        old_required = set()
        new_required = set()

        def extract_required_fields(schema_dict: dict, path: str = "") -> Set[str]:
            required = set()
            if isinstance(schema_dict, dict):
                if schema_dict.get("type") == "record":
                    record_name = schema_dict.get("name", "")
                    for field in schema_dict.get("fields", []):
                        field_name = field.get("name", "")
                        if not field.get("default", False) and not field.get("default", {}).get("value"):
                            required.add(f"{record_name}.{field_name}")
            return required

        old_required = extract_required_fields(old_json)
        new_required = extract_required_fields(new_json)

        # Check for removed required fields
        removed_required = old_required - new_required
        if removed_required:
            print(f"❌ Breaking change: Required fields removed: {removed_required}")
            return False

        return True

    except Exception as e:
        print(f"❌ Error checking compatibility: {e}")
        return False


def find_avro_schemas() -> List[Path]:
    """Find all Avro schema files in the repository."""
    schema_dirs = [
        Path("kafka/schemas"),
        Path("seatunnel/jobs/templates"),
    ]

    schemas = []
    for schema_dir in schema_dirs:
        if schema_dir.exists():
            schemas.extend(schema_dir.glob("**/*.avsc"))

    return schemas


def main():
    """Main function to check Avro compatibility."""
    print("🔍 Checking Avro schema compatibility...")

    schemas = find_avro_schemas()

    if not schemas:
        print("⚠️ No Avro schema files found")
        return 0

    print(f"Found {len(schemas)} schema files")

    # Group schemas by name for comparison
    schemas_by_name = {}
    for schema_path in schemas:
        try:
            schema = load_avro_schema(str(schema_path))
            schema_name = schema.name if hasattr(schema, 'name') else schema_path.stem
            if schema_name not in schemas_by_name:
                schemas_by_name[schema_name] = []
            schemas_by_name[schema_name].append((schema_path, schema))
        except Exception as e:
            print(f"❌ Error loading schema {schema_path}: {e}")
            return 1

    # Check compatibility for each schema group
    all_compatible = True
    for schema_name, schema_list in schemas_by_name.items():
        if len(schema_list) > 1:
            print(f"\n📋 Checking compatibility for {schema_name}...")

            # Sort by path to get a consistent order
            schema_list.sort(key=lambda x: x[0])

            for i in range(1, len(schema_list)):
                old_path, old_schema = schema_list[i-1]
                new_path, new_schema = schema_list[i]

                print(f"  Comparing {old_path} → {new_path}")

                if not check_backward_compatibility(old_schema, new_schema):
                    print(f"❌ Incompatible change in {schema_name}")
                    all_compatible = False
                else:
                    print(f"✅ Compatible change in {schema_name}")

    if all_compatible:
        print("\n✅ All Avro schemas are backward compatible")
        return 0
    else:
        print("\n❌ Some Avro schemas have breaking changes")
        return 1


if __name__ == "__main__":
    sys.exit(main())
