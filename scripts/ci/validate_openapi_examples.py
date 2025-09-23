#!/usr/bin/env python3
"""Validate examples in OpenAPI specifications."""

import json
import sys
import yaml
from pathlib import Path
from typing import Any, Dict


def load_openapi_spec(file_path: str) -> dict:
    """Load OpenAPI specification from YAML or JSON file."""
    with open(file_path) as f:
        if file_path.endswith('.json'):
            return json.load(f)
        else:
            return yaml.safe_load(f)


def validate_json_schema(schema: dict, data: Any) -> bool:
    """Validate data against JSON schema."""
    # Simple validation - could be enhanced with jsonschema library
    try:
        if schema.get('type') == 'object':
            if not isinstance(data, dict):
                return False
            required = schema.get('required', [])
            for field in required:
                if field not in data:
                    return False
        elif schema.get('type') == 'array':
            if not isinstance(data, list):
                return False
        elif schema.get('type') == 'string':
            if not isinstance(data, str):
                return False
        elif schema.get('type') == 'integer':
            if not isinstance(data, int):
                return False
        elif schema.get('type') == 'number':
            if not isinstance(data, (int, float)):
                return False
        elif schema.get('type') == 'boolean':
            if not isinstance(data, bool):
                return False
        return True
    except Exception:
        return False


def validate_example_against_schema(example: Any, schema: dict) -> bool:
    """Validate an example against its schema."""
    if not schema:
        return True  # No schema to validate against

    if schema.get('type') == 'object' and '$ref' in schema:
        # Handle schema references - simplified for now
        return True

    return validate_json_schema(schema, example)


def extract_examples_from_spec(spec: dict) -> Dict[str, Any]:
    """Extract all examples from an OpenAPI specification."""
    examples = {}

    def traverse_schema(schema: Any, path: str = "") -> None:
        if isinstance(schema, dict):
            if 'example' in schema:
                examples[path] = schema['example']

            # Check for examples in responses
            if 'responses' in schema:
                for status_code, response in schema['responses'].items():
                    if isinstance(response, dict):
                        content = response.get('content', {})
                        for content_type, content_schema in content.items():
                            if 'example' in content_schema:
                                examples[f"{path}/responses/{status_code}"] = content_schema['example']

            # Check for examples in request bodies
            if 'requestBody' in schema:
                request_body = schema['requestBody']
                if isinstance(request_body, dict):
                    content = request_body.get('content', {})
                    for content_type, content_schema in content.items():
                        if 'example' in content_schema:
                            examples[f"{path}/requestBody"] = content_schema['example']

            for key, value in schema.items():
                traverse_schema(value, f"{path}/{key}")

        elif isinstance(schema, list):
            for i, item in enumerate(schema):
                traverse_schema(item, f"{path}[{i}]")

    # Start traversal from the main paths
    paths = spec.get('paths', {})
    for path, path_item in paths.items():
        traverse_schema(path_item, f"paths{path}")

    return examples


def find_schemas_for_examples(spec: dict) -> Dict[str, Any]:
    """Find schemas corresponding to examples."""
    schemas = {}

    def traverse_schema_for_schemas(schema: Any, path: str = "") -> None:
        if isinstance(schema, dict):
            # Check if this schema has an example
            if 'example' in schema:
                schemas[path] = schema

            for key, value in schema.items():
                traverse_schema_for_schemas(value, f"{path}/{key}")

        elif isinstance(schema, list):
            for i, item in enumerate(schema):
                traverse_schema_for_schemas(item, f"{path}[{i}]")

    paths = spec.get('paths', {})
    for path, path_item in paths.items():
        traverse_schema_for_schemas(path_item, f"paths{path}")

    return schemas


def main():
    """Main function to validate OpenAPI examples."""
    print("🔍 Validating examples in OpenAPI specifications...")

    # Find OpenAPI files
    openapi_files = []
    for pattern in ['openapi/**/*.yaml', 'openapi/**/*.yml']:
        openapi_files.extend(Path('.').glob(pattern))

    if not openapi_files:
        print("⚠️ No OpenAPI files found")
        return 0

    all_valid = True

    for file_path in openapi_files:
        print(f"\n📋 Validating {file_path}...")

        try:
            spec = load_openapi_spec(str(file_path))
        except Exception as e:
            print(f"❌ Error loading {file_path}: {e}")
            all_valid = False
            continue

        examples = extract_examples_from_spec(spec)
        schemas = find_schemas_for_examples(spec)

        print(f"Found {len(examples)} examples")

        for example_path, example in examples.items():
            print(f"  Validating example: {example_path}")

            # Find corresponding schema
            schema_path = example_path.rsplit('/example', 1)[0] if '/example' in example_path else example_path
            schema = schemas.get(schema_path)

            if schema:
                if validate_example_against_schema(example, schema):
                    print(f"    ✅ {example_path}: Valid example")
                else:
                    print(f"    ❌ {example_path}: Invalid example")
                    all_valid = False
            else:
                print(f"    ⚠️ {example_path}: No schema found for validation")

    if all_valid:
        print("\n✅ All OpenAPI examples are valid")
        return 0
    else:
        print("\n❌ Some OpenAPI examples are invalid")
        return 1


if __name__ == "__main__":
    sys.exit(main())