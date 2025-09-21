#!/usr/bin/env python3
"""Validate OpenAPI specification consistency and best practices."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Set

import yaml


def load_openapi_spec(file_path: Path) -> Dict[str, Any]:
    """Load OpenAPI specification from YAML file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            if file_path.suffix in ['.yaml', '.yml']:
                return yaml.safe_load(f)
            else:
                return json.load(f)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return {}


def validate_paths_consistency(spec: Dict[str, Any]) -> List[str]:
    """Validate consistency across paths."""
    errors = []
    paths = spec.get('paths', {})

    if not paths:
        errors.append("No paths defined in specification")
        return errors

    # Check for duplicate operationIds
    operation_ids: Set[str] = set()
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            operation_id = operation.get('operationId')
            if operation_id:
                if operation_id in operation_ids:
                    errors.append(f"Duplicate operationId: {operation_id}")
                operation_ids.add(operation_id)

    return errors


def validate_response_consistency(spec: Dict[str, Any]) -> List[str]:
    """Validate response consistency across operations."""
    errors = []
    paths = spec.get('paths', {})

    # Collect all response schemas by status code
    response_schemas = {}

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            responses = operation.get('responses', {})
            for status_code, response in responses.items():
                if not isinstance(response, dict):
                    continue

                schema = response.get('schema')
                if schema:
                    key = f"{method.upper()}_{status_code}"
                    if key not in response_schemas:
                        response_schemas[key] = []
                    response_schemas[key].append((path, schema))

    # Check for inconsistencies in common response patterns
    for key, schemas in response_schemas.items():
        if len(schemas) > 1:
            # Check if all schemas have the same structure
            first_schema = schemas[0][1]
            for path, schema in schemas[1:]:
                if not _schemas_equivalent(first_schema, schema):
                    errors.append(f"Inconsistent response schema for {key} at {path}")

    return errors


def _schemas_equivalent(schema1: Dict[str, Any], schema2: Dict[str, Any]) -> bool:
    """Check if two schemas are equivalent."""
    def normalize_schema(schema: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize schema for comparison."""
        normalized = {}
        for key, value in schema.items():
            if key == 'properties':
                # Sort properties for consistent comparison
                if isinstance(value, dict):
                    normalized[key] = dict(sorted(value.items()))
                else:
                    normalized[key] = value
            elif key == 'required':
                # Sort required fields
                if isinstance(value, list):
                    normalized[key] = sorted(value)
                else:
                    normalized[key] = value
            else:
                normalized[key] = value
        return normalized

    return normalize_schema(schema1) == normalize_schema(schema2)


def validate_parameter_consistency(spec: Dict[str, Any]) -> List[str]:
    """Validate parameter consistency across similar operations."""
    errors = []
    paths = spec.get('paths', {})

    # Group operations by path pattern
    path_patterns = {}

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        # Extract path parameters
        path_params = set()
        for part in path.split('/'):
            if part.startswith('{') and part.endswith('}'):
                path_params.add(part[1:-1])

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            parameters = operation.get('parameters', [])
            query_params = []

            for param in parameters:
                if param.get('in') == 'query':
                    query_params.append(param.get('name'))

            pattern_key = f"{method.upper()}_{len(path_params)}"
            if pattern_key not in path_patterns:
                path_patterns[pattern_key] = []

            path_patterns[pattern_key].append((path, query_params))

    # Check for inconsistencies within patterns
    for pattern, operations in path_patterns.items():
        if len(operations) > 1:
            first_path, first_params = operations[0]
            for path, params in operations[1:]:
                if set(first_params) != set(params):
                    errors.append(
                        f"Inconsistent query parameters for pattern {pattern}: "
                        f"{first_path} has {first_params}, {path} has {params}"
                    )

    return errors


def validate_security_consistency(spec: Dict[str, Any]) -> List[str]:
    """Validate security scheme consistency."""
    errors = []
    paths = spec.get('paths', {})

    # Check if operations use security schemes that are defined
    security_schemes = spec.get('components', {}).get('securitySchemes', {})

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            security = operation.get('security', [])
            if security:
                for sec_req in security:
                    for scheme_name in sec_req.keys():
                        if scheme_name not in security_schemes:
                            errors.append(
                                f"Operation {method.upper()} {path} references undefined security scheme: {scheme_name}"
                            )

    return errors


def validate_examples(spec: Dict[str, Any]) -> List[str]:
    """Validate that examples in the specification are valid."""
    errors = []
    paths = spec.get('paths', {})

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            # Check request body examples
            request_body = operation.get('requestBody')
            if request_body and isinstance(request_body, dict):
                content = request_body.get('content', {})
                for content_type, content_obj in content.items():
                    if 'examples' in content_obj:
                        examples = content_obj['examples']
                        if not isinstance(examples, dict) or not examples:
                            errors.append(f"Invalid examples in {method.upper()} {path}")

            # Check response examples
            responses = operation.get('responses', {})
            for status_code, response in responses.items():
                if isinstance(response, dict):
                    content = response.get('content', {})
                    for content_type, content_obj in content.items():
                        if 'examples' in content_obj:
                            examples = content_obj['examples']
                            if not isinstance(examples, dict) or not examples:
                                errors.append(f"Invalid examples in {method.upper()} {path} {status_code}")

    return errors


def main() -> int:
    """Main validation function."""
    errors = []

    # Find OpenAPI files
    openapi_dir = Path('openapi')
    if not openapi_dir.exists():
        print("No openapi directory found")
        return 1

    openapi_files = list(openapi_dir.glob('**/*.yaml')) + list(openapi_dir.glob('**/*.yml'))

    if not openapi_files:
        print("No OpenAPI specification files found")
        return 1

    for file_path in openapi_files:
        print(f"Validating {file_path}...")
        spec = load_openapi_spec(file_path)

        if not spec:
            errors.append(f"Could not load specification from {file_path}")
            continue

        # Run all validation checks
        errors.extend(validate_paths_consistency(spec))
        errors.extend(validate_response_consistency(spec))
        errors.extend(validate_parameter_consistency(spec))
        errors.extend(validate_security_consistency(spec))
        errors.extend(validate_examples(spec))

    if errors:
        print("\nValidation errors found:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("All OpenAPI specifications are consistent and valid!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
