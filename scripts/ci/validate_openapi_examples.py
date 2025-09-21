#!/usr/bin/env python3
"""Validate OpenAPI specification examples and references."""

import json
import sys
from pathlib import Path
from typing import Any, Dict, List

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


def validate_examples_in_spec(spec: Dict[str, Any]) -> List[str]:
    """Validate that all examples in the specification are valid."""
    errors = []

    def validate_example(example: Any, path: str) -> List[str]:
        """Validate a single example."""
        example_errors = []

        if isinstance(example, dict):
            # Check for required example properties
            if 'value' not in example and 'summary' not in example:
                example_errors.append(f"Example at {path} should have 'value' or 'summary'")

            # Validate value if present
            if 'value' in example:
                value = example['value']
                if value is None:
                    example_errors.append(f"Example value at {path} should not be null")

                # Check for empty objects/arrays if that's not expected
                if isinstance(value, (dict, list)) and len(value) == 0:
                    example_errors.append(f"Example value at {path} is empty but should contain sample data")

        elif isinstance(example, (list, str, int, float, bool)):
            # Primitive examples are always valid
            pass

        else:
            example_errors.append(f"Invalid example type at {path}: {type(example)}")

        return example_errors

    def traverse_for_examples(obj: Any, path: str = "") -> List[str]:
        """Traverse the spec and validate examples."""
        all_errors = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key

                # Check if this is an example
                if key == 'example' or key == 'examples':
                    if key == 'example':
                        all_errors.extend(validate_example(value, current_path))
                    elif key == 'examples':
                        if isinstance(value, dict):
                            for example_name, example_value in value.items():
                                example_path = f"{current_path}.{example_name}"
                                all_errors.extend(validate_example(example_value, example_path))

                # Recurse into nested objects
                else:
                    all_errors.extend(traverse_for_examples(value, current_path))

        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                item_path = f"{path}[{i}]"
                all_errors.extend(traverse_for_examples(item, item_path))

        return all_errors

    return traverse_for_examples(spec)


def validate_schema_references(spec: Dict[str, Any]) -> List[str]:
    """Validate that all schema references are valid."""
    errors = []

    def find_references(obj: Any, path: str = "") -> List[str]:
        """Find all $ref references."""
        refs = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key

                if key == '$ref':
                    refs.append((current_path, str(value)))

                else:
                    refs.extend(find_references(value, current_path))

        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                item_path = f"{path}[{i}]"
                refs.extend(find_references(item, item_path))

        return refs

    def validate_reference(ref: str, ref_path: str) -> List[str]:
        """Validate a single reference."""
        validation_errors = []

        if not ref.startswith('#/'):
            # External reference - skip validation for now
            return []

        # Remove the #/ prefix
        ref_parts = ref[2:].split('/')

        # Navigate the spec to find the referenced component
        current = spec
        try:
            for part in ref_parts:
                if part in current:
                    current = current[part]
                else:
                    validation_errors.append(f"Reference {ref} at {ref_path} not found")
                    break
        except Exception as e:
            validation_errors.append(f"Error validating reference {ref} at {ref_path}: {e}")

        return validation_errors

    # Find all references
    all_refs = find_references(spec)

    # Validate each reference
    for ref_path, ref in all_refs:
        errors.extend(validate_reference(ref, ref_path))

    return errors


def validate_parameter_examples(spec: Dict[str, Any]) -> List[str]:
    """Validate parameter examples are appropriate."""
    errors = []
    paths = spec.get('paths', {})

    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue

        for method, operation in path_item.items():
            if not isinstance(operation, dict):
                continue

            parameters = operation.get('parameters', [])

            for param in parameters:
                param_name = param.get('name', 'unknown')
                param_in = param.get('in', 'unknown')

                # Check for examples in parameters
                if 'example' in param:
                    example = param['example']
                    param_path = f"{method.upper()} {path} parameter '{param_name}'"

                    # Validate example based on parameter type
                    schema = param.get('schema', {})
                    param_type = schema.get('type', 'string')

                    if param_type == 'string' and not isinstance(example, str):
                        errors.append(f"String parameter example should be string at {param_path}")
                    elif param_type == 'integer' and not isinstance(example, int):
                        errors.append(f"Integer parameter example should be integer at {param_path}")
                    elif param_type == 'number' and not isinstance(example, (int, float)):
                        errors.append(f"Number parameter example should be number at {param_path}")
                    elif param_type == 'boolean' and not isinstance(example, bool):
                        errors.append(f"Boolean parameter example should be boolean at {param_path}")
                    elif param_type == 'array' and not isinstance(example, list):
                        errors.append(f"Array parameter example should be array at {param_path}")

    return errors


def validate_response_examples(spec: Dict[str, Any]) -> List[str]:
    """Validate response examples are realistic."""
    errors = []
    paths = spec.get('paths', {})

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

                content = response.get('content', {})

                for content_type, content_obj in content.items():
                    examples = content_obj.get('examples', {})

                    if examples:
                        # Check that examples exist and are not empty
                        if not isinstance(examples, dict) or len(examples) == 0:
                            errors.append(f"Empty examples for {method.upper()} {path} {status_code}")
                        else:
                            # Validate example structure
                            for example_name, example in examples.items():
                                if not isinstance(example, dict) or 'value' not in example:
                                    errors.append(
                                        f"Invalid example '{example_name}' for {method.upper()} {path} {status_code}"
                                    )

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
        print(f"Validating examples in {file_path}...")
        spec = load_openapi_spec(file_path)

        if not spec:
            errors.append(f"Could not load specification from {file_path}")
            continue

        # Run all validation checks
        errors.extend(validate_examples_in_spec(spec))
        errors.extend(validate_schema_references(spec))
        errors.extend(validate_parameter_examples(spec))
        errors.extend(validate_response_examples(spec))

    if errors:
        print("\nExample validation errors found:")
        for error in errors:
            print(f"  - {error}")
        return 1

    print("All OpenAPI examples are valid!")
    return 0


if __name__ == '__main__':
    sys.exit(main())
