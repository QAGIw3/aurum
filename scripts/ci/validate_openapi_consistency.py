#!/usr/bin/env python3
"""Validate OpenAPI specification consistency (legacy script - use validate_openapi_complete.py for comprehensive validation)."""

import json
import sys
import yaml
from pathlib import Path
from typing import Dict, List, Set

# Import comprehensive validation
from validate_openapi_complete import main as validate_comprehensive

def main():
    """Main validation function."""
    print("🔄 Running legacy OpenAPI consistency validation...")
    print("⚠️  This script is deprecated. Use 'scripts/ci/validate_openapi_complete.py' for comprehensive validation.")

    # Run comprehensive validation
    return validate_comprehensive()


def load_openapi_spec(file_path: str) -> dict:
    """Load OpenAPI specification from YAML or JSON file."""
    with open(file_path) as f:
        if file_path.endswith('.json'):
            return json.load(f)
        else:
            return yaml.safe_load(f)


def validate_openapi_structure(spec: dict, file_path: str) -> bool:
    """Validate the basic structure of an OpenAPI specification."""
    print(f"Validating {file_path}...")

    # Check required fields
    required_fields = ['openapi', 'info', 'paths']
    for field in required_fields:
        if field not in spec:
            print(f"❌ Missing required field: {field}")
            return False

    # Check OpenAPI version
    version = spec.get('openapi', '')
    if not version.startswith('3.'):
        print(f"❌ Unsupported OpenAPI version: {version}")
        return False

    # Check info section
    info = spec.get('info', {})
    if not info.get('title'):
        print("❌ Missing title in info section")
        return False

    if not info.get('version'):
        print("❌ Missing version in info section")
        return False

    # Check paths
    paths = spec.get('paths', {})
    if not paths:
        print("❌ No paths defined")
        return False

    # Validate each path
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            print(f"❌ Invalid path item for {path}")
            return False

        # Check for at least one HTTP method
        http_methods = ['get', 'post', 'put', 'patch', 'delete', 'head', 'options']
        has_method = any(method in path_item for method in http_methods)

        if not has_method:
            print(f"❌ No HTTP methods defined for path {path}")
            return False

        # Validate each method
        for method, operation in path_item.items():
            if method not in http_methods:
                continue

            if not isinstance(operation, dict):
                print(f"❌ Invalid operation definition for {method} {path}")
                return False

            # Check for responses
            if 'responses' not in operation:
                print(f"❌ Missing responses for {method} {path}")
                return False

            responses = operation.get('responses', {})
            if not responses:
                print(f"❌ No responses defined for {method} {path}")
                return False

    print(f"✅ {file_path} structure is valid")
    return True


def check_consistency_across_specs(specs: List[Tuple[str, dict]]) -> bool:
    """Check consistency across multiple OpenAPI specifications."""
    print("\n🔍 Checking consistency across specifications...")

    # Check for consistent naming conventions
    all_paths = set()
    for file_path, spec in specs:
        paths = spec.get('paths', {})
        for path in paths.keys():
            all_paths.add(path)

    # Check for conflicting path definitions
    path_conflicts = {}
    for file_path, spec in specs:
        for path in spec.get('paths', {}):
            if path in path_conflicts:
                path_conflicts[path].append(file_path)
            else:
                path_conflicts[path] = [file_path]

    conflicts = {path: files for path, files in path_conflicts.items() if len(files) > 1}
    if conflicts:
        print("❌ Path conflicts found:")
        for path, files in conflicts.items():
            print(f"  {path}: {', '.join(files)}")
        return False

    print("✅ No path conflicts found")
    return True


def validate_response_schemas(specs: List[Tuple[str, dict]]) -> bool:
    """Validate that response schemas are properly defined."""
    print("\n🔍 Validating response schemas...")

    all_valid = True

    for file_path, spec in specs:
        paths = spec.get('paths', {})

        for path, path_item in paths.items():
            for method, operation in path_item.items():
                if method not in ['get', 'post', 'put', 'patch', 'delete']:
                    continue

                responses = operation.get('responses', {})

                # Check for success responses
                has_success = any(str(code).startswith('2') for code in responses.keys())
                if not has_success:
                    print(f"❌ No success responses for {method} {path} in {file_path}")
                    all_valid = False

                # Check for error responses
                has_error = any(str(code).startswith('4') or str(code).startswith('5') for code in responses.keys())
                if not has_error:
                    print(f"❌ No error responses for {method} {path} in {file_path}")
                    all_valid = False

    if all_valid:
        print("✅ All response schemas are properly defined")
    return all_valid


if __name__ == "__main__":
    sys.exit(main())