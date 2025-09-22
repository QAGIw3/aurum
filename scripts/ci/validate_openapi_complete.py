#!/usr/bin/env python3
"""Comprehensive OpenAPI specification validation including Spectral rules."""

import json
import subprocess
import sys
import tempfile
import yaml
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def run_spectral_validation(file_path: str) -> Tuple[bool, List[str]]:
    """Run Spectral validation against OpenAPI spec."""
    try:
        result = subprocess.run(
            ["spectral", "lint", file_path, "--format", "json"],
            capture_output=True,
            text=True,
            check=False
        )

        if result.returncode == 0:
            return True, []

        # Parse Spectral JSON output
        errors = []
        try:
            spectral_results = json.loads(result.stdout)
            for issue in spectral_results:
                errors.append(f"❌ {issue['code']}: {issue['message']} at {issue['path']}")
        except json.JSONDecodeError:
            errors.append(f"❌ Spectral validation failed: {result.stdout}")

        return False, errors

    except FileNotFoundError:
        return True, ["⚠️  Spectral not installed - skipping style validation"]
    except Exception as e:
        return False, [f"❌ Spectral validation error: {str(e)}"]


def validate_schema_structure(spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Validate OpenAPI schema structure and best practices."""
    errors = []
    warnings = []

    print(f"🔍 Validating schema structure: {file_path}")

    # Required fields
    required_fields = ['openapi', 'info', 'paths', 'components']
    for field in required_fields:
        if field not in spec:
            errors.append(f"❌ Missing required field: {field}")

    if 'openapi' in spec:
        version = spec['openapi']
        if not version.startswith('3.0') and not version.startswith('3.1'):
            errors.append(f"❌ Unsupported OpenAPI version: {version}")

    # Info section validation
    info = spec.get('info', {})
    required_info = ['title', 'version']
    for field in required_info:
        if field not in info:
            errors.append(f"❌ Missing required info field: {field}")

    # Paths validation
    paths = spec.get('paths', {})
    if not paths:
        errors.append("❌ No paths defined")
    else:
        for path, path_item in paths.items():
            if not isinstance(path_item, dict):
                errors.append(f"❌ Invalid path item for {path}")
                continue

            for method, operation in path_item.items():
                if method.upper() not in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'OPTIONS', 'HEAD']:
                    continue

                # Check for required operation fields
                if 'responses' not in operation:
                    errors.append(f"❌ Missing responses for {method} {path}")

                # Check for 400, 401, 403, 404, 429, 500 responses
                responses = operation.get('responses', {})
                expected_codes = ['400', '401', '403', '404', '429', '500']
                for code in expected_codes:
                    if code not in responses:
                        warnings.append(f"⚠️  Missing {code} response for {method} {path}")

    # Components validation
    components = spec.get('components', {})
    if 'schemas' not in components:
        warnings.append("⚠️  No reusable schemas defined")

    # Security schemes
    if 'securitySchemes' not in components:
        warnings.append("⚠️  No security schemes defined")

    return len(errors) == 0, errors + warnings


def validate_examples(spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Validate that all schemas have examples."""
    errors = []
    warnings = []

    print(f"📝 Validating examples in: {file_path}")

    components = spec.get('components', {})
    schemas = components.get('schemas', {})

    for schema_name, schema in schemas.items():
        if 'example' not in schema and 'examples' not in schema:
            warnings.append(f"⚠️  No example provided for schema: {schema_name}")

    # Check operation examples
    paths = spec.get('paths', {})
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                if 'requestBody' in operation:
                    request_body = operation['requestBody']
                    if 'content' in request_body:
                        for content_type, content in request_body['content'].items():
                            if 'schema' in content and 'example' not in content['schema']:
                                warnings.append(f"⚠️  No example for request body in {method} {path}")

    return len(errors) == 0, errors + warnings


def validate_security_consistency(spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Validate security scheme consistency."""
    errors = []
    warnings = []

    print(f"🔒 Validating security consistency: {file_path}")

    components = spec.get('components', {})
    security_schemes = components.get('securitySchemes', {})

    if not security_schemes:
        warnings.append("⚠️  No security schemes defined")
        return True, warnings

    # Check that all operations have security defined
    paths = spec.get('paths', {})
    for path, path_item in paths.items():
        for method, operation in path_item.items():
            if method.upper() in ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']:
                if 'security' not in operation:
                    warnings.append(f"⚠️  No security defined for {method} {path}")

    return len(errors) == 0, errors + warnings


def generate_openapi_artifacts(spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Generate JSON and YAML artifacts from OpenAPI spec."""
    messages = []

    print(f"📦 Generating artifacts from: {file_path}")

    # Generate JSON version
    json_path = file_path.replace('.yaml', '.json').replace('.yml', '.json')
    try:
        with open(json_path, 'w') as f:
            json.dump(spec, f, indent=2)
        messages.append(f"✅ Generated JSON spec: {json_path}")
    except Exception as e:
        messages.append(f"❌ Failed to generate JSON spec: {str(e)}")
        return False, messages

    # Generate YAML version from JSON (for consistency check)
    yaml_path = file_path.replace('.json', '.yaml')
    try:
        with open(yaml_path, 'w') as f:
            yaml.dump(spec, f, default_flow_style=False, sort_keys=False)
        messages.append(f"✅ Generated YAML spec: {yaml_path}")
    except Exception as e:
        messages.append(f"❌ Failed to generate YAML spec: {str(e)}")
        return False, messages

    return True, messages


def validate_schema_diff(base_spec: dict, new_spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Validate that schema changes are not breaking."""
    errors = []
    warnings = []

    print(f"🔄 Validating schema diff: {file_path}")

    # Check for removed paths
    base_paths = set(base_spec.get('paths', {}).keys())
    new_paths = set(new_spec.get('paths', {}).keys())

    removed_paths = base_paths - new_paths
    for path in removed_paths:
        errors.append(f"❌ Breaking change: Removed path {path}")

    # Check for removed operations
    for path in base_paths.intersection(new_paths):
        base_ops = set(base_spec['paths'][path].keys())
        new_ops = set(new_spec['paths'][path].keys())

        removed_ops = base_ops - new_ops
        for op in removed_ops:
            errors.append(f"❌ Breaking change: Removed {op} operation from {path}")

    # Check for removed response codes
    for path in base_paths.intersection(new_paths):
        for op in base_ops.intersection(new_ops):
            base_responses = set(base_spec['paths'][path][op].get('responses', {}).keys())
            new_responses = set(new_spec['paths'][path][op].get('responses', {}).keys())

            removed_responses = base_responses - new_responses
            for response in removed_responses:
                errors.append(f"❌ Breaking change: Removed {response} response from {op} {path}")

    return len(errors) == 0, errors + warnings


def run_schemathesis_validation(file_path: str) -> Tuple[bool, List[str]]:
    """Run Schemathesis validation against OpenAPI spec."""
    try:
        import subprocess
        result = subprocess.run(
            ["schemathesis", "run", file_path, "--format", "json"],
            capture_output=True,
            text=True,
            check=False,
            timeout=300  # 5 minute timeout
        )

        if result.returncode == 0:
            return True, ["✅ Schemathesis validation passed"]

        # Parse Schemathesis JSON output
        errors = []
        try:
            schemathesis_results = json.loads(result.stdout)
            for test in schemathesis_results.get("tests", []):
                if test.get("status") == "failure":
                    errors.append(f"❌ Schemathesis test failed: {test.get('name', 'Unknown')}")
        except json.JSONDecodeError:
            errors.append(f"❌ Schemathesis validation failed: {result.stdout}")

        return False, errors

    except FileNotFoundError:
        return True, ["⚠️  Schemathesis not installed - skipping property-based testing"]
    except subprocess.TimeoutExpired:
        return False, ["❌ Schemathesis validation timed out"]
    except Exception as e:
        return False, [f"❌ Schemathesis validation error: {str(e)}"]


def validate_schema_drift(base_spec: dict, new_spec: dict, file_path: str) -> Tuple[bool, List[str]]:
    """Validate that schema changes don't introduce breaking changes."""
    errors = []
    warnings = []

    print(f"🔄 Validating schema drift: {file_path}")

    # Check for removed paths (breaking change)
    base_paths = set(base_spec.get('paths', {}).keys())
    new_paths = set(new_spec.get('paths', {}).keys())

    removed_paths = base_paths - new_paths
    for path in removed_paths:
        errors.append(f"❌ BREAKING: Removed path {path}")

    # Check for removed operations
    for path in base_paths.intersection(new_paths):
        base_ops = set(base_spec['paths'][path].keys())
        new_ops = set(new_spec['paths'][path].keys())

        removed_ops = base_ops - new_ops
        for op in removed_ops:
            errors.append(f"❌ BREAKING: Removed {op} operation from {path}")

    # Check for removed response codes
    for path in base_paths.intersection(new_paths):
        for op in base_ops.intersection(new_ops):
            base_responses = set(base_spec['paths'][path][op].get('responses', {}).keys())
            new_responses = set(new_spec['paths'][path][op].get('responses', {}).keys())

            removed_responses = base_responses - new_responses
            for response in removed_responses:
                errors.append(f"❌ BREAKING: Removed {response} response from {op} {path}")

    # Check for changed required parameters
    for path in base_paths.intersection(new_paths):
        for op in base_ops.intersection(new_ops):
            base_params = base_spec['paths'][path][op].get('parameters', [])
            new_params = new_spec['paths'][path][op].get('parameters', [])

            base_required = {p.get('name') for p in base_params if p.get('required', False)}
            new_required = {p.get('name') for p in new_params if p.get('required', False)}

            new_required_params = new_required - base_required
            if new_required_params:
                warnings.append(f"⚠️  ADDED: New required parameters in {op} {path}: {new_required_params}")

    return len(errors) == 0, errors + warnings


def main():
    """Main validation function."""
    openapi_files = []
    for pattern in ['openapi/**/*.yaml', 'openapi/**/*.yml', '**/openapi.yaml', '**/openapi.yml']:
        openapi_files.extend(Path('.').glob(pattern))

    if not openapi_files:
        print("❌ No OpenAPI specification files found")
        return 1

    all_success = True

    for file_path in openapi_files:
        print(f"\n{'='*60}")
        print(f"🔬 Validating: {file_path}")
        print(f"{'='*60}")

        try:
            spec = load_openapi_spec(str(file_path))

            # Run all validations
            validations = [
                ("Spectral validation", run_spectral_validation, str(file_path)),
                ("Schema structure", validate_schema_structure, spec, str(file_path)),
                ("Examples validation", validate_examples, spec, str(file_path)),
                ("Security consistency", validate_security_consistency, spec, str(file_path)),
                ("Schemathesis validation", run_schemathesis_validation, str(file_path)),
            ]

            file_success = True

            for name, validator, *args in validations:
                try:
                    success, messages = validator(*args)
                    if not success:
                        file_success = False
                        all_success = False
                        for msg in messages:
                            print(msg)
                    else:
                        for msg in messages:
                            print(msg)
                except Exception as e:
                    print(f"❌ {name} failed: {str(e)}")
                    file_success = False
                    all_success = False

            # Generate artifacts
            artifact_success, artifact_messages = generate_openapi_artifacts(spec, str(file_path))
            if not artifact_success:
                all_success = False
            for msg in artifact_messages:
                print(msg)

            # Final status for file
            if file_success:
                print(f"✅ {file_path}: All validations passed")
            else:
                print(f"❌ {file_path}: Some validations failed")

        except Exception as e:
            print(f"❌ Failed to validate {file_path}: {str(e)}")
            all_success = False

    print(f"\n{'='*60}")
    if all_success:
        print("🎉 All OpenAPI validations passed!")
        print("🚀 Ready to generate Redoc documentation and publish SDKs")
        return 0
    else:
        print("💥 Some OpenAPI validations failed!")
        print("🔧 Please fix the issues before proceeding")
        return 1


def load_openapi_spec(file_path: str) -> dict:
    """Load OpenAPI specification from YAML or JSON file."""
    with open(file_path) as f:
        if file_path.endswith('.json'):
            return json.load(f)
        else:
            return yaml.safe_load(f)


if __name__ == "__main__":
    sys.exit(main())
