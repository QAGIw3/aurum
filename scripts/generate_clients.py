#!/usr/bin/env python3
"""Generate typed clients from OpenAPI specification."""

import sys
from pathlib import Path

from aurum.api.openapi_generator import OpenAPIGenerator


def main():
    """Generate Python and TypeScript clients."""
    # Find the OpenAPI spec
    openapi_file = Path("docs/api/openapi-spec.yaml")
    if not openapi_file.exists():
        print(f"OpenAPI specification not found at {openapi_file}")
        return 1

    # Load the OpenAPI spec
    with open(openapi_file, 'r') as f:
        import yaml
        spec = yaml.safe_load(f)

    if not spec:
        print("Failed to load OpenAPI specification")
        return 1

    # Create generator
    generator = OpenAPIGenerator(spec)

    # Output directory
    output_dir = Path("clients")
    output_dir.mkdir(exist_ok=True)

    print("Generating Python client...")
    try:
        generator.generate_python_sdk(str(output_dir))
        print("✓ Python client generated")
    except Exception as e:
        print(f"✗ Failed to generate Python client: {e}")
        return 1

    print("Generating TypeScript client...")
    try:
        generator.generate_typescript_sdk(str(output_dir))
        print("✓ TypeScript client generated")
    except Exception as e:
        print(f"✗ Failed to generate TypeScript client: {e}")
        return 1

    # Generate additional language clients
    print("Generating additional language clients...")
    try:
        generator.generate_go_sdk(str(output_dir))
        print("✓ Go client generated")
    except Exception as e:
        print(f"✗ Failed to generate Go client: {e}")

    try:
        generator.generate_rust_sdk(str(output_dir))
        print("✓ Rust client generated")
    except Exception as e:
        print(f"✗ Failed to generate Rust client: {e}")

    print(f"\nClient SDKs generated successfully in {output_dir}/")
    print("\nAvailable clients:")
    print("  - Python: clients/python/client.py")
    print("  - TypeScript: clients/typescript/client.ts")
    print("  - Go: clients/go/client.go")
    print("  - Rust: clients/rust/src/lib.rs")

    return 0


if __name__ == "__main__":
    sys.exit(main())
