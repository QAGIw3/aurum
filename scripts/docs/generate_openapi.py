#!/usr/bin/env python3
"""Generate OpenAPI documentation and validate for drift."""

import sys
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from aurum.api.openapi_generator import generate_documentation, DocumentationFormat, SDKLanguage
from aurum.api.app import create_app
from aurum.core import AurumSettings


async def main(output_dir: str = "docs/api", check_drift: bool = False) -> None:
    """Generate API documentation and optionally check for drift."""
    print("🚀 Generating OpenAPI documentation...")

    # Create the FastAPI app
    settings = AurumSettings.from_env()
    app = await create_app(settings)

    # Generate documentation
    formats = [
        DocumentationFormat.JSON,
        DocumentationFormat.YAML,
        DocumentationFormat.MARKDOWN
    ]

    languages = [
        SDKLanguage.PYTHON,
        SDKLanguage.TYPESCRIPT
    ]

    generate_documentation(
        app=app,
        output_dir=output_dir,
        formats=formats,
        languages=languages
    )

    if check_drift:
        print("🔍 Checking for OpenAPI spec drift...")
        # Check if generated files differ from existing ones
        import subprocess
        result = subprocess.run([
            "git", "diff", "--quiet",
            "docs/api/openapi.json",
            "docs/api/openapi.yaml"
        ], capture_output=True)

        if result.returncode != 0:
            print("❌ OpenAPI spec drift detected!")
            print("Run: git diff docs/api/openapi.json docs/api/openapi.yaml")
            sys.exit(1)
        else:
            print("✅ No OpenAPI spec drift detected")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate OpenAPI documentation")
    parser.add_argument("--output-dir", default="docs/api", help="Output directory")
    parser.add_argument("--check-drift", action="store_true", help="Check for spec drift")

    args = parser.parse_args()
    import asyncio
    asyncio.run(main(args.output_dir, args.check_drift))