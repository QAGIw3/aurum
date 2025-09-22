#!/usr/bin/env python3
"""Generate and compare OpenAPI specifications for API changes."""

import json
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, Optional

import requests
from deepdiff import DeepDiff


def load_openapi_spec(file_path: Path) -> Dict[str, Any]:
    """Load OpenAPI specification from file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error loading OpenAPI spec from {file_path}: {e}")
        return {}


def generate_openapi_spec(output_path: Path, base_url: str = "http://localhost:8000"):
    """Generate OpenAPI spec from running application."""
    try:
        response = requests.get(f"{base_url}/openapi.json", timeout=10)
        response.raise_for_status()

        spec = response.json()

        with open(output_path, 'w') as f:
            json.dump(spec, f, indent=2)

        print(f"✅ Generated OpenAPI spec: {output_path}")
        return spec

    except Exception as e:
        print(f"❌ Error generating OpenAPI spec: {e}")
        return {}


def compare_openapi_specs(old_spec: Dict[str, Any], new_spec: Dict[str, Any]) -> Dict[str, Any]:
    """Compare two OpenAPI specifications and return differences."""
    try:
        # Perform deep comparison
        diff = DeepDiff(
            old_spec,
            new_spec,
            ignore_order=True,
            exclude_paths=[
                "root['info']['version']",  # Ignore version changes
                "root['servers']",  # Ignore server configuration
            ]
        )

        return diff.to_dict()

    except Exception as e:
        print(f"❌ Error comparing OpenAPI specs: {e}")
        return {}


def analyze_breaking_changes(diff: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze differences for breaking changes."""
    breaking_changes = {
        "removed_endpoints": [],
        "changed_parameters": [],
        "changed_response_schemas": [],
        "changed_request_schemas": [],
        "summary": ""
    }

    # Check for removed paths
    if "dictionary_item_removed" in diff:
        for removed_item in diff["dictionary_item_removed"]:
            if removed_item.startswith("root['paths']"):
                breaking_changes["removed_endpoints"].append(removed_item)

    # Check for changed parameters
    if "values_changed" in diff:
        for changed_item in diff["values_changed"]:
            if "parameters" in changed_item:
                breaking_changes["changed_parameters"].append(changed_item)

    # Check for changed response schemas
    if "type_changes" in diff:
        for changed_item in diff["type_changes"]:
            if "responses" in changed_item or "schema" in changed_item:
                breaking_changes["changed_response_schemas"].append(changed_item)

    # Generate summary
    total_changes = len(breaking_changes["removed_endpoints"]) + \
                   len(breaking_changes["changed_parameters"]) + \
                   len(breaking_changes["changed_response_schemas"])

    if total_changes == 0:
        breaking_changes["summary"] = "✅ No breaking changes detected"
    else:
        breaking_changes["summary"] = f"⚠️ Found {total_changes} potential breaking changes"

    return breaking_changes


def format_breaking_changes(breaking_changes: Dict[str, Any]) -> str:
    """Format breaking changes for display."""
    output = []
    output.append("# 🔍 OpenAPI Breaking Changes Analysis")
    output.append("")
    output.append(breaking_changes["summary"])
    output.append("")

    if breaking_changes["removed_endpoints"]:
        output.append("## 🗑️ Removed Endpoints")
        for endpoint in breaking_changes["removed_endpoints"]:
            output.append(f"- {endpoint}")
        output.append("")

    if breaking_changes["changed_parameters"]:
        output.append("## 🔧 Changed Parameters")
        for param in breaking_changes["changed_parameters"]:
            output.append(f"- {param}")
        output.append("")

    if breaking_changes["changed_response_schemas"]:
        output.append("## 📊 Changed Response Schemas")
        for schema in breaking_changes["changed_response_schemas"]:
            output.append(f"- {schema}")
        output.append("")

    return "\n".join(output)


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Generate and compare OpenAPI specifications")
    parser.add_argument("--base-spec", type=Path, help="Path to base OpenAPI spec")
    parser.add_argument("--new-spec", type=Path, help="Path to new OpenAPI spec")
    parser.add_argument("--output", type=Path, default=Path("openapi-diff.md"), help="Output file for diff")
    parser.add_argument("--generate-from", type=str, help="Generate spec from running API at this URL")

    args = parser.parse_args()

    if args.generate_from:
        print(f"🔧 Generating OpenAPI spec from {args.generate_from}")
        new_spec = generate_openapi_spec(Path("openapi-spec-new.json"), args.generate_from)
    else:
        print("🔧 Loading new OpenAPI spec")
        new_spec = load_openapi_spec(args.new_spec) if args.new_spec else {}

    if not new_spec:
        print("❌ No new spec available for comparison")
        sys.exit(1)

    # Load base spec
    if args.base_spec and args.base_spec.exists():
        print(f"🔧 Loading base OpenAPI spec from {args.base_spec}")
        old_spec = load_openapi_spec(args.base_spec)
    else:
        print("⚠️ No base spec provided - generating baseline comparison")
        old_spec = {}

    # Compare specs
    print("🔍 Comparing OpenAPI specifications...")
    diff = compare_openapi_specs(old_spec, new_spec)

    # Analyze breaking changes
    breaking_changes = analyze_breaking_changes(diff)

    # Format and save results
    formatted_diff = format_breaking_changes(breaking_changes)

    with open(args.output, 'w') as f:
        f.write(formatted_diff)

    print(f"✅ Analysis complete. Results saved to {args.output}")

    # Exit with error code if breaking changes found
    total_breaking_changes = len(breaking_changes["removed_endpoints"]) + \
                            len(breaking_changes["changed_parameters"]) + \
                            len(breaking_changes["changed_response_schemas"])

    if total_breaking_changes > 0:
        print(f"⚠️ Found {total_breaking_changes} breaking changes")
        print("Review the diff file for details")
        # Don't exit with error in CI - just warn
        # sys.exit(1)
    else:
        print("✅ No breaking changes detected")


if __name__ == "__main__":
    main()
