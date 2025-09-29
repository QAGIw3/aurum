#!/usr/bin/env python3
"""
Configuration diffing and comparison tool.

Usage:
    python scripts/config/diff.py show-effective [--env=ENVIRONMENT]
    python scripts/config/diff.py diff --from=VERSION --to=VERSION
    python scripts/config/diff.py versions [--limit=COUNT]
    python scripts/config/diff.py changes [--namespace=NAMESPACE] [--actor=ACTOR] [--limit=COUNT]
    python scripts/config/diff.py export-schema --output=PATH
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from aurum.config.change_tracking import get_change_tracker
from aurum.config.dynamic_config import DynamicConfigService
from aurum.config.validation import export_all_schemas


def show_effective_config(environment: str) -> None:
    """Show the effective configuration for an environment."""
    try:
        service = DynamicConfigService(environment=environment)
        config = service.get()

        print(f"Effective configuration for environment '{environment}':")
        print("=" * 60)
        print(json.dumps(config, indent=2, default=str))
    except Exception as e:
        print(f"Error getting effective config: {e}")
        sys.exit(1)


def show_config_diff(from_version: int, to_version: int) -> None:
    """Show diff between two configuration versions."""
    try:
        tracker = get_change_tracker()
        diff = tracker.compare_versions(from_version, to_version)

        print(f"Configuration diff from version {from_version} to {to_version}:")
        print("=" * 60)
        print(json.dumps(diff, indent=2, default=str))
    except Exception as e:
        print(f"Error getting config diff: {e}")
        sys.exit(1)


def show_config_versions(limit: int) -> None:
    """Show recent configuration versions."""
    try:
        tracker = get_change_tracker()
        versions = tracker.list_versions(limit=limit)

        print(f"Recent configuration versions (latest {len(versions)}):")
        print("=" * 60)
        for version in versions:
            print(f"Version {version.version}:")
            print(f"  Timestamp: {version.timestamp}")
            print(f"  Content Hash: {version.content_hash[:16]}...")
            print(f"  Change ID: {version.change_id}")
            print(f"  Compressed Size: {version.compressed_size} bytes")
            if version.metadata:
                print(f"  Metadata: {version.metadata}")
            print()
    except Exception as e:
        print(f"Error getting config versions: {e}")
        sys.exit(1)


def show_config_changes(namespace: Optional[str], actor: Optional[str], limit: int) -> None:
    """Show configuration change history."""
    try:
        tracker = get_change_tracker()
        changes = tracker.get_change_history(limit=limit, namespace=namespace, actor=actor)

        print(f"Configuration changes (latest {len(changes)}):")
        print("=" * 60)
        for change in changes:
            print(f"Change {change.change_id}:")
            print(f"  Type: {change.change_type.value}")
            print(f"  Source: {change.source.value}")
            print(f"  Actor: {change.actor}")
            print(f"  Timestamp: {change.timestamp}")
            if change.namespace:
                print(f"  Namespace: {change.namespace}")
            if change.reason:
                print(f"  Reason: {change.reason}")
            if change.correlation_id:
                print(f"  Correlation ID: {change.correlation_id}")
            print()
    except Exception as e:
        print(f"Error getting config changes: {e}")
        sys.exit(1)


def export_schemas(output_dir: str) -> None:
    """Export all configuration schemas to JSON Schema files."""
    try:
        export_all_schemas(output_dir)
        print(f"Exported schemas to {output_dir}")
    except Exception as e:
        print(f"Error exporting schemas: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Configuration management CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # show-effective command
    show_parser = subparsers.add_parser("show-effective", help="Show effective configuration")
    show_parser.add_argument("--env", default="development", help="Environment name")

    # diff command
    diff_parser = subparsers.add_parser("diff", help="Show diff between versions")
    diff_parser.add_argument("--from", type=int, required=True, help="From version")
    diff_parser.add_argument("--to", type=int, required=True, help="To version")

    # versions command
    versions_parser = subparsers.add_parser("versions", help="Show configuration versions")
    versions_parser.add_argument("--limit", type=int, default=10, help="Number of versions to show")

    # changes command
    changes_parser = subparsers.add_parser("changes", help="Show configuration changes")
    changes_parser.add_argument("--namespace", help="Filter by namespace")
    changes_parser.add_argument("--actor", help="Filter by actor")
    changes_parser.add_argument("--limit", type=int, default=20, help="Number of changes to show")

    # export-schema command
    export_parser = subparsers.add_parser("export-schema", help="Export configuration schemas")
    export_parser.add_argument("--output", default="docs/schemas", help="Output directory")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "show-effective":
        show_effective_config(args.env)
    elif args.command == "diff":
        show_config_diff(args.from, args.to)
    elif args.command == "versions":
        show_config_versions(args.limit)
    elif args.command == "changes":
        show_config_changes(args.namespace, args.actor, args.limit)
    elif args.command == "export-schema":
        export_schemas(args.output)


if __name__ == "__main__":
    main()
