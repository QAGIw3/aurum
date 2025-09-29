#!/usr/bin/env python3
"""
Configuration backup and restore tool.

Usage:
    python scripts/config/backup_restore.py backup --reason="REASON" [--output=PATH]
    python scripts/config/backup_restore.py restore --version=VERSION --actor="ACTOR" --reason="REASON"
    python scripts/config/backup_restore.py list-backups [--limit=COUNT]
"""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from aurum.config.change_tracking import get_change_tracker, ChangeType, ChangeSource
from aurum.config.dynamic_config import DynamicConfigService


async def backup_config(reason: str, output_path: Optional[str] = None) -> None:
    """Backup the current configuration."""
    try:
        # Get current configuration
        service = DynamicConfigService()
        config = service.get()

        # Record backup change
        tracker = get_change_tracker()
        change_id = await tracker.backup_current_config(config, reason)

        print(f"Configuration backed up successfully")
        print(f"Change ID: {change_id}")
        print(f"Version: {tracker.get_latest_version().version}")
        print(f"Reason: {reason}")

        if output_path:
            # Export to file
            with open(output_path, 'w') as f:
                json.dump(config, f, indent=2, default=str)
            print(f"Configuration exported to: {output_path}")

    except Exception as e:
        print(f"Error backing up configuration: {e}")
        sys.exit(1)


async def restore_config(version: int, actor: str, reason: str) -> None:
    """Restore configuration to a specific version."""
    try:
        tracker = get_change_tracker()

        # Get the target version
        target_version = tracker.get_version(version)
        if not target_version:
            print(f"Version {version} not found")
            sys.exit(1)

        print(f"Restoring configuration to version {version}")
        print(f"Original timestamp: {target_version.timestamp}")
        print(f"Change ID: {target_version.change_id}")
        print(f"Content hash: {target_version.content_hash[:16]}...")

        # Confirm restoration
        confirm = input("This will overwrite the current configuration. Continue? (yes/no): ")
        if confirm.lower() not in ("yes", "y"):
            print("Restoration cancelled")
            return

        # Perform restoration
        change_id = await tracker.restore_version(version, actor, reason)

        print("Configuration restored successfully"        print(f"Change ID: {change_id}")
        print(f"New version: {tracker.get_latest_version().version}")

    except Exception as e:
        print(f"Error restoring configuration: {e}")
        sys.exit(1)


def list_backups(limit: int) -> None:
    """List recent configuration backups."""
    try:
        tracker = get_change_tracker()
        versions = tracker.list_versions(limit=limit)

        print(f"Recent configuration backups (latest {len(versions)}):")
        print("=" * 80)
        for version in versions:
            print(f"Version {version.version}:")
            print(f"  Timestamp: {version.timestamp}")
            print(f"  Content Hash: {version.content_hash[:16]}...")
            print(f"  Change ID: {version.change_id}")
            print(f"  Compressed Size: {version.compressed_size} bytes")
            if version.metadata.get("backup_reason"):
                print(f"  Reason: {version.metadata['backup_reason']}")
            print()
    except Exception as e:
        print(f"Error listing backups: {e}")
        sys.exit(1)


async def main():
    parser = argparse.ArgumentParser(description="Configuration backup and restore CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # backup command
    backup_parser = subparsers.add_parser("backup", help="Backup current configuration")
    backup_parser.add_argument("--reason", required=True, help="Reason for backup")
    backup_parser.add_argument("--output", help="Export backup to JSON file")

    # restore command
    restore_parser = subparsers.add_parser("restore", help="Restore configuration to version")
    restore_parser.add_argument("--version", type=int, required=True, help="Version to restore")
    restore_parser.add_argument("--actor", required=True, help="Actor performing restore")
    restore_parser.add_argument("--reason", default="", help="Reason for restore")

    # list-backups command
    list_parser = subparsers.add_parser("list-backups", help="List configuration backups")
    list_parser.add_argument("--limit", type=int, default=10, help="Number of backups to show")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "backup":
        await backup_config(args.reason, args.output)
    elif args.command == "restore":
        await restore_config(args.version, args.actor, args.reason)
    elif args.command == "list-backups":
        list_backups(args.limit)


if __name__ == "__main__":
    asyncio.run(main())
