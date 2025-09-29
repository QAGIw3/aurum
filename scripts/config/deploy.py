#!/usr/bin/env python3
"""
Configuration deployment script for promoting configuration across environments.

Usage:
    python scripts/config/deploy.py promote --from=development --to=staging --actor=admin
    python scripts/config/deploy.py deploy --env=production --version=123 --actor=admin --reason="Production deployment"
    python scripts/config/deploy.py rollback --env=production --to-version=122 --actor=admin --reason="Emergency rollback"
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
from aurum.config.validation import validate_and_coerce_config


async def promote_config(from_env: str, to_env: str, actor: str, reason: str = "") -> None:
    """Promote configuration from one environment to another."""
    try:
        # Get configuration from source environment
        from_service = DynamicConfigService(environment=from_env)
        from_config = from_service.get()

        # Validate the configuration
        try:
            validated_config = validate_and_coerce_config(from_config, strict_mode=True)
        except Exception as e:
            print(f"❌ Configuration validation failed: {e}")
            sys.exit(1)

        # Get configuration from target environment for comparison
        to_service = DynamicConfigService(environment=to_env)
        to_config = to_service.get()

        # Compare configurations
        tracker = get_change_tracker()
        diff = tracker._calculate_diff(to_config, from_config)

        print(f"Promoting configuration from {from_env} to {to_env}")
        print("=" * 60)

        if diff:
            print("Configuration changes:")
            print(json.dumps(diff, indent=2))
        else:
            print("No configuration changes detected")

        # Confirm promotion
        confirm = input(f"\nProceed with promotion to {to_env}? (yes/no): ")
        if confirm.lower() not in ("yes", "y"):
            print("Promotion cancelled")
            return

        # Record the change
        change_id = await tracker.record_change(
            change_type=ChangeType.UPDATED,
            source=ChangeSource.CI_CD,
            actor=actor,
            namespace=None,
            reason=f"Promote: {reason or f'Configuration promotion from {from_env} to {to_env}'}",
            old_config=to_config,
            new_config=from_config,
            metadata={"from_env": from_env, "to_env": to_env}
        )

        # Create version for the promoted configuration
        await tracker.create_version(from_config, change_id, {
            "deployment_type": "promotion",
            "from_env": from_env,
            "to_env": to_env,
            "actor": actor
        })

        print("✅ Configuration promotion completed successfully")
        print(f"Change ID: {change_id}")

    except Exception as e:
        print(f"❌ Configuration promotion failed: {e}")
        sys.exit(1)


async def deploy_config(environment: str, version: int, actor: str, reason: str = "") -> None:
    """Deploy a specific configuration version to an environment."""
    try:
        tracker = get_change_tracker()

        # Get the target version
        target_version = tracker.get_version(version)
        if not target_version:
            print(f"❌ Version {version} not found")
            sys.exit(1)

        # Get current configuration for comparison
        service = DynamicConfigService(environment=environment)
        current_config = service.get()

        print(f"Deploying version {version} to {environment}")
        print("=" * 60)
        print(f"Target version timestamp: {target_version.timestamp}")
        print(f"Content hash: {target_version.content_hash[:16]}...")

        # Validate the target configuration
        try:
            validated_config = validate_and_coerce_config(target_version.config, strict_mode=True)
        except Exception as e:
            print(f"❌ Target configuration validation failed: {e}")
            sys.exit(1)

        # Show diff
        diff = tracker._calculate_diff(current_config, target_version.config)
        if diff:
            print("Configuration changes:")
            print(json.dumps(diff, indent=2))

        # Confirm deployment
        confirm = input(f"\nDeploy version {version} to {environment}? (yes/no): ")
        if confirm.lower() not in ("yes", "y"):
            print("Deployment cancelled")
            return

        # Record the change
        change_id = await tracker.record_change(
            change_type=ChangeType.UPDATED,
            source=ChangeSource.CI_CD,
            actor=actor,
            namespace=None,
            reason=f"Deploy: {reason or f'Deployment of version {version} to {environment}'}",
            old_config=current_config,
            new_config=target_version.config,
            metadata={"target_version": version, "environment": environment}
        )

        # Create version for the deployed configuration
        await tracker.create_version(target_version.config, change_id, {
            "deployment_type": "deployment",
            "target_version": version,
            "environment": environment,
            "actor": actor
        })

        print("✅ Configuration deployment completed successfully")
        print(f"Change ID: {change_id}")

    except Exception as e:
        print(f"❌ Configuration deployment failed: {e}")
        sys.exit(1)


async def rollback_config(environment: str, to_version: int, actor: str, reason: str = "") -> None:
    """Rollback configuration to a previous version."""
    try:
        tracker = get_change_tracker()

        # Get the target version
        target_version = tracker.get_version(to_version)
        if not target_version:
            print(f"❌ Version {to_version} not found")
            sys.exit(1)

        # Get current configuration
        service = DynamicConfigService(environment=environment)
        current_config = service.get()

        print(f"Rolling back {environment} to version {to_version}")
        print("=" * 60)
        print(f"Current version: {tracker.get_latest_version().version}")
        print(f"Target version: {to_version}")
        print(f"Target timestamp: {target_version.timestamp}")

        # Validate the target configuration
        try:
            validated_config = validate_and_coerce_config(target_version.config, strict_mode=True)
        except Exception as e:
            print(f"❌ Target configuration validation failed: {e}")
            sys.exit(1)

        # Show diff
        diff = tracker._calculate_diff(current_config, target_version.config)
        if diff:
            print("Configuration changes:")
            print(json.dumps(diff, indent=2))

        # Confirm rollback
        confirm = input(f"\nRollback {environment} to version {to_version}? (yes/no): ")
        if confirm.lower() not in ("yes", "y"):
            print("Rollback cancelled")
            return

        # Record the change
        change_id = await tracker.record_change(
            change_type=ChangeType.UPDATED,
            source=ChangeSource.CI_CD,
            actor=actor,
            namespace=None,
            reason=f"Rollback: {reason or f'Rollback to version {to_version} in {environment}'}",
            old_config=current_config,
            new_config=target_version.config,
            metadata={"rollback_to_version": to_version, "environment": environment}
        )

        # Create version for the rolled back configuration
        await tracker.create_version(target_version.config, change_id, {
            "deployment_type": "rollback",
            "rollback_to_version": to_version,
            "environment": environment,
            "actor": actor
        })

        print("✅ Configuration rollback completed successfully")
        print(f"Change ID: {change_id}")

    except Exception as e:
        print(f"❌ Configuration rollback failed: {e}")
        sys.exit(1)


async def main():
    parser = argparse.ArgumentParser(description="Configuration deployment and promotion CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # promote command
    promote_parser = subparsers.add_parser("promote", help="Promote configuration between environments")
    promote_parser.add_argument("--from", required=True, help="Source environment")
    promote_parser.add_argument("--to", required=True, help="Target environment")
    promote_parser.add_argument("--actor", required=True, help="Actor performing the promotion")
    promote_parser.add_argument("--reason", default="", help="Reason for promotion")

    # deploy command
    deploy_parser = subparsers.add_parser("deploy", help="Deploy a specific version to an environment")
    deploy_parser.add_argument("--env", required=True, help="Target environment")
    deploy_parser.add_argument("--version", type=int, required=True, help="Version to deploy")
    deploy_parser.add_argument("--actor", required=True, help="Actor performing the deployment")
    deploy_parser.add_argument("--reason", default="", help="Reason for deployment")

    # rollback command
    rollback_parser = subparsers.add_parser("rollback", help="Rollback to a previous version")
    rollback_parser.add_argument("--env", required=True, help="Environment to rollback")
    rollback_parser.add_argument("--to-version", type=int, required=True, help="Version to rollback to")
    rollback_parser.add_argument("--actor", required=True, help="Actor performing the rollback")
    rollback_parser.add_argument("--reason", default="", help="Reason for rollback")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "promote":
        await promote_config(args.from, args.to, args.actor, args.reason)
    elif args.command == "deploy":
        await deploy_config(args.env, args.version, args.actor, args.reason)
    elif args.command == "rollback":
        await rollback_config(args.env, args.to_version, args.actor, args.reason)


if __name__ == "__main__":
    asyncio.run(main())
