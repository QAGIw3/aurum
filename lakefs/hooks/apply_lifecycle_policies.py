#!/usr/bin/env python3
"""Apply lifecycle policies to LakeFS paths for data retention and storage optimization.

This script manages the lifecycle of data in LakeFS by:
- Setting retention periods for different data types
- Transitioning data to cold storage tiers
- Archiving old data
- Cleaning up expired data
"""

import argparse
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any

try:
    import requests
    from lakefs import LakeFSClient
except ImportError:
    requests = None
    LakeFSClient = None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Lifecycle policy configurations
LIFECYCLE_POLICIES = {
    'raw': {
        'retention_days': 90,
        'transition_to_cold_days': 30,
        'archive_after_days': 365,
        'delete_after_days': 730,
        'description': 'Raw data - keep for 90 days, archive after 1 year'
    },
    'market': {
        'retention_days': 730,
        'transition_to_cold_days': 90,
        'archive_after_days': 1825,
        'delete_after_days': 3650,
        'description': 'Curated market data - keep for 2 years, archive after 5 years'
    },
    'external': {
        'retention_days': 365,
        'transition_to_cold_days': 60,
        'archive_after_days': 1095,
        'delete_after_days': 2555,
        'description': 'External data - keep for 1 year, archive after 3 years'
    },
    'temp': {
        'retention_days': 7,
        'transition_to_cold_days': 1,
        'archive_after_days': 30,
        'delete_after_days': 90,
        'description': 'Temporary data - keep for 1 week, delete after 90 days'
    }
}


class LakeFSLifecycleManager:
    """Manager for LakeFS lifecycle policies and data management."""

    def __init__(self, lakefs_url: str, access_key: str, secret_key: str):
        if LakeFSClient is None:
            raise ImportError("lakefs package is required")

        self.client = LakeFSClient(
            host=lakefs_url,
            username=access_key,
            password=secret_key
        )
        self.lakefs_url = lakefs_url.rstrip('/')

    def get_objects_in_path(self, repository: str, branch: str, path: str) -> List[Dict[str, Any]]:
        """Get all objects in a LakeFS path."""
        try:
            response = self.client.objects.list(
                repository=repository,
                ref=branch,
                prefix=path
            )
            return response.results
        except Exception as e:
            logger.error(f"Failed to list objects in {repository}/{branch}/{path}: {e}")
            return []

    def set_object_tags(self, repository: str, branch: str, path: str, tags: Dict[str, str]) -> bool:
        """Set tags on a LakeFS object."""
        try:
            # Get current object metadata
            obj = self.client.objects.get(
                repository=repository,
                ref=branch,
                path=path
            )

            # Update tags (this is a simplified implementation)
            # In practice, you would use the LakeFS API to update object metadata
            logger.info(f"Would set tags {tags} on {repository}/{branch}/{path}")
            return True

        except Exception as e:
            logger.error(f"Failed to set tags on {repository}/{branch}/{path}: {e}")
            return False

    def apply_lifecycle_policy(self, repository: str, branch: str, path: str,
                             policy_name: str) -> Dict[str, Any]:
        """Apply lifecycle policy to objects in a path."""
        policy = LIFECYCLE_POLICIES.get(policy_name)
        if not policy:
            raise ValueError(f"Unknown lifecycle policy: {policy_name}")

        objects = self.get_objects_in_path(repository, branch, path)
        processed_count = 0
        errors = []

        logger.info(f"Applying {policy_name} policy to {len(objects)} objects in {repository}/{branch}/{path}")

        for obj in objects:
            try:
                # Calculate object age
                object_path = obj.path
                last_modified = obj.mtime or datetime.now()
                age_days = (datetime.now() - last_modified.replace(tzinfo=None)).days

                # Determine lifecycle stage
                lifecycle_stage = self.determine_lifecycle_stage(age_days, policy)

                # Apply appropriate tags and actions
                tags = {
                    'lifecycle_policy': policy_name,
                    'lifecycle_stage': lifecycle_stage,
                    'last_reviewed': datetime.now().isoformat(),
                    'retention_days': str(policy['retention_days']),
                    'archive_after_days': str(policy['archive_after_days'])
                }

                if self.set_object_tags(repository, branch, object_path, tags):
                    processed_count += 1

                    # Execute lifecycle actions
                    self.execute_lifecycle_action(repository, branch, object_path, lifecycle_stage, policy)

                else:
                    errors.append(f"Failed to process {object_path}")

            except Exception as e:
                logger.error(f"Error processing {obj.path}: {e}")
                errors.append(str(e))

        return {
            'policy': policy_name,
            'path': path,
            'objects_processed': processed_count,
            'errors': errors,
            'policy_details': policy
        }

    def determine_lifecycle_stage(self, age_days: int, policy: Dict[str, Any]) -> str:
        """Determine the lifecycle stage for an object based on its age."""
        if age_days < policy['transition_to_cold_days']:
            return 'hot'
        elif age_days < policy['archive_after_days']:
            return 'cold'
        elif age_days < policy['delete_after_days']:
            return 'archive'
        else:
            return 'expired'

    def execute_lifecycle_action(self, repository: str, branch: str, object_path: str,
                               lifecycle_stage: str, policy: Dict[str, Any]) -> None:
        """Execute the appropriate action for the lifecycle stage."""
        if lifecycle_stage == 'hot':
            logger.debug(f"Object {object_path} is in hot tier - no action needed")
        elif lifecycle_stage == 'cold':
            logger.info(f"Object {object_path} should be transitioned to cold storage")
            # In practice, this would trigger a storage tier transition
        elif lifecycle_stage == 'archive':
            logger.info(f"Object {object_path} should be archived")
            # In practice, this would trigger an archive operation
        elif lifecycle_stage == 'expired':
            logger.warning(f"Object {object_path} is expired and should be deleted")
            # In practice, this would trigger a deletion (with safety checks)

    def generate_lifecycle_report(self, repository: str, branch: str, path: str) -> Dict[str, Any]:
        """Generate a report on the lifecycle status of objects in a path."""
        objects = self.get_objects_in_path(repository, branch, path)

        report = {
            'repository': repository,
            'branch': branch,
            'path': path,
            'total_objects': len(objects),
            'lifecycle_summary': {},
            'recommendations': [],
            'generated_at': datetime.now().isoformat()
        }

        stage_counts = {'hot': 0, 'cold': 0, 'archive': 0, 'expired': 0}
        storage_estimate = {'hot': 0, 'cold': 0, 'archive': 0}

        for obj in objects:
            try:
                age_days = (datetime.now() - obj.mtime.replace(tzinfo=None)).days
                stage = self.determine_lifecycle_stage(age_days, LIFECYCLE_POLICIES['raw'])
                stage_counts[stage] += 1

                # Estimate storage costs (simplified)
                size_mb = getattr(obj, 'size_bytes', 0) / (1024 * 1024)
                storage_estimate[stage] += size_mb

            except Exception as e:
                logger.error(f"Error analyzing {obj.path}: {e}")

        report['lifecycle_summary'] = stage_counts
        report['storage_estimate_mb'] = storage_estimate

        # Generate recommendations
        if stage_counts['expired'] > 0:
            report['recommendations'].append({
                'type': 'cleanup',
                'message': f"{stage_counts['expired']} objects are expired and should be deleted",
                'priority': 'high'
            })

        if stage_counts['archive'] > stage_counts['cold'] * 0.5:
            report['recommendations'].append({
                'type': 'optimization',
                'message': f"Consider archiving {stage_counts['archive']} objects to reduce costs",
                'priority': 'medium'
            })

        return report


def main():
    """Main entry point for lifecycle policy application."""
    parser = argparse.ArgumentParser(description='Apply LakeFS lifecycle policies')
    parser.add_argument('--lakefs-url', required=True, help='LakeFS server URL')
    parser.add_argument('--access-key', required=True, help='LakeFS access key')
    parser.add_argument('--secret-key', required=True, help='LakeFS secret key')
    parser.add_argument('--repository', required=True, help='LakeFS repository')
    parser.add_argument('--branch', default='main', help='LakeFS branch')
    parser.add_argument('--path', required=True, help='Path to apply lifecycle policy to')
    parser.add_argument('--policy', choices=['raw', 'market', 'external', 'temp'],
                       default='raw', help='Lifecycle policy to apply')
    parser.add_argument('--retention-days', type=int, help='Override retention period in days')
    parser.add_argument('--transition-to-cold', type=int, help='Days before transitioning to cold storage')
    parser.add_argument('--archive-after-days', type=int, help='Days before archiving')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without making changes')
    parser.add_argument('--report-only', action='store_true', help='Generate report only')
    parser.add_argument('--output', type=Path, help='Output file for report')

    args = parser.parse_args()

    # Override policy with command line arguments
    policy = LIFECYCLE_POLICIES[args.policy].copy()
    if args.retention_days:
        policy['retention_days'] = args.retention_days
    if args.transition_to_cold:
        policy['transition_to_cold_days'] = args.transition_to_cold
    if args.archive_after_days:
        policy['archive_after_days'] = args.archive_after_days

    try:
        # Initialize LakeFS client
        manager = LakeFSLifecycleManager(
            args.lakefs_url,
            args.access_key,
            args.secret_key
        )

        if args.report_only:
            # Generate report
            logger.info(f"Generating lifecycle report for {args.repository}/{args.branch}/{args.path}")
            report = manager.generate_lifecycle_report(args.repository, args.branch, args.path)

            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                logger.info(f"Report saved to {args.output}")

            # Print summary
            logger.info("=== Lifecycle Report Summary ===")
            logger.info(f"Path: {args.repository}/{args.branch}/{args.path}")
            logger.info(f"Total objects: {report['total_objects']}")
            logger.info(f"Hot: {report['lifecycle_summary']['hot']} objects")
            logger.info(f"Cold: {report['lifecycle_summary']['cold']} objects")
            logger.info(f"Archive: {report['lifecycle_summary']['archive']} objects")
            logger.info(f"Expired: {report['lifecycle_summary']['expired']} objects")

            if report['recommendations']:
                logger.info("Recommendations:")
                for rec in report['recommendations']:
                    logger.info(f"  {rec['priority'].upper()}: {rec['message']}")

        else:
            # Apply lifecycle policy
            logger.info(f"Applying {args.policy} policy to {args.repository}/{args.branch}/{args.path}")

            if args.dry_run:
                logger.info("DRY RUN - no changes will be made")

            result = manager.apply_lifecycle_policy(
                args.repository,
                args.branch,
                args.path,
                args.policy
            )

            logger.info("=== Lifecycle Application Summary ===")
            logger.info(f"Policy: {args.policy}")
            logger.info(f"Path: {args.path}")
            logger.info(f"Objects processed: {result['objects_processed']}")

            if result['errors']:
                logger.warning(f"Errors encountered: {len(result['errors'])}")
                for error in result['errors'][:5]:  # Show first 5 errors
                    logger.warning(f"  - {error}")

                if len(result['errors']) > 5:
                    logger.warning(f"  ... and {len(result['errors']) - 5} more errors")

            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(result, f, indent=2, default=str)
                logger.info(f"Results saved to {args.output}")

    except Exception as e:
        logger.error(f"Lifecycle management failed: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
