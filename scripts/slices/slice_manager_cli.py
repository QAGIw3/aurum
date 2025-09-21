#!/usr/bin/env python3
"""CLI for managing slice-based incremental resume operations."""

import argparse
import asyncio
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

from aurum.slices import SliceClient, SliceConfig, SliceManager, SliceType, SliceStatus
from aurum.logging import create_logger, LogLevel


class SliceManagerCLI:
    """CLI for slice management operations."""

    def __init__(self):
        """Initialize CLI."""
        self.logger = create_logger(
            source_name="slice_manager_cli",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.slices.cli",
            dataset="slice_cli"
        )

    async def run_list_slices(
        self,
        source_name: Optional[str] = None,
        slice_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 20,
        output_file: Optional[str] = None
    ) -> int:
        """List slices based on criteria.

        Args:
            source_name: Optional source name filter
            slice_type: Optional slice type filter
            status: Optional status filter
            limit: Maximum number of slices to return
            output_file: Optional output file path

        Returns:
            Exit code
        """
        try:
            # Create slice client
            config = SliceConfig(database_url="postgresql://aurum_user:change_me_in_production@localhost:5432/aurum")
            async with SliceClient(config) as client:

                # Build query
                query = SliceQuery(
                    source_name=source_name,
                    slice_type=SliceType(slice_type) if slice_type else None,
                    status=SliceStatus(status) if status else None,
                    limit=limit,
                    include_metadata=True
                )

                # Get slices
                slices = await client.get_slices(query)

                # Prepare output
                output_data = {
                    "query": query.to_dict(),
                    "total_slices": len(slices),
                    "slices": [slice_info.to_dict() for slice_info in slices]
                }

                # Output results
                if output_file:
                    with open(output_file, 'w') as f:
                        json.dump(output_data, f, indent=2, default=str)
                    print(f"Results saved to: {output_file}")
                else:
                    print(json.dumps(output_data, indent=2, default=str))

                return 0

        except Exception as e:
            print(f"Error listing slices: {e}")
            return 1

    async def run_create_slice(
        self,
        source_name: str,
        slice_key: str,
        slice_type: str,
        slice_data: Dict[str, Any],
        priority: int = 100,
        max_retries: int = 3,
        records_expected: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """Create a new slice.

        Args:
            source_name: Name of the data source
            slice_key: Unique key for the slice
            slice_type: Type of slice
            slice_data: Slice data
            priority: Slice priority
            max_retries: Maximum retry attempts
            records_expected: Expected number of records
            metadata: Additional metadata

        Returns:
            Exit code
        """
        try:
            # Create slice client
            config = SliceConfig(database_url="postgresql://aurum_user:change_me_in_production@localhost:5432/aurum")
            async with SliceClient(config) as client:

                # Create slice
                slice_id = await client.register_slice(
                    source_name=source_name,
                    slice_key=slice_key,
                    slice_type=SliceType(slice_type),
                    slice_data=slice_data,
                    priority=priority,
                    max_retries=max_retries,
                    records_expected=records_expected,
                    metadata=metadata
                )

                print(f"Created slice: {slice_id}")

                # Get and display slice info
                slice_info = await client.get_slice(slice_id)
                if slice_info:
                    print(json.dumps(slice_info.to_dict(), indent=2, default=str))

                return 0

        except Exception as e:
            print(f"Error creating slice: {e}")
            return 1

    async def run_start_worker(
        self,
        worker_id: str,
        source_name: Optional[str] = None,
        slice_types: Optional[list] = None,
        max_slices: int = 50
    ) -> int:
        """Start a slice processing worker.

        Args:
            worker_id: Worker identifier
            source_name: Optional source name filter
            slice_types: Optional slice types to process
            max_slices: Maximum slices to process

        Returns:
            Exit code
        """
        try:
            # Create slice client and manager
            config = SliceConfig(database_url="postgresql://aurum_user:change_me_in_production@localhost:5432/aurum")
            async with SliceClient(config) as client:
                manager = SliceManager(client)

                # Parse slice types
                slice_type_objects = None
                if slice_types:
                    slice_type_objects = [SliceType(st) for st in slice_types]

                # Start worker
                await manager.start_worker(
                    worker_id=worker_id,
                    source_name=source_name,
                    slice_types=slice_type_objects,
                    max_slices=max_slices
                )

                print(f"Started slice worker: {worker_id}")

                # Monitor worker for a short time
                try:
                    while True:
                        await asyncio.sleep(5)

                        # Check worker status
                        status = await manager.get_worker_status()

                        # Get slice statistics
                        stats = await client.get_slice_statistics(source_name)

                        print(f"Worker {worker_id} status: {status['active_workers']}/{status['total_workers']} active")
                        print(f"Slice stats: {stats.get('completed_slices', 0)} completed, {stats.get('failed_slices', 0)} failed")

                except KeyboardInterrupt:
                    print(f"\nStopping worker {worker_id}")
                    await manager.stop_worker(worker_id)
                    return 0

        except Exception as e:
            print(f"Error starting worker: {e}")
            return 1

    async def run_retry_slices(
        self,
        source_name: Optional[str] = None,
        max_slices: int = 10
    ) -> int:
        """Retry failed slices.

        Args:
            source_name: Optional source name filter
            max_slices: Maximum slices to retry

        Returns:
            Exit code
        """
        try:
            # Create slice client and manager
            config = SliceConfig(database_url="postgresql://aurum_user:change_me_in_production@localhost:5432/aurum")
            async with SliceClient(config) as client:
                manager = SliceManager(client)

                # Retry failed slices
                retried_ids = await manager.retry_failed_slices(source_name, max_slices)

                print(f"Retried {len(retried_ids)} slices:")
                for slice_id in retried_ids:
                    print(f"  - {slice_id}")

                return 0

        except Exception as e:
            print(f"Error retrying slices: {e}")
            return 1

    async def run_get_stats(
        self,
        source_name: Optional[str] = None,
        hours_back: int = 24,
        output_file: Optional[str] = None
    ) -> int:
        """Get slice operation statistics.

        Args:
            source_name: Optional source name filter
            hours_back: Time range in hours
            output_file: Optional output file path

        Returns:
            Exit code
        """
        try:
            # Create slice client
            config = SliceConfig(database_url="postgresql://aurum_user:change_me_in_production@localhost:5432/aurum")
            async with SliceClient(config) as client:

                # Get statistics
                stats = await client.get_slice_statistics(source_name, hours_back)

                # Prepare output
                output_data = {
                    "generated_at": datetime.now().isoformat(),
                    "time_range_hours": hours_back,
                    "source_name": source_name,
                    "statistics": stats
                }

                # Output results
                if output_file:
                    with open(output_file, 'w') as f:
                        json.dump(output_data, f, indent=2, default=str)
                    print(f"Statistics saved to: {output_file}")
                else:
                    print(json.dumps(output_data, indent=2, default=str))

                return 0

        except Exception as e:
            print(f"Error getting statistics: {e}")
            return 1


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Slice-based incremental resume operations CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all slices for EIA source
  python slice_manager_cli.py list --source eia --limit 10

  # List failed slices
  python slice_manager_cli.py list --status failed --limit 5

  # Create a time window slice
  python slice_manager_cli.py create eia "2024-12-31_14:00" time_window '{"start_time": "2024-12-31T14:00:00", "end_time": "2024-12-31T15:00:00"}' --priority 50

  # Start a worker for EIA time window slices
  python slice_manager_cli.py worker eia_worker --source eia --slice-types time_window --max-slices 20

  # Retry failed slices for EIA
  python slice_manager_cli.py retry --source eia --max-slices 5

  # Get operation statistics
  python slice_manager_cli.py stats --source eia --hours-back 48 --output stats.json

  # List slices by type
  python slice_manager_cli.py list --slice-type time_window --status pending --limit 10
        """
    )

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # List command
    list_parser = subparsers.add_parser('list', help='List slices')
    list_parser.add_argument('--source', dest='source_name', help='Source name filter')
    list_parser.add_argument('--slice-type', help='Slice type filter')
    list_parser.add_argument('--status', help='Status filter')
    list_parser.add_argument('--limit', type=int, default=20, help='Maximum slices to return')
    list_parser.add_argument('--output', help='Output file path')

    # Create command
    create_parser = subparsers.add_parser('create', help='Create a slice')
    create_parser.add_argument('source_name', help='Source name')
    create_parser.add_argument('slice_key', help='Slice key')
    create_parser.add_argument('slice_type', help='Slice type')
    create_parser.add_argument('slice_data', help='Slice data as JSON string')
    create_parser.add_argument('--priority', type=int, default=100, help='Slice priority')
    create_parser.add_argument('--max-retries', type=int, default=3, help='Maximum retry attempts')
    create_parser.add_argument('--records-expected', type=int, help='Expected number of records')
    create_parser.add_argument('--metadata', help='Metadata as JSON string')

    # Worker command
    worker_parser = subparsers.add_parser('worker', help='Start a slice processing worker')
    worker_parser.add_argument('worker_id', help='Worker identifier')
    worker_parser.add_argument('--source', dest='source_name', help='Source name filter')
    worker_parser.add_argument('--slice-types', nargs='+', help='Slice types to process')
    worker_parser.add_argument('--max-slices', type=int, default=50, help='Maximum slices to process')

    # Retry command
    retry_parser = subparsers.add_parser('retry', help='Retry failed slices')
    retry_parser.add_argument('--source', dest='source_name', help='Source name filter')
    retry_parser.add_argument('--max-slices', type=int, default=10, help='Maximum slices to retry')

    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Get operation statistics')
    stats_parser.add_argument('--source', dest='source_name', help='Source name filter')
    stats_parser.add_argument('--hours-back', type=int, default=24, help='Time range in hours')
    stats_parser.add_argument('--output', help='Output file path')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    # Initialize CLI
    cli = SliceManagerCLI()

    async def run_async():
        try:
            if args.command == 'list':
                return await cli.run_list_slices(
                    args.source_name,
                    args.slice_type,
                    args.status,
                    args.limit,
                    args.output
                )
            elif args.command == 'create':
                # Parse JSON arguments
                slice_data = json.loads(args.slice_data)
                metadata = json.loads(args.metadata) if args.metadata else {}

                return await cli.run_create_slice(
                    args.source_name,
                    args.slice_key,
                    args.slice_type,
                    slice_data,
                    args.priority,
                    args.max_retries,
                    args.records_expected,
                    metadata
                )
            elif args.command == 'worker':
                return await cli.run_start_worker(
                    args.worker_id,
                    args.source_name,
                    args.slice_types,
                    args.max_slices
                )
            elif args.command == 'retry':
                return await cli.run_retry_slices(args.source_name, args.max_slices)
            elif args.command == 'stats':
                return await cli.run_get_stats(args.source_name, args.hours_back, args.output)
            else:
                parser.print_help()
                return 1

        except KeyboardInterrupt:
            print("\nOperation interrupted by user")
            return 130
        except Exception as e:
            print(f"Operation failed: {e}")
            return 1

    return asyncio.run(run_async())


if __name__ == "__main__":
    sys.exit(main())
