#!/usr/bin/env python3
"""Backfill driver for historical data ingestion operations."""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
import uuid

from aurum.cost_profiler import CostProfiler, ProfilerConfig
from aurum.logging import create_logger, LogLevel


@dataclass
class BackfillConfig:
    """Configuration for backfill operations."""

    source: str
    start_date: date
    end_date: date
    concurrency: int = 3
    batch_size: int = 1000
    max_retries: int = 3
    rate_limit: Optional[int] = None
    dry_run: bool = False
    priority: str = "normal"  # normal, high, low
    job_id: str = field(default_factory=lambda: f"BACKFILL_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


@dataclass
class BackfillJob:
    """Represents a single backfill job."""

    job_id: str
    source: str
    date: date
    batch_size: int
    status: str = "pending"  # pending, running, completed, failed, cancelled
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    errors: List[str] = field(default_factory=list)
    retry_count: int = 0


class BackfillDriver:
    """Driver for executing backfill operations."""

    def __init__(self, config: BackfillConfig):
        """Initialize backfill driver.

        Args:
            config: Backfill configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="backfill_driver",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.backfill.operations",
            dataset="backfill_operations"
        )

        # Initialize cost profiler
        profiler_config = ProfilerConfig(
            collection_interval_seconds=30,
            enable_memory_monitoring=True
        )
        self.profiler = CostProfiler(profiler_config)

        # Job tracking
        self.jobs: Dict[str, BackfillJob] = {}
        self.active_jobs: Dict[str, asyncio.Task] = {}

        # Results tracking
        self.total_jobs = 0
        self.completed_jobs = 0
        self.failed_jobs = 0
        self.cancelled_jobs = 0

    async def execute(self) -> Dict[str, Any]:
        """Execute the backfill operation.

        Returns:
            Operation results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting backfill operation {self.config.job_id}",
            "backfill_started",
            job_id=self.config.job_id,
            source=self.config.source,
            start_date=str(self.config.start_date),
            end_date=str(self.config.end_date),
            concurrency=self.config.concurrency
        )

        if self.config.dry_run:
            return await self._dry_run()

        # Generate job list
        jobs = self._generate_jobs()

        # Start profiler session
        profiler_session = self.profiler.start_profiling(
            dataset_name=f"backfill_{self.config.source}",
            data_source=self.config.source,
            session_id=self.config.job_id
        )

        try:
            # Execute jobs
            await self._execute_jobs(jobs)

            # Generate results
            results = self._generate_results()

            # End profiler session
            self.profiler.end_profiling(profiler_session)

            self.logger.log(
                LogLevel.INFO,
                f"Backfill operation {self.config.job_id} completed",
                "backfill_completed",
                job_id=self.config.job_id,
                total_jobs=self.total_jobs,
                completed_jobs=self.completed_jobs,
                failed_jobs=self.failed_jobs
            )

            return results

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Backfill operation {self.config.job_id} failed: {e}",
                "backfill_failed",
                job_id=self.config.job_id,
                error=str(e)
            )

            # Still end profiler session
            self.profiler.end_profiling(profiler_session)
            raise

    async def _dry_run(self) -> Dict[str, Any]:
        """Perform a dry run to validate configuration.

        Returns:
            Dry run results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Performing dry run for backfill {self.config.job_id}",
            "backfill_dry_run",
            job_id=self.config.job_id
        )

        # Generate jobs (without executing)
        jobs = self._generate_jobs()

        # Validate configuration
        validation_results = await self._validate_configuration(jobs)

        dry_run_results = {
            "job_id": self.config.job_id,
            "source": self.config.source,
            "start_date": str(self.config.start_date),
            "end_date": str(self.config.end_date),
            "total_jobs": len(jobs),
            "estimated_records": sum(job.batch_size * 365 for job in jobs),  # Rough estimate
            "estimated_duration_hours": len(jobs) / self.config.concurrency * 0.5,  # Rough estimate
            "validation_results": validation_results,
            "is_valid": all(v["valid"] for v in validation_results.values()),
            "recommendations": self._generate_dry_run_recommendations(validation_results)
        }

        self.logger.log(
            LogLevel.INFO,
            f"Dry run completed for {self.config.job_id}",
            "backfill_dry_run_completed",
            job_id=self.config.job_id,
            is_valid=dry_run_results["is_valid"],
            total_jobs=dry_run_results["total_jobs"]
        )

        return dry_run_results

    def _generate_jobs(self) -> List[BackfillJob]:
        """Generate individual backfill jobs for each date.

        Returns:
            List of backfill jobs
        """
        jobs = []

        current_date = self.config.start_date
        while current_date <= self.config.end_date:
            job = BackfillJob(
                job_id=f"{self.config.job_id}_{current_date.strftime('%Y%m%d')}",
                source=self.config.source,
                date=current_date,
                batch_size=self.config.batch_size
            )

            jobs.append(job)
            self.jobs[job.job_id] = job
            current_date += timedelta(days=1)

        self.total_jobs = len(jobs)
        return jobs

    async def _execute_jobs(self, jobs: List[BackfillJob]) -> None:
        """Execute backfill jobs with controlled concurrency.

        Args:
            jobs: List of jobs to execute
        """
        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.concurrency)

        async def execute_job(job: BackfillJob):
            async with semaphore:
                await self._execute_single_job(job)

        # Create tasks for all jobs
        tasks = [execute_job(job) for job in jobs]

        # Execute with progress reporting
        completed = 0
        for coro in asyncio.as_completed(tasks):
            try:
                await coro
                completed += 1

                if completed % max(1, len(jobs) // 20) == 0:  # Progress every 5%
                    progress = (completed / len(jobs)) * 100
                    self.logger.log(
                        LogLevel.INFO,
                        f"Backfill progress: {completed}/{len(jobs)} jobs ({progress".1f"}%)",
                        "backfill_progress",
                        job_id=self.config.job_id,
                        completed=completed,
                        total=len(jobs),
                        progress_percent=progress
                    )

            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Job failed: {e}",
                    "backfill_job_failed",
                    job_id=self.config.job_id,
                    error=str(e)
                )

    async def _execute_single_job(self, job: BackfillJob) -> None:
        """Execute a single backfill job.

        Args:
            job: Job to execute
        """
        job.status = "running"
        job.start_time = datetime.now()

        try:
            # Simulate SeaTunnel job execution
            await self._run_seatunnel_job(job)

            job.status = "completed"
            job.end_time = datetime.now()
            self.completed_jobs += 1

            self.logger.log(
                LogLevel.INFO,
                f"Job {job.job_id} completed successfully",
                "backfill_job_completed",
                job_id=job.job_id,
                records_processed=job.records_processed
            )

        except Exception as e:
            job.status = "failed"
            job.errors.append(str(e))
            job.end_time = datetime.now()
            self.failed_jobs += 1

            self.logger.log(
                LogLevel.ERROR,
                f"Job {job.job_id} failed: {e}",
                "backfill_job_failed",
                job_id=job.job_id,
                error=str(e)
            )

    async def _run_seatunnel_job(self, job: BackfillJob) -> None:
        """Run SeaTunnel job for a specific date.

        Args:
            job: Job to run
        """
        # This would integrate with actual SeaTunnel execution
        # For now, simulate the execution

        import time

        # Simulate job execution time (30 seconds to 5 minutes)
        execution_time = 30 + (hash(job.job_id) % 270)  # 30s to 5min

        # Simulate some processing
        await asyncio.sleep(min(execution_time, 60))  # Cap at 1 minute for demo

        # Simulate records processed
        job.records_processed = job.batch_size + (hash(job.job_id) % 1000)

        # Simulate occasional failures (5% failure rate)
        if hash(job.job_id) % 20 == 0:
            raise Exception(f"Simulated failure for job {job.job_id}")

    async def _validate_configuration(self, jobs: List[BackfillJob]) -> Dict[str, Any]:
        """Validate configuration before execution.

        Args:
            jobs: List of jobs to validate

        Returns:
            Validation results
        """
        validation = {
            "quota_compliance": True,
            "resource_availability": True,
            "data_source_availability": True,
            "configuration_validity": True
        }

        # Check quotas
        if self.config.rate_limit:
            estimated_api_calls = sum(job.batch_size for job in jobs)
            if estimated_api_calls > self.config.rate_limit * len(jobs) * 86400:  # Daily limit
                validation["quota_compliance"] = False

        # Check concurrency
        if self.config.concurrency > 10:  # Arbitrary limit
            validation["resource_availability"] = False

        return validation

    def _generate_dry_run_recommendations(self, validation: Dict[str, Any]) -> List[str]:
        """Generate recommendations from dry run.

        Args:
            validation: Validation results

        Returns:
            List of recommendations
        """
        recommendations = []

        if not validation["quota_compliance"]:
            recommendations.append("Reduce concurrency or batch size to comply with API quotas")

        if not validation["resource_availability"]:
            recommendations.append("Reduce concurrency to avoid resource exhaustion")

        if self.config.concurrency > 5:
            recommendations.append("Consider starting with lower concurrency and scaling up")

        if len(self._generate_jobs()) > 100:
            recommendations.append("Large backfill detected - consider breaking into smaller chunks")

        return recommendations

    def _generate_results(self) -> Dict[str, Any]:
        """Generate final operation results.

        Returns:
            Operation results
        """
        duration = None
        if self.jobs:
            start_times = [job.start_time for job in self.jobs.values() if job.start_time]
            end_times = [job.end_time for job in self.jobs.values() if job.end_time]

            if start_times and end_times:
                duration = (max(end_times) - min(start_times)).total_seconds()

        total_records = sum(job.records_processed for job in self.jobs.values())

        results = {
            "job_id": self.config.job_id,
            "source": self.config.source,
            "start_date": str(self.config.start_date),
            "end_date": str(self.config.end_date),
            "total_jobs": self.total_jobs,
            "completed_jobs": self.completed_jobs,
            "failed_jobs": self.failed_jobs,
            "cancelled_jobs": self.cancelled_jobs,
            "total_records_processed": total_records,
            "duration_seconds": duration,
            "success_rate": self.completed_jobs / max(self.total_jobs, 1),
            "average_records_per_job": total_records / max(self.completed_jobs, 1),
            "jobs": {job_id: self._job_to_dict(job) for job_id, job in self.jobs.items()}
        }

        return results

    def _job_to_dict(self, job: BackfillJob) -> Dict[str, Any]:
        """Convert job to dictionary.

        Args:
            job: Job to convert

        Returns:
            Job dictionary
        """
        return {
            "job_id": job.job_id,
            "source": job.source,
            "date": str(job.date),
            "batch_size": job.batch_size,
            "status": job.status,
            "start_time": job.start_time.isoformat() if job.start_time else None,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "records_processed": job.records_processed,
            "errors": job.errors,
            "retry_count": job.retry_count
        }

    async def pause(self) -> None:
        """Pause the backfill operation."""
        self.logger.log(
            LogLevel.INFO,
            f"Pausing backfill operation {self.config.job_id}",
            "backfill_paused",
            job_id=self.config.job_id
        )

        # Cancel all active tasks
        for task in self.active_jobs.values():
            task.cancel()

        # Wait for cancellation to complete
        if self.active_jobs:
            await asyncio.gather(*self.active_jobs.values(), return_exceptions=True)

        self.logger.log(
            LogLevel.INFO,
            f"Backfill operation {self.config.job_id} paused",
            "backfill_pause_completed",
            job_id=self.config.job_id
        )

    async def resume(self) -> None:
        """Resume a paused backfill operation."""
        self.logger.log(
            LogLevel.INFO,
            f"Resuming backfill operation {self.config.job_id}",
            "backfill_resumed",
            job_id=self.config.job_id
        )

        # Resume from where we left off
        pending_jobs = [job for job in self.jobs.values() if job.status == "pending"]
        await self._execute_jobs(pending_jobs)

    async def cancel(self, force: bool = False) -> None:
        """Cancel the backfill operation.

        Args:
            force: Force cancellation without cleanup
        """
        self.logger.log(
            LogLevel.WARNING,
            f"Cancelling backfill operation {self.config.job_id}",
            "backfill_cancelled",
            job_id=self.config.job_id,
            force=force
        )

        # Cancel all active tasks
        for task in self.active_jobs.values():
            task.cancel()

        if not force:
            # Mark remaining jobs as cancelled
            for job in self.jobs.values():
                if job.status in ["pending", "running"]:
                    job.status = "cancelled"
                    self.cancelled_jobs += 1

        self.logger.log(
            LogLevel.INFO,
            f"Backfill operation {self.config.job_id} cancelled",
            "backfill_cancel_completed",
            job_id=self.config.job_id
        )


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Execute backfill operations for historical data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic backfill
  python backfill_driver.py --source eia --start-date 2024-01-01 --end-date 2024-12-31

  # Dry run first
  python backfill_driver.py --source eia --start-date 2024-01-01 --end-date 2024-12-31 --dry-run

  # High concurrency backfill
  python backfill_driver.py --source fred --start-date 2024-06-01 --end-date 2024-12-31 --concurrency 8

  # Conservative backfill
  python backfill_driver.py --source noaa --start-date 2024-01-01 --end-date 2024-03-31 --concurrency 1 --batch-size 500
        """
    )

    parser.add_argument(
        "--source",
        required=True,
        choices=["eia", "fred", "cpi", "noaa", "iso"],
        help="Data source for backfill"
    )

    parser.add_argument(
        "--start-date",
        required=True,
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        help="Start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end-date",
        required=True,
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        help="End date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Maximum concurrent jobs"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Records per batch"
    )

    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retry attempts"
    )

    parser.add_argument(
        "--rate-limit",
        type=int,
        help="API calls per second limit"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing"
    )

    parser.add_argument(
        "--priority",
        choices=["low", "normal", "high"],
        default="normal",
        help="Operation priority"
    )

    parser.add_argument(
        "--job-id",
        help="Custom job ID"
    )

    parser.add_argument(
        "--output",
        help="Output file for results"
    )

    parser.add_argument(
        "--progress-interval",
        type=int,
        default=300,
        help="Progress report interval in seconds"
    )

    args = parser.parse_args()

    # Validate date range
    if args.start_date > args.end_date:
        print("Error: Start date must be before end date")
        return 1

    if (args.end_date - args.start_date).days > 365:
        print("Warning: Large date range detected (>365 days)")
        print("Consider breaking into smaller chunks")

    # Create configuration
    config = BackfillConfig(
        source=args.source,
        start_date=args.start_date,
        end_date=args.end_date,
        concurrency=args.concurrency,
        batch_size=args.batch_size,
        max_retries=args.max_retries,
        rate_limit=args.rate_limit,
        dry_run=args.dry_run,
        priority=args.priority,
        job_id=args.job_id or f"BACKFILL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    # Create and run driver
    driver = BackfillDriver(config)

    async def run():
        try:
            results = await driver.execute()

            # Output results
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(results, f, indent=2, default=str)
                print(f"Results saved to: {args.output}")
            else:
                print(json.dumps(results, indent=2, default=str))

            # Exit with appropriate code
            if results.get("failed_jobs", 0) > 0:
                return 1
            else:
                return 0

        except KeyboardInterrupt:
            print("\nOperation interrupted by user")
            await driver.pause()
            return 130
        except Exception as e:
            print(f"Operation failed: {e}")
            return 1

    # Run async function
    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
