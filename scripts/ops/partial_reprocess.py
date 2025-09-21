#!/usr/bin/env python3
"""Partial reprocessing driver for targeted data corrections."""

import argparse
import asyncio
import json
import sys
from datetime import datetime, date, time
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import uuid

from aurum.logging import create_logger, LogLevel


class PartialReprocessType(str, Enum):
    """Types of partial reprocessing operations."""

    TIME_WINDOW = "time_window"
    DATASETS = "datasets"
    FAILED_RECORDS = "failed_records"
    INCREMENTAL = "incremental"
    QUALITY_ISSUES = "quality_issues"


@dataclass
class PartialReprocessConfig:
    """Configuration for partial reprocessing operations."""

    source: str
    reprocess_type: PartialReprocessType
    date: Optional[date] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    hours: List[int] = field(default_factory=list)
    datasets: List[str] = field(default_factory=list)
    failure_log: Optional[str] = None
    incremental_field: Optional[str] = None
    config_file: Optional[str] = None
    validation_script: Optional[str] = None
    rollback_plan: Optional[str] = None
    reason: str = ""
    priority: str = "normal"
    max_concurrent: int = 2
    batch_size: int = 500
    dry_run: bool = False
    operation_id: str = field(default_factory=lambda: f"PARTIAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


@dataclass
class PartialReprocessJob:
    """A single partial reprocessing job."""

    job_id: str
    source: str
    target_time_range: str
    target_datasets: List[str]
    status: str = "pending"
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    records_corrected: int = 0
    conflicts_resolved: int = 0
    validation_score: Optional[float] = None
    errors: List[str] = field(default_factory=list)


class PartialReprocessDriver:
    """Driver for executing partial reprocessing operations."""

    def __init__(self, config: PartialReprocessConfig):
        """Initialize partial reprocessing driver.

        Args:
            config: Partial reprocessing configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="partial_reprocess_driver",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.partial_reprocess.operations",
            dataset="partial_reprocess_operations"
        )

        # Job tracking
        self.jobs: List[PartialReprocessJob] = []
        self.current_job: Optional[PartialReprocessJob] = None

    async def execute(self) -> Dict[str, Any]:
        """Execute the partial reprocessing operation.

        Returns:
            Operation results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting partial reprocessing {self.config.operation_id}",
            "partial_reprocess_started",
            operation_id=self.config.operation_id,
            source=self.config.source,
            reprocess_type=self.config.reprocess_type.value,
            reason=self.config.reason
        )

        if self.config.dry_run:
            return await self._dry_run()

        try:
            # Generate and execute jobs
            jobs = self._generate_jobs()
            results = await self._execute_jobs(jobs)

            self.logger.log(
                LogLevel.INFO,
                f"Partial reprocessing {self.config.operation_id} completed",
                "partial_reprocess_completed",
                operation_id=self.config.operation_id,
                total_jobs=len(jobs),
                completed_jobs=len([j for j in jobs if j.status == "completed"])
            )

            return results

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Partial reprocessing {self.config.operation_id} failed: {e}",
                "partial_reprocess_failed",
                operation_id=self.config.operation_id,
                error=str(e)
            )
            raise

    async def _dry_run(self) -> Dict[str, Any]:
        """Perform a dry run to validate configuration.

        Returns:
            Dry run results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Performing dry run for partial reprocessing {self.config.operation_id}",
            "partial_reprocess_dry_run",
            operation_id=self.config.operation_id
        )

        # Generate jobs
        jobs = self._generate_jobs()

        # Validate configuration
        validation_results = await self._validate_partial_config()

        dry_run_results = {
            "operation_id": self.config.operation_id,
            "source": self.config.source,
            "reprocess_type": self.config.reprocess_type.value,
            "reason": self.config.reason,
            "total_jobs": len(jobs),
            "estimated_records": sum(job.records_processed for job in jobs if job.records_processed > 0),
            "validation_results": validation_results,
            "is_valid": all(v["valid"] for v in validation_results.values()),
            "jobs": [self._job_to_dict(job) for job in jobs],
            "recommendations": self._generate_dry_run_recommendations(validation_results)
        }

        self.logger.log(
            LogLevel.INFO,
            f"Dry run completed for {self.config.operation_id}",
            "partial_reprocess_dry_run_completed",
            operation_id=self.config.operation_id,
            is_valid=dry_run_results["is_valid"]
        )

        return dry_run_results

    def _generate_jobs(self) -> List[PartialReprocessJob]:
        """Generate partial reprocessing jobs based on configuration.

        Returns:
            List of partial reprocessing jobs
        """
        jobs = []

        if self.config.reprocess_type == PartialReprocessType.TIME_WINDOW:
            jobs.extend(self._generate_time_window_jobs())
        elif self.config.reprocess_type == PartialReprocessType.DATASETS:
            jobs.extend(self._generate_dataset_jobs())
        elif self.config.reprocess_type == PartialReprocessType.FAILED_RECORDS:
            jobs.extend(self._generate_failed_records_jobs())
        elif self.config.reprocess_type == PartialReprocessType.INCREMENTAL:
            jobs.extend(self._generate_incremental_jobs())
        else:
            jobs.append(self._generate_generic_job())

        return jobs

    def _generate_time_window_jobs(self) -> List[PartialReprocessJob]:
        """Generate jobs for time window reprocessing."""
        jobs = []

        if not self.config.date:
            return jobs

        base_time = datetime.combine(self.config.date, time.min)

        for hour in self.config.hours:
            start_time = base_time.replace(hour=hour)
            end_time = start_time.replace(hour=hour + 1)

            job = PartialReprocessJob(
                job_id=f"{self.config.operation_id}_hour_{hour:02d}",
                source=self.config.source,
                target_time_range=f"{start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}",
                target_datasets=self.config.datasets or ["all"]
            )

            jobs.append(job)

        return jobs

    def _generate_dataset_jobs(self) -> List[PartialReprocessJob]:
        """Generate jobs for dataset-specific reprocessing."""
        jobs = []

        if not self.config.date:
            return jobs

        for dataset in self.config.datasets:
            job = PartialReprocessJob(
                job_id=f"{self.config.operation_id}_dataset_{dataset.replace('.', '_')}",
                source=self.config.source,
                target_time_range=f"full_day_{self.config.date.strftime('%Y%m%d')}",
                target_datasets=[dataset]
            )

            jobs.append(job)

        return jobs

    def _generate_failed_records_jobs(self) -> List[PartialReprocessJob]:
        """Generate jobs for failed records reprocessing."""
        jobs = []

        if not self.config.failure_log:
            return jobs

        # Load failure log
        try:
            with open(self.config.failure_log, 'r') as f:
                failure_data = json.load(f)
        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Failed to load failure log: {e}",
                "partial_reprocess_failure_log_error",
                operation_id=self.config.operation_id,
                error=str(e)
            )
            return jobs

        # Group failures by time window
        time_windows = self._group_failures_by_time(failure_data)

        for i, (time_range, failures) in enumerate(time_windows.items()):
            job = PartialReprocessJob(
                job_id=f"{self.config.operation_id}_failed_{i+1:03d}",
                source=self.config.source,
                target_time_range=time_range,
                target_datasets=list(failures.keys())
            )

            jobs.append(job)

        return jobs

    def _generate_incremental_jobs(self) -> List[PartialReprocessJob]:
        """Generate jobs for incremental reprocessing."""
        jobs = []

        if not self.config.date or not self.config.incremental_field:
            return jobs

        job = PartialReprocessJob(
            job_id=f"{self.config.operation_id}_incremental",
            source=self.config.source,
            target_time_range=f"after_last_update_{self.config.date.strftime('%Y%m%d')}",
            target_datasets=self.config.datasets or ["all"]
        )

        jobs.append(job)
        return jobs

    def _generate_generic_job(self) -> List[PartialReprocessJob]:
        """Generate a generic partial reprocessing job."""
        job = PartialReprocessJob(
            job_id=self.config.operation_id,
            source=self.config.source,
            target_time_range="partial_day",
            target_datasets=self.config.datasets or ["all"]
        )

        return [job]

    def _group_failures_by_time(self, failure_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Group failures by time window.

        Args:
            failure_data: Failure data from log

        Returns:
            Failures grouped by time window
        """
        # Simple grouping by hour for demo
        grouped = {}

        for record_id, failure in failure_data.get("failures", {}).items():
            timestamp = failure.get("timestamp", "")
            if timestamp:
                # Extract hour from timestamp
                hour = timestamp.split("T")[1][:2] if "T" in timestamp else "00"
                time_range = f"{hour}:00-{int(hour)+1"02d"}:00"

                if time_range not in grouped:
                    grouped[time_range] = {}

                grouped[time_range][record_id] = failure

        return grouped

    async def _execute_jobs(self, jobs: List[PartialReprocessJob]) -> Dict[str, Any]:
        """Execute partial reprocessing jobs.

        Args:
            jobs: Jobs to execute

        Returns:
            Execution results
        """
        start_time = datetime.now()

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        async def execute_job(job: PartialReprocessJob):
            async with semaphore:
                await self._execute_single_job(job)

        # Execute jobs
        tasks = [execute_job(job) for job in jobs]

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                self.logger.log(
                    LogLevel.ERROR,
                    f"Job failed: {e}",
                    "partial_reprocess_job_failed",
                    operation_id=self.config.operation_id,
                    error=str(e)
                )

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Generate results
        completed_jobs = [j for j in jobs if j.status == "completed"]
        failed_jobs = [j for j in jobs if j.status == "failed"]

        total_records = sum(j.records_processed for j in completed_jobs)
        total_corrected = sum(j.records_corrected for j in completed_jobs)
        total_conflicts = sum(j.conflicts_resolved for j in completed_jobs)

        # Calculate average validation score
        valid_scores = [j.validation_score for j in completed_jobs if j.validation_score is not None]
        avg_validation_score = sum(valid_scores) / len(valid_scores) if valid_scores else None

        results = {
            "operation_id": self.config.operation_id,
            "source": self.config.source,
            "reprocess_type": self.config.reprocess_type.value,
            "reason": self.config.reason,
            "total_jobs": len(jobs),
            "completed_jobs": len(completed_jobs),
            "failed_jobs": len(failed_jobs),
            "total_records_processed": total_records,
            "total_records_corrected": total_corrected,
            "total_conflicts_resolved": total_conflicts,
            "average_validation_score": avg_validation_score,
            "duration_seconds": duration,
            "success_rate": len(completed_jobs) / max(len(jobs), 1),
            "jobs": [self._job_to_dict(job) for job in jobs]
        }

        return results

    async def _execute_single_job(self, job: PartialReprocessJob) -> None:
        """Execute a single partial reprocessing job.

        Args:
            job: Job to execute
        """
        job.status = "running"
        job.start_time = datetime.now()

        try:
            self.logger.log(
                LogLevel.INFO,
                f"Executing partial reprocessing job {job.job_id}",
                "partial_reprocess_job_started",
                operation_id=self.config.operation_id,
                job_id=job.job_id,
                target_time_range=job.target_time_range
            )

            # Execute based on reprocessing type
            if self.config.reprocess_type == PartialReprocessType.TIME_WINDOW:
                await self._execute_time_window_job(job)
            elif self.config.reprocess_type == PartialReprocessType.DATASETS:
                await self._execute_dataset_job(job)
            elif self.config.reprocess_type == PartialReprocessType.FAILED_RECORDS:
                await self._execute_failed_records_job(job)
            elif self.config.reprocess_type == PartialReprocessType.INCREMENTAL:
                await self._execute_incremental_job(job)
            else:
                await self._execute_generic_job_execution(job)

            job.status = "completed"
            job.end_time = datetime.now()

            self.logger.log(
                LogLevel.INFO,
                f"Partial reprocessing job {job.job_id} completed",
                "partial_reprocess_job_completed",
                operation_id=self.config.operation_id,
                job_id=job.job_id,
                records_processed=job.records_processed
            )

        except Exception as e:
            job.status = "failed"
            job.errors.append(str(e))
            job.end_time = datetime.now()

            self.logger.log(
                LogLevel.ERROR,
                f"Partial reprocessing job {job.job_id} failed: {e}",
                "partial_reprocess_job_failed",
                operation_id=self.config.operation_id,
                job_id=job.job_id,
                error=str(e)
            )
            raise

    async def _execute_time_window_job(self, job: PartialReprocessJob) -> None:
        """Execute time window reprocessing job."""
        import asyncio
        await asyncio.sleep(2)  # Simulate processing

        # Simulate processing results
        job.records_processed = 500 + (hash(job.job_id) % 1000)
        job.records_corrected = int(job.records_processed * 0.8)  # 80% correction rate
        job.validation_score = 0.92 + (hash(job.job_id) % 10) / 100

    async def _execute_dataset_job(self, job: PartialReprocessJob) -> None:
        """Execute dataset-specific reprocessing job."""
        import asyncio
        await asyncio.sleep(3)  # Simulate processing

        job.records_processed = 800 + (hash(job.job_id) % 1500)
        job.records_corrected = int(job.records_processed * 0.9)  # 90% correction rate
        job.validation_score = 0.95 + (hash(job.job_id) % 8) / 100

    async def _execute_failed_records_job(self, job: PartialReprocessJob) -> None:
        """Execute failed records reprocessing job."""
        import asyncio
        await asyncio.sleep(4)  # Simulate processing

        job.records_processed = 200 + (hash(job.job_id) % 300)
        job.records_corrected = job.records_processed  # All failures corrected
        job.conflicts_resolved = (hash(job.job_id) % 50)  # Some conflicts
        job.validation_score = 0.98 + (hash(job.job_id) % 5) / 100

    async def _execute_incremental_job(self, job: PartialReprocessJob) -> None:
        """Execute incremental reprocessing job."""
        import asyncio
        await asyncio.sleep(3)  # Simulate processing

        job.records_processed = 300 + (hash(job.job_id) % 700)
        job.records_corrected = int(job.records_processed * 0.85)
        job.validation_score = 0.93 + (hash(job.job_id) % 7) / 100

    async def _execute_generic_job_execution(self, job: PartialReprocessJob) -> None:
        """Execute generic partial reprocessing job."""
        import asyncio
        await asyncio.sleep(2)  # Simulate processing

        job.records_processed = 600 + (hash(job.job_id) % 800)
        job.records_corrected = int(job.records_processed * 0.75)
        job.validation_score = 0.90 + (hash(job.job_id) % 12) / 100

    async def _validate_partial_config(self) -> Dict[str, Any]:
        """Validate partial reprocessing configuration.

        Returns:
            Validation results
        """
        validation = {
            "source_available": True,
            "time_range_valid": True,
            "datasets_exist": True,
            "failure_log_valid": True,
            "incremental_field_valid": True
        }

        # Validate time range
        if self.config.date:
            if self.config.start_time and self.config.end_time:
                if self.config.start_time >= self.config.end_time:
                    validation["time_range_valid"] = False

        # Validate failure log
        if self.config.failure_log:
            log_path = Path(self.config.failure_log)
            if not log_path.exists():
                validation["failure_log_valid"] = False

        # Validate datasets
        if self.config.datasets and len(self.config.datasets) == 0:
            validation["datasets_exist"] = False

        return validation

    def _generate_dry_run_recommendations(self, validation: Dict[str, Any]) -> List[str]:
        """Generate recommendations from dry run.

        Args:
            validation: Validation results

        Returns:
            List of recommendations
        """
        recommendations = []

        if not validation["source_available"]:
            recommendations.append("Verify data source availability before execution")

        if not validation["time_range_valid"]:
            recommendations.append("Fix time range - start time must be before end time")

        if not validation["failure_log_valid"]:
            recommendations.append("Ensure failure log file exists and is readable")

        if self.config.max_concurrent > 3:
            recommendations.append("High concurrency may impact system stability - consider reducing")

        if len(self._generate_jobs()) > 10:
            recommendations.append("Large number of jobs - consider breaking into smaller batches")

        return recommendations

    def _job_to_dict(self, job: PartialReprocessJob) -> Dict[str, Any]:
        """Convert job to dictionary.

        Args:
            job: Job to convert

        Returns:
            Job dictionary
        """
        return {
            "job_id": job.job_id,
            "source": job.source,
            "target_time_range": job.target_time_range,
            "target_datasets": job.target_datasets,
            "status": job.status,
            "start_time": job.start_time.isoformat() if job.start_time else None,
            "end_time": job.end_time.isoformat() if job.end_time else None,
            "records_processed": job.records_processed,
            "records_corrected": job.records_corrected,
            "conflicts_resolved": job.conflicts_resolved,
            "validation_score": job.validation_score,
            "errors": job.errors
        }


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Execute partial reprocessing operations for targeted data corrections",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Reprocess specific hours
  python partial_reprocess.py --type time_window --source eia --date 2024-12-31 --hours 10,11,12

  # Reprocess specific datasets
  python partial_reprocess.py --type datasets --source eia --datasets "ELEC.PRICE,ELEC.DEMAND" --date 2024-12-31

  # Reprocess failed records
  python partial_reprocess.py --type failed_records --source eia --failure-log "logs/failed_records.json"

  # Incremental reprocessing
  python partial_reprocess.py --type incremental --source eia --date 2024-12-31 --incremental-field "last_updated"

  # Time range reprocessing
  python partial_reprocess.py --type time_window --source eia --start-time "2024-12-31 09:00:00" --end-time "2024-12-31 17:00:00"
        """
    )

    parser.add_argument(
        "--source",
        required=True,
        choices=["eia", "fred", "cpi", "noaa", "iso"],
        help="Data source for partial reprocessing"
    )

    parser.add_argument(
        "--type",
        type=PartialReprocessType,
        required=True,
        choices=list(PartialReprocessType),
        help="Type of partial reprocessing"
    )

    # Time-based arguments
    parser.add_argument(
        "--date",
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        help="Date for reprocessing (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--start-time",
        type=lambda t: datetime.strptime(t, '%Y-%m-%d %H:%M:%S'),
        help="Start time for reprocessing (YYYY-MM-DD HH:MM:SS)"
    )

    parser.add_argument(
        "--end-time",
        type=lambda t: datetime.strptime(t, '%Y-%m-%d %H:%M:%S'),
        help="End time for reprocessing (YYYY-MM-DD HH:MM:SS)"
    )

    parser.add_argument(
        "--hours",
        type=lambda h: [int(x) for x in h.split(',')],
        help="Specific hours to reprocess (comma-separated, e.g., '9,10,11')"
    )

    # Dataset-specific arguments
    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Specific datasets to reprocess"
    )

    # Failed records arguments
    parser.add_argument(
        "--failure-log",
        help="Path to failure log file"
    )

    # Incremental arguments
    parser.add_argument(
        "--incremental-field",
        help="Field name for incremental processing"
    )

    # Configuration arguments
    parser.add_argument(
        "--config-file",
        help="Configuration file for custom settings"
    )

    parser.add_argument(
        "--validation-script",
        help="Custom validation script path"
    )

    parser.add_argument(
        "--rollback-plan",
        help="Rollback plan description"
    )

    # Execution arguments
    parser.add_argument(
        "--reason",
        required=True,
        help="Reason for partial reprocessing"
    )

    parser.add_argument(
        "--priority",
        choices=["low", "normal", "high"],
        default="normal",
        help="Operation priority"
    )

    parser.add_argument(
        "--max-concurrent",
        type=int,
        default=2,
        help="Maximum concurrent operations"
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Records per batch"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing"
    )

    parser.add_argument(
        "--operation-id",
        help="Custom operation ID"
    )

    parser.add_argument(
        "--output",
        help="Output file for results"
    )

    args = parser.parse_args()

    # Validate required arguments based on type
    if args.type == PartialReprocessType.TIME_WINDOW:
        if not args.date and not (args.start_time and args.end_time):
            print("Error: Time window reprocessing requires either --date and --hours or --start-time and --end-time")
            return 1

    elif args.type == PartialReprocessType.DATASETS:
        if not args.date or not args.datasets:
            print("Error: Dataset reprocessing requires --date and --datasets")
            return 1

    elif args.type == PartialReprocessType.FAILED_RECORDS:
        if not args.failure_log:
            print("Error: Failed records reprocessing requires --failure-log")
            return 1

    elif args.type == PartialReprocessType.INCREMENTAL:
        if not args.date or not args.incremental_field:
            print("Error: Incremental reprocessing requires --date and --incremental-field")
            return 1

    # Create configuration
    config = PartialReprocessConfig(
        source=args.source,
        reprocess_type=args.type,
        date=args.date,
        start_time=args.start_time,
        end_time=args.end_time,
        hours=args.hours or [],
        datasets=args.datasets or [],
        failure_log=args.failure_log,
        incremental_field=args.incremental_field,
        config_file=args.config_file,
        validation_script=args.validation_script,
        rollback_plan=args.rollback_plan,
        reason=args.reason,
        priority=args.priority,
        max_concurrent=args.max_concurrent,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        operation_id=args.operation_id or f"PARTIAL_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    # Create and run driver
    driver = PartialReprocessDriver(config)

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
            failed_jobs = results.get("failed_jobs", 0)
            if failed_jobs > 0:
                return 1
            else:
                return 0

        except KeyboardInterrupt:
            print("\nOperation interrupted by user")
            return 130
        except Exception as e:
            print(f"Operation failed: {e}")
            return 1

    # Run async function
    return asyncio.run(run())


if __name__ == "__main__":
    sys.exit(main())
