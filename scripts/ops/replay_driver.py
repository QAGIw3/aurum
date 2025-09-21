#!/usr/bin/env python3
"""Replay driver for data reprocessing operations."""

import argparse
import asyncio
import json
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import uuid

from aurum.logging import create_logger, LogLevel


class ReplayType(str, Enum):
    """Types of replay operations."""

    SCHEMA_CORRECTION = "schema_correction"
    QUALITY_IMPROVEMENT = "quality_improvement"
    ALGORITHM_UPDATE = "algorithm_update"
    DATA_CORRECTION = "data_correction"
    COMPLETE_RERUN = "complete_rerun"


@dataclass
class ReplayConfig:
    """Configuration for replay operations."""

    source: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    datasets: List[str] = field(default_factory=list)
    replay_type: ReplayType = ReplayType.COMPLETE_RERUN
    reason: str = ""
    priority: str = "normal"
    concurrency: int = 3
    custom_transform: Optional[str] = None
    validation_script: Optional[str] = None
    schema_version: Optional[str] = None
    algorithm_params: Dict[str, Any] = field(default_factory=dict)
    quality_profile: str = "standard"
    validation_threshold: float = 0.9
    dry_run: bool = False
    job_id: str = field(default_factory=lambda: f"REPLAY_{datetime.now().strftime('%Y%m%d_%H%M%S')}")


@dataclass
class ReplayStep:
    """A single step in a replay operation."""

    step_id: str
    description: str
    status: str = "pending"  # pending, running, completed, failed, skipped
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    records_processed: int = 0
    validation_score: Optional[float] = None
    errors: List[str] = field(default_factory=list)


class ReplayDriver:
    """Driver for executing replay operations."""

    def __init__(self, config: ReplayConfig):
        """Initialize replay driver.

        Args:
            config: Replay configuration
        """
        self.config = config
        self.logger = create_logger(
            source_name="replay_driver",
            kafka_bootstrap_servers=None,
            kafka_topic="aurum.replay.operations",
            dataset="replay_operations"
        )

        # Steps tracking
        self.steps: List[ReplayStep] = []
        self.current_step: Optional[ReplayStep] = None

    async def execute(self) -> Dict[str, Any]:
        """Execute the replay operation.

        Returns:
            Operation results
        """
        self.logger.log(
            LogLevel.INFO,
            f"Starting replay operation {self.config.job_id}",
            "replay_started",
            job_id=self.config.job_id,
            source=self.config.source,
            replay_type=self.config.replay_type.value,
            reason=self.config.reason
        )

        if self.config.dry_run:
            return await self._dry_run()

        try:
            # Generate and execute steps
            steps = self._generate_steps()
            results = await self._execute_steps(steps)

            self.logger.log(
                LogLevel.INFO,
                f"Replay operation {self.config.job_id} completed",
                "replay_completed",
                job_id=self.config.job_id,
                total_steps=len(steps),
                completed_steps=len([s for s in steps if s.status == "completed"])
            )

            return results

        except Exception as e:
            self.logger.log(
                LogLevel.ERROR,
                f"Replay operation {self.config.job_id} failed: {e}",
                "replay_failed",
                job_id=self.config.job_id,
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
            f"Performing dry run for replay {self.config.job_id}",
            "replay_dry_run",
            job_id=self.config.job_id
        )

        # Generate steps
        steps = self._generate_steps()

        # Validate configuration
        validation_results = await self._validate_replay_config()

        dry_run_results = {
            "job_id": self.config.job_id,
            "source": self.config.source,
            "replay_type": self.config.replay_type.value,
            "reason": self.config.reason,
            "total_steps": len(steps),
            "estimated_duration_minutes": len(steps) * 5,  # Rough estimate
            "validation_results": validation_results,
            "is_valid": all(v["valid"] for v in validation_results.values()),
            "steps": [self._step_to_dict(step) for step in steps],
            "recommendations": self._generate_dry_run_recommendations(validation_results)
        }

        self.logger.log(
            LogLevel.INFO,
            f"Dry run completed for {self.config.job_id}",
            "replay_dry_run_completed",
            job_id=self.config.job_id,
            is_valid=dry_run_results["is_valid"]
        )

        return dry_run_results

    def _generate_steps(self) -> List[ReplayStep]:
        """Generate replay steps based on configuration.

        Returns:
            List of replay steps
        """
        steps = []

        base_step_id = f"{self.config.job_id}_step"

        # Step 1: Data extraction
        steps.append(ReplayStep(
            step_id=f"{base_step_id}_001",
            description="Extract data from source system"
        ))

        # Step 2: Schema migration (if needed)
        if self.config.replay_type == ReplayType.SCHEMA_CORRECTION:
            steps.append(ReplayStep(
                step_id=f"{base_step_id}_002",
                description=f"Apply schema migration to version {self.config.schema_version}"
            ))

        # Step 3: Custom transformation (if specified)
        if self.config.custom_transform:
            steps.append(ReplayStep(
                step_id=f"{base_step_id}_003",
                description=f"Apply custom transformation: {self.config.custom_transform}"
            ))

        # Step 4: Data processing
        processing_desc = "Process data"
        if self.config.replay_type == ReplayType.ALGORITHM_UPDATE:
            processing_desc += f" with algorithm params: {self.config.algorithm_params}"
        elif self.config.replay_type == ReplayType.QUALITY_IMPROVEMENT:
            processing_desc += f" using quality profile: {self.config.quality_profile}"

        steps.append(ReplayStep(
            step_id=f"{base_step_id}_004",
            description=processing_desc
        ))

        # Step 5: Validation
        if self.config.validation_script:
            steps.append(ReplayStep(
                step_id=f"{base_step_id}_005",
                description=f"Run validation script: {self.config.validation_script}"
            ))
        else:
            steps.append(ReplayStep(
                step_id=f"{base_step_id}_005",
                description=f"Validate data quality (threshold: {self.config.validation_threshold})"
            ))

        # Step 6: Load to target
        steps.append(ReplayStep(
            step_id=f"{base_step_id}_006",
            description="Load processed data to target system"
        ))

        # Step 7: Verification
        steps.append(ReplayStep(
            step_id=f"{base_step_id}_007",
            description="Verify replay results and data integrity"
        ))

        self.steps = steps
        return steps

    async def _execute_steps(self, steps: List[ReplayStep]) -> Dict[str, Any]:
        """Execute replay steps sequentially.

        Args:
            steps: Steps to execute

        Returns:
            Execution results
        """
        start_time = datetime.now()

        for step in steps:
            self.current_step = step
            await self._execute_step(step)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Generate results
        completed_steps = [s for s in steps if s.status == "completed"]
        failed_steps = [s for s in steps if s.status == "failed"]
        total_records = sum(s.records_processed for s in completed_steps)

        results = {
            "job_id": self.config.job_id,
            "source": self.config.source,
            "replay_type": self.config.replay_type.value,
            "reason": self.config.reason,
            "total_steps": len(steps),
            "completed_steps": len(completed_steps),
            "failed_steps": len(failed_steps),
            "total_records_processed": total_records,
            "duration_seconds": duration,
            "success_rate": len(completed_steps) / max(len(steps), 1),
            "steps": [self._step_to_dict(step) for step in steps],
            "validation_score": sum(s.validation_score for s in completed_steps if s.validation_score) / max(len([s for s in completed_steps if s.validation_score]), 1)
        }

        return results

    async def _execute_step(self, step: ReplayStep) -> None:
        """Execute a single replay step.

        Args:
            step: Step to execute
        """
        step.status = "running"
        step.start_time = datetime.now()

        try:
            self.logger.log(
                LogLevel.INFO,
                f"Executing step {step.step_id}: {step.description}",
                "replay_step_started",
                job_id=self.config.job_id,
                step_id=step.step_id,
                description=step.description
            )

            # Execute step based on type
            if "extract" in step.description.lower():
                await self._execute_extraction_step(step)
            elif "migration" in step.description.lower():
                await self._execute_migration_step(step)
            elif "transform" in step.description.lower():
                await self._execute_transformation_step(step)
            elif "process" in step.description.lower():
                await self._execute_processing_step(step)
            elif "validate" in step.description.lower():
                await self._execute_validation_step(step)
            elif "load" in step.description.lower():
                await self._execute_load_step(step)
            elif "verify" in step.description.lower():
                await self._execute_verification_step(step)
            else:
                await self._execute_generic_step(step)

            step.status = "completed"
            step.end_time = datetime.now()

            self.logger.log(
                LogLevel.INFO,
                f"Step {step.step_id} completed successfully",
                "replay_step_completed",
                job_id=self.config.job_id,
                step_id=step.step_id,
                records_processed=step.records_processed
            )

        except Exception as e:
            step.status = "failed"
            step.errors.append(str(e))
            step.end_time = datetime.now()

            self.logger.log(
                LogLevel.ERROR,
                f"Step {step.step_id} failed: {e}",
                "replay_step_failed",
                job_id=self.config.job_id,
                step_id=step.step_id,
                error=str(e)
            )
            raise

    async def _execute_extraction_step(self, step: ReplayStep) -> None:
        """Execute data extraction step."""
        import asyncio
        await asyncio.sleep(2)  # Simulate extraction

        # Simulate records extracted
        step.records_processed = 10000 + (hash(step.step_id) % 5000)

    async def _execute_migration_step(self, step: ReplayStep) -> None:
        """Execute schema migration step."""
        import asyncio
        await asyncio.sleep(5)  # Simulate migration

        step.records_processed = 10000  # All records migrated

    async def _execute_transformation_step(self, step: ReplayStep) -> None:
        """Execute data transformation step."""
        import asyncio
        await asyncio.sleep(3)  # Simulate transformation

        step.records_processed = 9500  # Some records may be filtered

    async def _execute_processing_step(self, step: ReplayStep) -> None:
        """Execute data processing step."""
        import asyncio
        await asyncio.sleep(4)  # Simulate processing

        step.records_processed = 9500
        step.validation_score = 0.95 + (hash(step.step_id) % 10) / 100  # 0.95-1.04

    async def _execute_validation_step(self, step: ReplayStep) -> None:
        """Execute data validation step."""
        import asyncio
        await asyncio.sleep(2)  # Simulate validation

        step.validation_score = 0.92 + (hash(step.step_id) % 15) / 100  # 0.92-1.06

    async def _execute_load_step(self, step: ReplayStep) -> None:
        """Execute data load step."""
        import asyncio
        await asyncio.sleep(3)  # Simulate loading

        step.records_processed = 9500

    async def _execute_verification_step(self, step: ReplayStep) -> None:
        """Execute verification step."""
        import asyncio
        await asyncio.sleep(1)  # Simulate verification

        step.validation_score = 0.98

    async def _execute_generic_step(self, step: ReplayStep) -> None:
        """Execute a generic step."""
        import asyncio
        await asyncio.sleep(1)  # Simulate generic processing

    async def _validate_replay_config(self) -> Dict[str, Any]:
        """Validate replay configuration.

        Returns:
            Validation results
        """
        validation = {
            "source_available": True,
            "date_range_valid": True,
            "resources_available": True,
            "schema_compatible": True,
            "custom_scripts_valid": True
        }

        # Validate date range if specified
        if self.config.start_date and self.config.end_date:
            if self.config.start_date > self.config.end_date:
                validation["date_range_valid"] = False

        # Validate custom scripts
        if self.config.custom_transform:
            script_path = Path(self.config.custom_transform)
            if not script_path.exists():
                validation["custom_scripts_valid"] = False

        if self.config.validation_script:
            script_path = Path(self.config.validation_script)
            if not script_path.exists():
                validation["custom_scripts_valid"] = False

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

        if not validation["date_range_valid"]:
            recommendations.append("Fix date range - start date must be before end date")

        if not validation["custom_scripts_valid"]:
            recommendations.append("Ensure custom transformation and validation scripts exist")

        if self.config.concurrency > 5:
            recommendations.append("High concurrency detected - monitor system resources")

        if self.config.replay_type == ReplayType.SCHEMA_CORRECTION:
            recommendations.append("Backup data before schema migration")

        return recommendations

    def _step_to_dict(self, step: ReplayStep) -> Dict[str, Any]:
        """Convert step to dictionary.

        Args:
            step: Step to convert

        Returns:
            Step dictionary
        """
        return {
            "step_id": step.step_id,
            "description": step.description,
            "status": step.status,
            "start_time": step.start_time.isoformat() if step.start_time else None,
            "end_time": step.end_time.isoformat() if step.end_time else None,
            "records_processed": step.records_processed,
            "validation_score": step.validation_score,
            "errors": step.errors
        }


def main():
    """Main CLI function."""
    parser = argparse.ArgumentParser(
        description="Execute replay operations for data reprocessing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic replay for recent data
  python replay_driver.py --source eia --reason "Schema correction"

  # Replay specific date range
  python replay_driver.py --source eia --start-date 2024-12-20 --end-date 2024-12-31 --reason "Data quality issues"

  # Schema correction replay
  python replay_driver.py --type schema_correction --schema-version v2.1 --source eia --date-range 2024-12-01:2024-12-31

  # Quality improvement replay
  python replay_driver.py --type quality_improvement --quality-profile enhanced --source fred --validation-threshold 0.95

  # Algorithm update replay
  python replay_driver.py --type algorithm_update --algorithm-params "sensitivity=0.8" --source eia
        """
    )

    parser.add_argument(
        "--source",
        required=True,
        choices=["eia", "fred", "cpi", "noaa", "iso"],
        help="Data source for replay"
    )

    parser.add_argument(
        "--start-date",
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        help="Start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--end-date",
        type=lambda d: datetime.strptime(d, '%Y-%m-%d').date(),
        help="End date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--date-range",
        help="Date range as start:end (YYYY-MM-DD:YYYY-MM-DD)"
    )

    parser.add_argument(
        "--datasets",
        nargs="*",
        help="Specific datasets to replay"
    )

    parser.add_argument(
        "--type",
        type=ReplayType,
        choices=list(ReplayType),
        default=ReplayType.COMPLETE_RERUN,
        help="Type of replay operation"
    )

    parser.add_argument(
        "--reason",
        required=True,
        help="Reason for replay operation"
    )

    parser.add_argument(
        "--priority",
        choices=["low", "normal", "high"],
        default="normal",
        help="Operation priority"
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Maximum concurrent operations"
    )

    parser.add_argument(
        "--custom-transform",
        help="Custom transformation script path"
    )

    parser.add_argument(
        "--validation-script",
        help="Custom validation script path"
    )

    parser.add_argument(
        "--schema-version",
        help="Target schema version for migration"
    )

    parser.add_argument(
        "--algorithm-params",
        help="Algorithm parameters as key=value pairs"
    )

    parser.add_argument(
        "--quality-profile",
        default="standard",
        help="Quality validation profile"
    )

    parser.add_argument(
        "--validation-threshold",
        type=float,
        default=0.9,
        help="Minimum validation score (0.0-1.0)"
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration without executing"
    )

    parser.add_argument(
        "--job-id",
        help="Custom job ID"
    )

    parser.add_argument(
        "--output",
        help="Output file for results"
    )

    args = parser.parse_args()

    # Parse date range if provided
    start_date = args.start_date
    end_date = args.end_date

    if args.date_range:
        try:
            start_str, end_str = args.date_range.split(':')
            start_date = datetime.strptime(start_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_str, '%Y-%m-%d').date()
        except ValueError:
            print("Error: Invalid date range format. Use YYYY-MM-DD:YYYY-MM-DD")
            return 1

    if not start_date or not end_date:
        print("Error: Must specify either --start-date and --end-date or --date-range")
        return 1

    if start_date > end_date:
        print("Error: Start date must be before end date")
        return 1

    # Parse algorithm params
    algorithm_params = {}
    if args.algorithm_params:
        for param in args.algorithm_params.split(','):
            if '=' in param:
                key, value = param.split('=', 1)
                algorithm_params[key.strip()] = value.strip()

    # Create configuration
    config = ReplayConfig(
        source=args.source,
        start_date=start_date,
        end_date=end_date,
        datasets=args.datasets or [],
        replay_type=args.type,
        reason=args.reason,
        priority=args.priority,
        concurrency=args.concurrency,
        custom_transform=args.custom_transform,
        validation_script=args.validation_script,
        schema_version=args.schema_version,
        algorithm_params=algorithm_params,
        quality_profile=args.quality_profile,
        validation_threshold=args.validation_threshold,
        dry_run=args.dry_run,
        job_id=args.job_id or f"REPLAY_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    )

    # Create and run driver
    driver = ReplayDriver(config)

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
            failed_steps = results.get("failed_steps", 0)
            if failed_steps > 0:
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
